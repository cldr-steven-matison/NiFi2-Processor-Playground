/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.iceberg;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.sql.IcebergTable;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@RequiresInstanceClassLoading(cloneAncestorResources = true)
@Tags({"iceberg", "sql", "query", "select", "pushdown", "record", "table", "calcite"})
@CapabilityDescription("Runs SQL SELECT queries against an Iceberg table through the configured catalog service, in the shape "
        + "of QueryRecord: each user-defined dynamic property is a SQL query and routes its results to a relationship of the "
        + "same name. Unlike layering QueryRecord over a full-table read, WHERE predicates and column projections are pushed "
        + "down into the Iceberg scan where provably equivalent, so partition- and stats-based file pruning happens at the "
        + "metadata layer; whatever cannot be pushed is still evaluated correctly by the SQL engine as a residual filter. "
        + "Each result FlowFile carries the pushed filter expression and the scan's skipped-file counters as proof of pruning.")
@DynamicProperties({
        @DynamicProperty(name = "The name of a relationship to route query results to",
                value = "A SQL SELECT statement over the configured table (referenced by its Table Name)",
                description = "Each dynamic property not prefixed with 'catalog.' defines a SQL query and a relationship of "
                        + "the property's name that its results are routed to."),
        @DynamicProperty(name = "catalog.<property>, e.g. catalog.s3.endpoint",
                value = "The value to pass through to the Iceberg catalog client (prefix stripped)",
                description = "Additional properties for the Iceberg catalog client, useful for object-store specifics like "
                        + "catalog.s3.endpoint, catalog.s3.path-style-access or catalog.client.region. These do not create "
                        + "relationships.")
})
@DynamicRelationship(name = "<query property name>", description = "Results of the SQL query defined by the same-named dynamic property.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records returned by the query."),
        @WritesAttribute(attribute = "mime.type", description = "The MIME type indicated by the configured Record Writer."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_QUERY_NAME, description = "The name of the query property (and relationship) that produced this FlowFile."),
        @WritesAttribute(attribute = GetIceberg.ICEBERG_CATALOG_NAMESPACE, description = "The catalog namespace the table was read from."),
        @WritesAttribute(attribute = GetIceberg.ICEBERG_TABLE_NAME, description = "The name of the Iceberg table that was queried."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_PUSHED_FILTER, description = "The filter expression pushed into the Iceberg scan; empty when the whole WHERE clause was evaluated as a residual."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_PUSHED_COLUMNS, description = "The columns the Iceberg scan was projected to."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_RESULT_DATA_FILES, description = "Data files the Iceberg scan planned to read."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_SKIPPED_DATA_FILES, description = "Data files skipped by the Iceberg scan — pruning proof."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_SKIPPED_DATA_MANIFESTS, description = "Data manifests skipped by the Iceberg scan."),
        @WritesAttribute(attribute = QueryIceberg.ATTR_QUERY_ERROR, description = "On the failure relationship: the error message of the failed query or table load.")
})
public class QueryIceberg extends AbstractProcessor {

    public static final String ATTR_QUERY_NAME = "QueryIceberg.query";
    public static final String ATTR_QUERY_ERROR = "iceberg.query.error";
    public static final String ATTR_PUSHED_FILTER = "iceberg.pushdown.filter";
    public static final String ATTR_PUSHED_COLUMNS = "iceberg.pushdown.columns";
    public static final String ATTR_RESULT_DATA_FILES = "iceberg.scan.result.data.files";
    public static final String ATTR_SKIPPED_DATA_FILES = "iceberg.scan.skipped.data.files";
    public static final String ATTR_SKIPPED_DATA_MANIFESTS = "iceberg.scan.skipped.data.manifests";

    static final String CATALOG_PROPERTY_PREFIX = "catalog.";

    static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("catalog-service")
            .displayName("Catalog Service")
            .description("Specifies the Controller Service to use for handling references to table’s metadata files.")
            .identifiesControllerService(IcebergCatalogService.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG_NAMESPACE = new PropertyDescriptor.Builder()
            .name("catalog-namespace")
            .displayName("Catalog Namespace")
            .description("The namespace of the catalog. Multi-level namespaces can be given separated with dots.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("The name of the Iceberg table to query. This is also the table name the SQL queries reference.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing the query results.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
            .name("default-precision")
            .displayName("Default Decimal Precision")
            .description("When a decimal value's precision cannot be derived from the query result, this value is used.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .required(true)
            .build();

    static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
            .name("default-scale")
            .displayName("Default Decimal Scale")
            .description("When a decimal value's scale cannot be derived from the query result, this value is used.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("Whether a query that returns no rows should still emit an empty FlowFile to its relationship.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the table cannot be loaded, or an individual query fails, a FlowFile carrying the error message "
                    + "is routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            CATALOG, CATALOG_NAMESPACE, TABLE_NAME, RECORD_WRITER, DEFAULT_PRECISION, DEFAULT_SCALE, INCLUDE_ZERO_RECORD_FLOWFILES);

    private final Set<Relationship> queryRelationships = ConcurrentHashMap.newKeySet();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(queryRelationships);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (isCatalogProperty(propertyDescriptorName)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .description("Iceberg catalog client property '" + propertyDescriptorName.substring(CATALOG_PROPERTY_PREFIX.length()) + "'")
                    .required(false)
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                    .dynamic(true)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("SQL query whose results are routed to the '" + propertyDescriptorName + "' relationship")
                .required(false)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic() || isCatalogProperty(descriptor.getName())) {
            return;
        }
        final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();
        if (newValue == null) {
            queryRelationships.remove(relationship);
        } else {
            queryRelationships.add(relationship);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        int queryCount = 0;
        for (final PropertyDescriptor descriptor : validationContext.getProperties().keySet()) {
            if (!descriptor.isDynamic() || isCatalogProperty(descriptor.getName())) {
                continue;
            }
            queryCount++;
            final String sql = validationContext.getProperty(descriptor).evaluateAttributeExpressions().getValue();
            try {
                SqlParser.create(sql, SqlParser.config().withLex(Lex.MYSQL_ANSI)).parseStmt();
            } catch (final SqlParseException e) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getName())
                        .input(sql)
                        .valid(false)
                        .explanation("SQL cannot be parsed: " + e.getMessage())
                        .build());
            }
        }
        if (queryCount == 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Queries")
                    .valid(false)
                    .explanation("at least one dynamic property defining a SQL query is required")
                    .build());
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();
        final int defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions().asInteger();
        final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean();

        final Map<String, String> queries = new TreeMap<>();
        context.getProperties().keySet().forEach(descriptor -> {
            if (descriptor.isDynamic() && !isCatalogProperty(descriptor.getName())) {
                queries.put(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        });

        Catalog catalog = null;
        try {
            catalog = loadCatalog(context);
            final TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(catalogNamespace.split("\\.")), tableName);
            final Table table = catalog.loadTable(tableIdentifier);
            final IcebergTable icebergTable = new IcebergTable(table, getLogger());

            try (Connection connection = createCalciteConnection(tableName, icebergTable)) {
                for (final Map.Entry<String, String> query : queries.entrySet()) {
                    try {
                        runQuery(session, connection, icebergTable, query.getKey(), query.getValue(), writerFactory,
                                defaultPrecision, defaultScale, includeZeroRecordFlowFiles, catalogNamespace, tableName, table.location());
                    } catch (final Exception e) {
                        getLogger().error("Query {} failed against Iceberg table {}.{}", query.getKey(), catalogNamespace, tableName, e);
                        transferFailure(session, catalogNamespace, tableName, query.getKey(), e);
                    }
                }
            }
        } catch (final Exception e) {
            getLogger().error("Failed to load Iceberg table {}.{}", catalogNamespace, tableName, e);
            transferFailure(session, catalogNamespace, tableName, null, e);
            context.yield();
        } finally {
            closeCatalog(catalog);
        }
    }

    private void runQuery(final ProcessSession session, final Connection connection, final IcebergTable icebergTable,
                          final String queryName, final String sql, final RecordSetWriterFactory writerFactory,
                          final int defaultPrecision, final int defaultScale, final boolean includeZeroRecordFlowFiles,
                          final String catalogNamespace, final String tableName, final String tableLocation) throws Exception {
        final long startNanos = System.nanoTime();
        icebergTable.resetCapture();

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            final ResultSetRecordSet recordSet = new ResultSetRecordSet(resultSet, null, defaultPrecision, defaultScale);
            final RecordSchema writeSchema = writerFactory.getSchema(Map.of(), recordSet.getSchema());

            FlowFile flowFile = session.create();
            final AtomicReference<WriteResult> writeResult = new AtomicReference<>();
            final AtomicReference<String> mimeType = new AtomicReference<>();
            final AtomicLong recordCount = new AtomicLong();
            final ComponentLog logger = getLogger();

            flowFile = session.write(flowFile, (final OutputStream out) -> {
                try (final RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, out, Map.of())) {
                    writer.beginRecordSet();
                    Record record;
                    while ((record = recordSet.next()) != null) {
                        writer.write(record);
                    }
                    writeResult.set(writer.finishRecordSet());
                    mimeType.set(writer.getMimeType());
                    recordCount.set(writeResult.get().getRecordCount());
                } catch (final Exception e) {
                    throw new IOException("Failed to write query results as records", e);
                }
            });

            if (recordCount.get() == 0 && !includeZeroRecordFlowFiles) {
                session.remove(flowFile);
                return;
            }

            final Map<String, String> attributes = new HashMap<>(writeResult.get().getAttributes());
            attributes.put("record.count", String.valueOf(recordCount.get()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType.get());
            attributes.put(ATTR_QUERY_NAME, queryName);
            attributes.put(GetIceberg.ICEBERG_CATALOG_NAMESPACE, catalogNamespace);
            attributes.put(GetIceberg.ICEBERG_TABLE_NAME, tableName);
            attributes.putAll(pushdownProofAttributes(icebergTable));
            flowFile = session.putAllAttributes(flowFile, attributes);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().receive(flowFile, tableLocation, transferMillis);
            session.transfer(flowFile, new Relationship.Builder().name(queryName).build());
        }
    }

    private Map<String, String> pushdownProofAttributes(final IcebergTable icebergTable) {
        final Map<String, String> attributes = new HashMap<>();
        if (icebergTable.getPushedFilter() != null) {
            attributes.put(ATTR_PUSHED_FILTER, icebergTable.getPushedFilter());
        }
        if (icebergTable.getPushedColumns() != null) {
            attributes.put(ATTR_PUSHED_COLUMNS, icebergTable.getPushedColumns());
        }
        final ScanReport report = icebergTable.getScanReport();
        if (report != null && report.scanMetrics() != null) {
            final ScanMetricsResult metrics = report.scanMetrics();
            putCounter(attributes, ATTR_RESULT_DATA_FILES, metrics.resultDataFiles());
            putCounter(attributes, ATTR_SKIPPED_DATA_FILES, metrics.skippedDataFiles());
            putCounter(attributes, ATTR_SKIPPED_DATA_MANIFESTS, metrics.skippedDataManifests());
        }
        return attributes;
    }

    private static void putCounter(final Map<String, String> attributes, final String key, final CounterResult counter) {
        if (counter != null) {
            attributes.put(key, String.valueOf(counter.value()));
        }
    }

    private void transferFailure(final ProcessSession session, final String catalogNamespace, final String tableName,
                                 final String queryName, final Exception e) {
        FlowFile failureFlowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GetIceberg.ICEBERG_CATALOG_NAMESPACE, catalogNamespace);
        attributes.put(GetIceberg.ICEBERG_TABLE_NAME, tableName);
        attributes.put(ATTR_QUERY_ERROR, e.getMessage() != null ? e.getMessage() : e.toString());
        if (queryName != null) {
            attributes.put(ATTR_QUERY_NAME, queryName);
        }
        failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);
        session.transfer(failureFlowFile, REL_FAILURE);
    }

    private Connection createCalciteConnection(final String tableName, final IcebergTable icebergTable) throws SQLException {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());
        // connect through the driver directly: DriverManager's caller-classloader checks are
        // unreliable inside a NAR classloader
        final Connection connection = new org.apache.calcite.jdbc.Driver().connect("jdbc:calcite:", properties);
        final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        calciteConnection.getRootSchema().add(tableName, icebergTable);
        return connection;
    }

    private Catalog loadCatalog(final ProcessContext context) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final Map<String, String> catalogProperties = new HashMap<>();
        context.getProperties().forEach((descriptor, value) -> {
            if (descriptor.isDynamic() && isCatalogProperty(descriptor.getName())) {
                final String evaluated = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                if (evaluated != null && !evaluated.isBlank()) {
                    catalogProperties.put(descriptor.getName().substring(CATALOG_PROPERTY_PREFIX.length()), evaluated);
                }
            }
        });
        return new IcebergCatalogFactory(catalogService, catalogProperties).create();
    }

    private void closeCatalog(final Catalog catalog) {
        if (catalog instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close catalog", e);
            }
        }
    }

    private static boolean isCatalogProperty(final String propertyName) {
        return propertyName.startsWith(CATALOG_PROPERTY_PREFIX);
    }
}
