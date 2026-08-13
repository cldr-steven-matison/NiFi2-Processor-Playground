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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.processors.iceberg.converter.IcebergToRecordConverter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@RequiresInstanceClassLoading(cloneAncestorResources = true)
@Tags({"iceberg", "get", "read", "table", "fetch", "record", "scan", "parquet"})
@CapabilityDescription("Reads all rows of an Iceberg table through the configured catalog service and emits them as a single "
        + "record-oriented FlowFile using the configured Record Writer. The stock Iceberg bundle is write-only (PutIceberg); "
        + "this processor is its read counterpart, aimed at REST catalogs with a read-capacity identity such as a CDP Data "
        + "Share consumer (vended credentials are requested on table load). Hadoop catalogs are also supported.")
@DynamicProperty(name = "An Iceberg catalog property name, e.g. s3.endpoint",
        value = "The value to pass through to the Iceberg REST catalog client",
        description = "Additional properties for the Iceberg catalog client. Applied on top of the properties derived from "
                + "the catalog service, useful for object-store specifics like s3.endpoint, s3.path-style-access or client.region.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records read from the Iceberg table."),
        @WritesAttribute(attribute = "iceberg.catalog.namespace", description = "The catalog namespace the table was read from."),
        @WritesAttribute(attribute = "iceberg.table.name", description = "The name of the Iceberg table that was read."),
        @WritesAttribute(attribute = "mime.type", description = "The MIME type indicated by the configured Record Writer.")
})
public class GetIceberg extends AbstractProcessor {

    public static final String ICEBERG_CATALOG_NAMESPACE = "iceberg.catalog.namespace";
    public static final String ICEBERG_TABLE_NAME = "iceberg.table.name";

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
            .description("The name of the Iceberg table to read from.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing the rows read from the Iceberg table.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
            .name("columns")
            .displayName("Columns")
            .description("A comma-separated list of column names to read from the table. If not set, all columns are read.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the rows read from the Iceberg table is routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(CATALOG, CATALOG_NAMESPACE, TABLE_NAME, RECORD_WRITER, COLUMNS);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final long startNanos = System.nanoTime();
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        Catalog catalog = null;
        try {
            catalog = loadCatalog(context);
            final TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(catalogNamespace.split("\\.")), tableName);
            final Table table = catalog.loadTable(tableIdentifier);

            final org.apache.iceberg.Schema projectedSchema = projectSchema(context, table);
            final Types.StructType struct = projectedSchema.asStruct();
            final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(projectedSchema);
            final RecordSchema writeSchema = writerFactory.getSchema(Map.of(), recordSchema);

            IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
            final List<String> columns = getColumns(context);
            if (columns != null) {
                scanBuilder = scanBuilder.select(columns);
            }

            FlowFile flowFile = session.create();
            final AtomicReference<WriteResult> writeResult = new AtomicReference<>();
            final AtomicReference<String> mimeType = new AtomicReference<>();
            final AtomicLong recordCount = new AtomicLong();
            final ComponentLog logger = getLogger();
            final IcebergGenerics.ScanBuilder finalScanBuilder = scanBuilder;

            try (final CloseableIterable<org.apache.iceberg.data.Record> rows = finalScanBuilder.build()) {
                flowFile = session.write(flowFile, (final OutputStream out) -> {
                    try (final RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, out, Map.of())) {
                        writer.beginRecordSet();
                        for (final org.apache.iceberg.data.Record row : rows) {
                            writer.write(IcebergToRecordConverter.toRecord(row, recordSchema, struct));
                        }
                        writeResult.set(writer.finishRecordSet());
                        mimeType.set(writer.getMimeType());
                        recordCount.set(writeResult.get().getRecordCount());
                    } catch (final Exception e) {
                        throw new IOException("Failed to write Iceberg rows as records", e);
                    }
                });
            }

            final Map<String, String> attributes = new HashMap<>(writeResult.get().getAttributes());
            attributes.put("record.count", String.valueOf(recordCount.get()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType.get());
            attributes.put(ICEBERG_CATALOG_NAMESPACE, catalogNamespace);
            attributes.put(ICEBERG_TABLE_NAME, tableName);
            flowFile = session.putAllAttributes(flowFile, attributes);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().receive(flowFile, table.location(), transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Exception occurred while reading Iceberg table {}.{}", catalogNamespace, tableName, e);
            session.rollback();
            context.yield();
        } finally {
            closeCatalog(catalog);
        }
    }

    private org.apache.iceberg.Schema projectSchema(ProcessContext context, Table table) {
        final List<String> columns = getColumns(context);
        return columns == null ? table.schema() : table.schema().select(columns);
    }

    private List<String> getColumns(ProcessContext context) {
        if (!context.getProperty(COLUMNS).isSet()) {
            return null;
        }
        final String columns = context.getProperty(COLUMNS).evaluateAttributeExpressions().getValue();
        return Arrays.stream(columns.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private Catalog loadCatalog(ProcessContext context) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final Map<String, String> dynamicProperties = new HashMap<>();
        context.getProperties().forEach((descriptor, value) -> {
            if (descriptor.isDynamic()) {
                final String evaluated = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                if (evaluated != null && !evaluated.isBlank()) {
                    dynamicProperties.put(descriptor.getName(), evaluated);
                }
            }
        });
        return new IcebergCatalogFactory(catalogService, dynamicProperties).create();
    }

    private void closeCatalog(Catalog catalog) {
        if (catalog instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close catalog", e);
            }
        }
    }
}
