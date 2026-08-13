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
package org.apache.nifi.processors.iceberg.sql;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.iceberg.converter.IcebergToRecordConverter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The pushdown seam: a Calcite {@link ProjectableFilterableTable} over a loaded Iceberg
 * {@link Table}. Calcite hands {@code scan} the projection ordinals and a mutable filter list;
 * every conjunct with a provable Iceberg equivalent is pushed into the Iceberg scan and removed
 * from the list, and whatever remains is evaluated by Calcite as a residual filter — so a partial
 * (or empty) translation is always still correct, just less pruned.
 *
 * Each scan also runs a metadata-only {@code planFiles()} pass with an
 * {@link InMemoryMetricsReporter} attached, capturing the skipped-file/manifest counters that
 * prove pruning happened; the processor reads them back after the query's ResultSet is drained.
 */
public class IcebergTable extends AbstractTable implements ProjectableFilterableTable {

    private final Table table;
    private final ComponentLog logger;
    private final org.apache.iceberg.Schema schema;
    private final List<String> fieldNames;

    private volatile String pushedFilter;
    private volatile String pushedColumns;
    private volatile ScanReport scanReport;

    public IcebergTable(final Table table, final ComponentLog logger) {
        this.table = table;
        this.logger = logger;
        this.schema = table.schema();
        this.fieldNames = schema.columns().stream().map(Types.NestedField::name).toList();
    }

    /** Clear the capture fields before running a query so attributes never carry over from the previous one. */
    public void resetCapture() {
        pushedFilter = null;
        pushedColumns = null;
        scanReport = null;
    }

    public String getPushedFilter() {
        return pushedFilter;
    }

    public String getPushedColumns() {
        return pushedColumns;
    }

    public ScanReport getScanReport() {
        return scanReport;
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        final JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
        final List<RelDataType> types = new ArrayList<>(fieldNames.size());
        for (final Types.NestedField field : schema.columns()) {
            final RelDataType type = javaTypeFactory.createJavaType(javaClassFor(field.type()));
            types.add(javaTypeFactory.createTypeWithNullability(type, field.isOptional()));
        }
        return javaTypeFactory.createStructType(types, fieldNames);
    }

    private static Class<?> javaClassFor(final Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> Boolean.class;
            case INTEGER -> Integer.class;
            case LONG -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case DECIMAL -> BigDecimal.class;
            case DATE -> Date.class;
            case TIME -> Time.class;
            case TIMESTAMP -> Timestamp.class;
            case STRING, UUID -> String.class;
            // complex and binary columns pass through as opaque objects; SQL can select them but not operate on them
            default -> Object.class;
        };
    }

    @Override
    public Enumerable<Object[]> scan(final DataContext root, final List<RexNode> filters, final int[] projects) {
        // scan may run more than once during planning/execution, so everything is re-derived
        // from the arguments on each call rather than relying on earlier mutations
        Expression pushedExpression = null;
        final List<RexNode> pushedNodes = new ArrayList<>();
        for (final RexNode filter : filters) {
            final Expression translated = RexToIcebergExpression.translate(filter, fieldNames, schema);
            if (translated != null) {
                pushedExpression = pushedExpression == null ? translated : Expressions.and(pushedExpression, translated);
                pushedNodes.add(filter);
            }
        }
        filters.removeAll(pushedNodes);

        final Expression rowFilter = pushedExpression == null ? Expressions.alwaysTrue() : pushedExpression;
        final List<String> selected = projects == null
                ? fieldNames
                : Arrays.stream(projects).mapToObj(fieldNames::get).toList();

        this.pushedFilter = pushedExpression == null ? "" : pushedExpression.toString();
        this.pushedColumns = String.join(",", selected);
        captureScanMetrics(rowFilter, selected);

        final org.apache.iceberg.Schema projectedSchema = projects == null ? schema : schema.select(selected);
        final RecordSchema projectedRecordSchema = IcebergToRecordConverter.toRecordSchema(projectedSchema);
        final Types.StructType struct = projectedSchema.asStruct();
        final boolean projectScan = projects != null;

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new IcebergEnumerator(() -> {
                    IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table).where(rowFilter);
                    if (projectScan) {
                        builder = builder.select(selected);
                    }
                    return builder.build();
                }, projectedRecordSchema, struct, selected);
            }
        };
    }

    private void captureScanMetrics(final Expression filter, final List<String> columns) {
        try {
            final InMemoryMetricsReporter reporter = new InMemoryMetricsReporter();
            final TableScan metricsScan = table.newScan().filter(filter).select(columns).metricsReporter(reporter);
            try (CloseableIterable<FileScanTask> tasks = metricsScan.planFiles()) {
                tasks.forEach(task -> {
                });
            }
            this.scanReport = reporter.scanReport();
        } catch (final Exception e) {
            logger.warn("Failed to capture Iceberg scan metrics for pushdown-proof attributes", e);
        }
    }
}
