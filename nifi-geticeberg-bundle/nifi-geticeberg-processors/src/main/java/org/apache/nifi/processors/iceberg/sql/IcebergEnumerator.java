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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.nifi.processors.iceberg.converter.IcebergToRecordConverter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Adapts an Iceberg row scan to a Calcite {@link Enumerator} of {@code Object[]} rows in the
 * projected column order. The Iceberg iterable is closed proactively on exhaustion because
 * Calcite's LINQ4J pipeline does not guarantee a {@code close()} call; {@code reset()} opens a
 * fresh Iceberg scan through the supplier.
 */
public class IcebergEnumerator implements Enumerator<Object[]> {

    private final Supplier<CloseableIterable<org.apache.iceberg.data.Record>> scanSupplier;
    private final RecordSchema recordSchema;
    private final Types.StructType struct;
    private final List<String> columnNames;

    private CloseableIterable<org.apache.iceberg.data.Record> iterable;
    private Iterator<org.apache.iceberg.data.Record> iterator;
    private Object[] current;

    public IcebergEnumerator(final Supplier<CloseableIterable<org.apache.iceberg.data.Record>> scanSupplier,
                             final RecordSchema recordSchema, final Types.StructType struct, final List<String> columnNames) {
        this.scanSupplier = scanSupplier;
        this.recordSchema = recordSchema;
        this.struct = struct;
        this.columnNames = columnNames;
        open();
    }

    private void open() {
        iterable = scanSupplier.get();
        iterator = iterable.iterator();
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (iterator != null && iterator.hasNext()) {
            final org.apache.iceberg.data.Record row = iterator.next();
            final Record record = IcebergToRecordConverter.toRecord(row, recordSchema, struct);
            final Object[] values = new Object[columnNames.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = record.getValue(columnNames.get(i));
            }
            current = values;
            return true;
        }
        closeIterable();
        return false;
    }

    @Override
    public void reset() {
        closeIterable();
        open();
    }

    @Override
    public void close() {
        closeIterable();
    }

    private void closeIterable() {
        if (iterable != null) {
            try {
                iterable.close();
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to close Iceberg scan", e);
            } finally {
                iterable = null;
                iterator = null;
            }
        }
    }
}
