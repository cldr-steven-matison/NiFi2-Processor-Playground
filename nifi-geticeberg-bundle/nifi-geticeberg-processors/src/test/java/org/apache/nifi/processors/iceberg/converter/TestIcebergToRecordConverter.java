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
package org.apache.nifi.processors.iceberg.converter;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct unit tests for the Iceberg→NiFi type mapping — every branch of {@code toDataType} and
 * {@code toRecordValue} is exercised without a live catalog or the Calcite engine.
 */
public class TestIcebergToRecordConverter {

    private static RecordFieldType fieldType(RecordSchema schema, String name) {
        final DataType dataType = schema.getDataType(name).orElseThrow();
        return dataType.getFieldType();
    }

    @Test
    public void testScalarTypesMapAndConvert() {
        final Schema schema = new Schema(
                required(1, "b", Types.BooleanType.get()),
                required(2, "i", Types.IntegerType.get()),
                required(3, "l", Types.LongType.get()),
                required(4, "f", Types.FloatType.get()),
                required(5, "d", Types.DoubleType.get()),
                required(6, "dec", Types.DecimalType.of(9, 2)),
                required(7, "s", Types.StringType.get()),
                required(8, "day", Types.DateType.get()),
                required(9, "clock", Types.TimeType.get()),
                required(10, "ts", Types.TimestampType.withoutZone()),
                required(11, "id", Types.UUIDType.get()));

        final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(schema);
        assertEquals(RecordFieldType.BOOLEAN, fieldType(recordSchema, "b"));
        assertEquals(RecordFieldType.INT, fieldType(recordSchema, "i"));
        assertEquals(RecordFieldType.LONG, fieldType(recordSchema, "l"));
        assertEquals(RecordFieldType.FLOAT, fieldType(recordSchema, "f"));
        assertEquals(RecordFieldType.DOUBLE, fieldType(recordSchema, "d"));
        assertEquals(RecordFieldType.DECIMAL, fieldType(recordSchema, "dec"));
        assertEquals(RecordFieldType.STRING, fieldType(recordSchema, "s"));
        assertEquals(RecordFieldType.DATE, fieldType(recordSchema, "day"));
        assertEquals(RecordFieldType.TIME, fieldType(recordSchema, "clock"));
        assertEquals(RecordFieldType.TIMESTAMP, fieldType(recordSchema, "ts"));
        // UUID has no dedicated Record type — it travels as a string
        assertEquals(RecordFieldType.STRING, fieldType(recordSchema, "id"));

        final UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
        final GenericRecord icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("b", Boolean.TRUE);
        icebergRecord.setField("i", 42);
        icebergRecord.setField("l", 100L);
        icebergRecord.setField("f", 1.5f);
        icebergRecord.setField("d", 2.5d);
        icebergRecord.setField("dec", new BigDecimal("1.23"));
        icebergRecord.setField("s", "hello");
        icebergRecord.setField("day", LocalDate.of(2026, 1, 2));
        icebergRecord.setField("clock", LocalTime.of(3, 4, 5));
        icebergRecord.setField("ts", LocalDateTime.of(2026, 1, 2, 3, 4, 5));
        icebergRecord.setField("id", uuid);

        final Record record = IcebergToRecordConverter.toRecord(icebergRecord, recordSchema, schema.asStruct());
        assertEquals(Boolean.TRUE, record.getValue("b"));
        assertEquals(42, record.getValue("i"));
        assertEquals(100L, record.getValue("l"));
        assertEquals(1.5f, record.getValue("f"));
        assertEquals(2.5d, record.getValue("d"));
        assertEquals(new BigDecimal("1.23"), record.getValue("dec"));
        assertEquals("hello", record.getValue("s"));
        assertEquals(java.sql.Date.valueOf(LocalDate.of(2026, 1, 2)), record.getValue("day"));
        assertEquals(Time.valueOf(LocalTime.of(3, 4, 5)), record.getValue("clock"));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 1, 2, 3, 4, 5)), record.getValue("ts"));
        assertEquals(uuid.toString(), record.getValue("id"));
    }

    @Test
    public void testTimestampWithZoneConverts() {
        final Schema schema = new Schema(required(1, "ts", Types.TimestampType.withZone()));
        final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(schema);

        final OffsetDateTime odt = OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
        final GenericRecord icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("ts", odt);

        final Record record = IcebergToRecordConverter.toRecord(icebergRecord, recordSchema, schema.asStruct());
        assertEquals(Timestamp.from(odt.toInstant()), record.getValue("ts"));
    }

    @Test
    public void testBinaryAndFixedBecomeByteArrays() {
        final Schema schema = new Schema(
                required(1, "bin", Types.BinaryType.get()),
                required(2, "fix", Types.FixedType.ofLength(4)));

        final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(schema);
        assertEquals(RecordFieldType.ARRAY, fieldType(recordSchema, "bin"));
        assertEquals(RecordFieldType.ARRAY, fieldType(recordSchema, "fix"));

        final GenericRecord icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("bin", ByteBuffer.wrap(new byte[]{9, 8, 7}));
        icebergRecord.setField("fix", new byte[]{1, 2, 3, 4});

        final Record record = IcebergToRecordConverter.toRecord(icebergRecord, recordSchema, schema.asStruct());
        assertArrayEquals(new byte[]{9, 8, 7}, (byte[]) record.getValue("bin"));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, (byte[]) record.getValue("fix"));
    }

    @Test
    public void testStructListAndMapConvertRecursively() {
        final Types.StructType nested = Types.StructType.of(
                required(2, "x", Types.IntegerType.get()),
                optional(3, "y", Types.StringType.get()));
        final Schema schema = new Schema(
                required(1, "child", nested),
                required(4, "tags", Types.ListType.ofRequired(5, Types.StringType.get())),
                required(6, "counts", Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.IntegerType.get())));

        final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(schema);
        assertEquals(RecordFieldType.RECORD, fieldType(recordSchema, "child"));
        assertEquals(RecordFieldType.ARRAY, fieldType(recordSchema, "tags"));
        assertEquals(RecordFieldType.MAP, fieldType(recordSchema, "counts"));

        final GenericRecord child = GenericRecord.create(nested);
        child.setField("x", 7);
        child.setField("y", "seven");

        final GenericRecord icebergRecord = GenericRecord.create(schema);
        icebergRecord.setField("child", child);
        icebergRecord.setField("tags", List.of("a", "b"));
        icebergRecord.setField("counts", Map.of("k", 5));

        final Record record = IcebergToRecordConverter.toRecord(icebergRecord, recordSchema, schema.asStruct());

        final Record childRecord = assertInstanceOf(Record.class, record.getValue("child"));
        assertEquals(7, childRecord.getValue("x"));
        assertEquals("seven", childRecord.getValue("y"));

        assertArrayEquals(new Object[]{"a", "b"}, (Object[]) record.getValue("tags"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> counts = (Map<String, Object>) record.getValue("counts");
        assertEquals(5, counts.get("k"));
    }

    @Test
    public void testNullValuesPassThrough() {
        final Schema schema = new Schema(
                optional(1, "s", Types.StringType.get()),
                optional(2, "i", Types.IntegerType.get()),
                optional(3, "day", Types.DateType.get()));

        final RecordSchema recordSchema = IcebergToRecordConverter.toRecordSchema(schema);
        final GenericRecord icebergRecord = GenericRecord.create(schema);
        // leave every field null

        final Record record = IcebergToRecordConverter.toRecord(icebergRecord, recordSchema, schema.asStruct());
        assertNull(record.getValue("s"));
        assertNull(record.getValue("i"));
        assertNull(record.getValue("day"));
        // an optional field maps to a nullable Record field
        assertTrue(recordSchema.getField("s").orElseThrow().isNullable());
    }
}
