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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Iceberg schemas and generic data records into their NiFi Record API equivalents —
 * the read-direction counterpart of the stock bundle's IcebergRecordConverter.
 */
public class IcebergToRecordConverter {

    private IcebergToRecordConverter() {
    }

    public static RecordSchema toRecordSchema(Schema schema) {
        return toRecordSchema(schema.asStruct());
    }

    private static RecordSchema toRecordSchema(Types.StructType struct) {
        final List<RecordField> fields = new ArrayList<>();
        for (final Types.NestedField field : struct.fields()) {
            fields.add(new RecordField(field.name(), toDataType(field.type()), field.isOptional()));
        }
        return new SimpleRecordSchema(fields);
    }

    private static DataType toDataType(Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> RecordFieldType.BOOLEAN.getDataType();
            case INTEGER -> RecordFieldType.INT.getDataType();
            case LONG -> RecordFieldType.LONG.getDataType();
            case FLOAT -> RecordFieldType.FLOAT.getDataType();
            case DOUBLE -> RecordFieldType.DOUBLE.getDataType();
            case DATE -> RecordFieldType.DATE.getDataType();
            case TIME -> RecordFieldType.TIME.getDataType();
            case TIMESTAMP -> RecordFieldType.TIMESTAMP.getDataType();
            case DECIMAL -> {
                final Types.DecimalType decimalType = (Types.DecimalType) type;
                yield RecordFieldType.DECIMAL.getDecimalDataType(decimalType.precision(), decimalType.scale());
            }
            case FIXED, BINARY -> RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case STRUCT -> RecordFieldType.RECORD.getRecordDataType(toRecordSchema(type.asStructType()));
            case LIST -> RecordFieldType.ARRAY.getArrayDataType(toDataType(type.asListType().elementType()));
            case MAP -> RecordFieldType.MAP.getMapDataType(toDataType(type.asMapType().valueType()));
            // UUID and anything the Record API has no natural slot for travels as a string
            default -> RecordFieldType.STRING.getDataType();
        };
    }

    public static Record toRecord(org.apache.iceberg.data.Record icebergRecord, RecordSchema recordSchema, Types.StructType struct) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Types.NestedField field : struct.fields()) {
            final DataType dataType = recordSchema.getDataType(field.name()).orElse(null);
            values.put(field.name(), toRecordValue(icebergRecord.getField(field.name()), field.type(), dataType));
        }
        return new MapRecord(recordSchema, values);
    }

    private static Object toRecordValue(Object value, Type type, DataType dataType) {
        if (value == null) {
            return null;
        }
        return switch (type.typeId()) {
            case DATE -> java.sql.Date.valueOf((LocalDate) value);
            case TIME -> Time.valueOf((LocalTime) value);
            case TIMESTAMP -> value instanceof OffsetDateTime offsetDateTime
                    ? Timestamp.from(offsetDateTime.toInstant())
                    : Timestamp.valueOf((LocalDateTime) value);
            case FIXED, BINARY -> toByteArray(value);
            case STRUCT -> {
                final RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                yield toRecord((org.apache.iceberg.data.Record) value, childSchema, type.asStructType());
            }
            case LIST -> {
                final DataType elementType = ((ArrayDataType) dataType).getElementType();
                final List<?> list = (List<?>) value;
                final Object[] converted = new Object[list.size()];
                for (int i = 0; i < list.size(); i++) {
                    converted[i] = toRecordValue(list.get(i), type.asListType().elementType(), elementType);
                }
                yield converted;
            }
            case MAP -> {
                final DataType valueType = ((MapDataType) dataType).getValueType();
                final Map<?, ?> map = (Map<?, ?>) value;
                final Map<String, Object> converted = new LinkedHashMap<>();
                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    converted.put(String.valueOf(entry.getKey()), toRecordValue(entry.getValue(), type.asMapType().valueType(), valueType));
                }
                yield converted;
            }
            case UUID -> value.toString();
            default -> value;
        };
    }

    private static Object toByteArray(Object value) {
        if (value instanceof ByteBuffer byteBuffer) {
            final byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.duplicate().get(bytes);
            return bytes;
        }
        return value;
    }
}
