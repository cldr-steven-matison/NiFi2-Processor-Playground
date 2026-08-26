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
package com.example.processors.sparkplug;

import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.MetricDataType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkplugMetricParser {

    private final SparkplugMetricParser parser = new SparkplugMetricParser();

    private static byte[] json(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void infersDataTypeFromJsonType() throws Exception {
        final List<Metric> metrics = parser.parse(json(
                "{\"Sensors/Temperature\": 22.5, \"Sensors/Count\": 1013, \"Sensors/Online\": true, \"Sensors/Label\": \"ok\"}"));

        final Map<String, Metric> byName = metrics.stream().collect(Collectors.toMap(Metric::getName, m -> m));
        assertEquals(4, byName.size());

        assertEquals(MetricDataType.Double, byName.get("Sensors/Temperature").getDataType());
        assertEquals(22.5, ((Number) byName.get("Sensors/Temperature").getValue()).doubleValue());

        assertEquals(MetricDataType.Int64, byName.get("Sensors/Count").getDataType());
        assertEquals(1013L, ((Number) byName.get("Sensors/Count").getValue()).longValue());

        assertEquals(MetricDataType.Boolean, byName.get("Sensors/Online").getDataType());
        assertEquals(Boolean.TRUE, byName.get("Sensors/Online").getValue());

        assertEquals(MetricDataType.String, byName.get("Sensors/Label").getDataType());
        assertEquals("ok", byName.get("Sensors/Label").getValue());
    }

    @Test
    void rejectsEmptyContent() {
        final IOException e = assertThrows(IOException.class, () -> parser.parse(new byte[0]));
        assertTrue(e.getMessage().contains("empty"));
    }

    @Test
    void rejectsNullContent() {
        assertThrows(IOException.class, () -> parser.parse(null));
    }

    @Test
    void rejectsNonObjectJson() {
        assertThrows(IOException.class, () -> parser.parse(json("[1, 2, 3]")));
    }

    @Test
    void rejectsEmptyObject() {
        final IOException e = assertThrows(IOException.class, () -> parser.parse(json("{}")));
        assertTrue(e.getMessage().contains("no metrics"));
    }

    @Test
    void rejectsUnsupportedValueType() {
        // a null value and a nested object are both unsupported
        assertThrows(IOException.class, () -> parser.parse(json("{\"a\": null}")));
        assertThrows(IOException.class, () -> parser.parse(json("{\"a\": {\"nested\": 1}}")));
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(Exception.class, () -> parser.parse(json("not json at all")));
    }
}
