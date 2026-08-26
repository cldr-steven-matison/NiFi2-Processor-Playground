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
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkplugPayloadFactory {

    private List<Metric> oneMetric() throws Exception {
        return List.of(new MetricBuilder("Sensors/Temperature", MetricDataType.Double, 21.0).createMetric());
    }

    @Test
    void beginSessionAdvancesBdSeqAndResetsSeq() throws Exception {
        final SparkplugPayloadFactory factory = new SparkplugPayloadFactory();

        factory.beginSession();
        assertEquals(0L, factory.sessionBdSeq());
        factory.createBirth(oneMetric());
        assertEquals(0L, factory.lastSeq(), "NBIRTH carries seq 0");
        factory.createData(oneMetric());
        assertEquals(1L, factory.lastSeq());

        // A reconnect is a new session: bdSeq increments, seq restarts at 0.
        factory.beginSession();
        assertEquals(1L, factory.sessionBdSeq());
        factory.createBirth(oneMetric());
        assertEquals(0L, factory.lastSeq());
    }

    @Test
    void bdSeqIsStableWithinASession() throws Exception {
        final SparkplugPayloadFactory factory = new SparkplugPayloadFactory();
        factory.beginSession();
        factory.createBirth(oneMetric());
        final long bd = factory.sessionBdSeq();
        factory.createData(oneMetric());
        factory.createData(oneMetric());
        assertEquals(bd, factory.sessionBdSeq(), "bdSeq must not change until a new session");
    }

    @Test
    void seqWrapsAt256() throws Exception {
        final SparkplugPayloadFactory factory = new SparkplugPayloadFactory();
        factory.beginSession();
        factory.createBirth(oneMetric()); // consumes seq 0

        long last = -1;
        for (int i = 1; i <= 255; i++) {
            factory.createData(oneMetric());
            last = factory.lastSeq();
        }
        assertEquals(255L, last, "255th data message after birth carries seq 255");

        factory.createData(oneMetric());
        assertEquals(0L, factory.lastSeq(), "seq wraps back to 0 after 255");
    }

    @Test
    void allPayloadsEncodeToNonEmptyBytes() throws Exception {
        final SparkplugPayloadFactory factory = new SparkplugPayloadFactory();
        factory.beginSession();
        assertTrue(factory.createDeath().length > 0);
        assertTrue(factory.createBirth(oneMetric()).length > 0);
        assertTrue(factory.createData(oneMetric()).length > 0);
    }
}
