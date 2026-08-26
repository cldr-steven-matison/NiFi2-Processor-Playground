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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPublishSparkplug {

    private TestRunner runner;
    private RecordingFactory factory;

    @BeforeEach
    void setUp() {
        final PublishSparkplug processor = new PublishSparkplug();
        factory = new RecordingFactory();
        processor.setPublisherFactory(factory);
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PublishSparkplug.BROKER_URI, "tcp://localhost:1883");
        runner.setProperty(PublishSparkplug.CLIENT_ID, "test-edge");
        runner.setProperty(PublishSparkplug.GROUP_ID, "TestGroup");
        runner.setProperty(PublishSparkplug.EDGE_NODE_ID, "Node-1");
    }

    @Test
    void publishesBirthThenDataWithAttributes() {
        runner.enqueue("{\"Sensors/Temperature\": 22.5}".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSparkplug.REL_SUCCESS, 1);
        final List<Published> published = factory.all();
        assertEquals("spBv1.0/TestGroup/NBIRTH/Node-1", published.get(0).topic(), "NBIRTH must be published first");
        assertEquals("spBv1.0/TestGroup/NDATA/Node-1", published.get(1).topic(), "NDATA follows the birth");
        assertTrue(published.get(0).payload().length > 0);

        final MockFlowFile out = runner.getFlowFilesForRelationship(PublishSparkplug.REL_SUCCESS).get(0);
        out.assertAttributeEquals("sparkplug.topic", "spBv1.0/TestGroup/NDATA/Node-1");
        out.assertAttributeEquals("sparkplug.message.type", "NDATA");
        out.assertAttributeEquals("sparkplug.seq", "1"); // NBIRTH took seq 0
        out.assertAttributeEquals("sparkplug.bdSeq", "0");
        out.assertAttributeEquals("sparkplug.metric.count", "1");
    }

    @Test
    void secondMessageReusesSessionWithNoRebirth() {
        runner.enqueue("{\"a\": 1}".getBytes(StandardCharsets.UTF_8));
        runner.enqueue("{\"a\": 2}".getBytes(StandardCharsets.UTF_8));
        runner.run(2);

        runner.assertAllFlowFilesTransferred(PublishSparkplug.REL_SUCCESS, 2);
        assertEquals(1, factory.connectCount, "the session should be opened exactly once for two messages");

        final long births = factory.all().stream().filter(p -> p.topic().contains("/NBIRTH/")).count();
        assertEquals(1L, births, "only one NBIRTH across the session");

        final long data = factory.all().stream().filter(p -> p.topic().contains("/NDATA/")).count();
        assertEquals(2L, data, "one NDATA per FlowFile");

        // seq advances across the session: birth=0, first data=1, second data=2.
        final List<MockFlowFile> out = runner.getFlowFilesForRelationship(PublishSparkplug.REL_SUCCESS);
        out.get(0).assertAttributeEquals("sparkplug.seq", "1");
        out.get(1).assertAttributeEquals("sparkplug.seq", "2");
    }

    @Test
    void routesBadContentToFailureWithoutConnecting() {
        runner.enqueue("this is not json".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSparkplug.REL_FAILURE, 1);
        assertEquals(0, factory.connectCount, "a parse failure must not open a connection");
        final MockFlowFile out = runner.getFlowFilesForRelationship(PublishSparkplug.REL_FAILURE).get(0);
        assertNotNull(out.getAttribute("sparkplug.error"));
    }

    @Test
    void routesPublishFailureToFailureAndResetsConnection() {
        factory.failOn = topic -> topic.contains("/NDATA/"); // birth succeeds, data throws
        runner.enqueue("{\"a\": 1}".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSparkplug.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PublishSparkplug.REL_FAILURE).get(0);
        assertNotNull(out.getAttribute("sparkplug.error"));
        // the broken publisher was closed so the next FlowFile would reconnect and rebirth
        assertTrue(factory.all().get(factory.all().size() - 1).topic().contains("/NBIRTH/"),
                "the birth was published before the data publish failed");
        assertTrue(factory.latest().closeCount >= 1, "the failed connection was closed");
    }

    // ---- in-memory MQTT fakes ------------------------------------------------------------

    private record Published(String topic, byte[] payload, int qos, boolean retained) {
    }

    private static final class RecordingPublisher implements MqttPublisher {
        final List<Published> published = new ArrayList<>();
        Predicate<String> failOn = t -> false;
        boolean connected = true;
        int closeCount = 0;

        @Override
        public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
            if (failOn.test(topic)) {
                throw new RuntimeException("simulated publish failure on " + topic);
            }
            published.add(new Published(topic, payload, qos, retained));
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public void close() {
            connected = false;
            closeCount++;
        }
    }

    private static final class RecordingFactory implements MqttPublisher.Factory {
        final List<RecordingPublisher> created = new ArrayList<>();
        Predicate<String> failOn = t -> false;
        int connectCount = 0;

        @Override
        public MqttPublisher connect(MqttConnectionConfig config) {
            connectCount++;
            final RecordingPublisher publisher = new RecordingPublisher();
            publisher.failOn = failOn;
            created.add(publisher);
            return publisher;
        }

        RecordingPublisher latest() {
            return created.get(created.size() - 1);
        }

        List<Published> all() {
            final List<Published> all = new ArrayList<>();
            for (final RecordingPublisher p : created) {
                all.addAll(p.published);
            }
            return all;
        }
    }
}
