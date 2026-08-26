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

/**
 * The MQTT transport seam for {@link PublishSparkplug}. Kept behind an interface so the
 * processor's Sparkplug logic — birth-before-data ordering, {@code bdSeq}/{@code seq}
 * sequencing, topic construction — is unit-testable with an in-memory fake, and the only
 * class that touches a live broker ({@link PahoMqttPublisher}) is isolated.
 */
public interface MqttPublisher extends AutoCloseable {

    /**
     * Publish an already-encoded payload to a topic.
     *
     * @param topic    the full MQTT topic (e.g. {@code spBv1.0/FactoryLine1/NDATA/Edge-01})
     * @param payload  the encoded Sparkplug B bytes
     * @param qos      MQTT quality of service (0, 1 or 2)
     * @param retained whether the broker should retain the message
     */
    void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception;

    /** @return true if the underlying client currently holds a live broker connection. */
    boolean isConnected();

    /** Disconnect (if connected) and release the client. Never throws. */
    @Override
    void close();

    /**
     * Opens a connected publisher. Implementations register the NDEATH will supplied in the
     * config <em>before</em> connecting, so an ungraceful drop still delivers the death
     * certificate.
     */
    @FunctionalInterface
    interface Factory {
        MqttPublisher connect(MqttConnectionConfig config) throws Exception;
    }
}
