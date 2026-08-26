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

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * The live-broker {@link MqttPublisher}, backed by the Eclipse Paho synchronous MQTT v3 client.
 * This is the one class in the bundle that opens a socket, so it is deliberately thin — all the
 * Sparkplug sequencing lives in {@link SparkplugPayloadFactory} — and it is excluded from the
 * JaCoCo coverage gate (it can only be exercised against a real broker).
 */
public class PahoMqttPublisher implements MqttPublisher {

    /** The factory the processor uses in production. Tests inject an in-memory fake instead. */
    public static final Factory FACTORY = PahoMqttPublisher::open;

    private final MqttClient client;

    private PahoMqttPublisher(MqttClient client) {
        this.client = client;
    }

    private static MqttPublisher open(MqttConnectionConfig config) throws Exception {
        final MqttClient client = new MqttClient(config.brokerUri(), config.clientId(), new MemoryPersistence());
        final MqttConnectOptions options = new MqttConnectOptions();
        // Sparkplug requires a clean session: a reconnect is a new node session with a new bdSeq.
        options.setCleanSession(true);
        options.setAutomaticReconnect(false);
        if (config.username() != null && !config.username().isEmpty()) {
            options.setUserName(config.username());
        }
        if (config.password() != null && config.password().length > 0) {
            options.setPassword(config.password());
        }
        // Register the NDEATH will BEFORE connecting so an ungraceful drop still publishes it.
        if (config.willPayload() != null) {
            options.setWill(config.willTopic(), config.willPayload(), config.willQos(), false);
        }
        client.connect(options);
        return new PahoMqttPublisher(client);
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        final MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        message.setRetained(retained);
        client.publish(topic, message);
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void close() {
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
        } catch (final Exception ignored) {
            // best-effort shutdown
        } finally {
            try {
                client.close();
            } catch (final Exception ignored) {
                // best-effort shutdown
            }
        }
    }
}
