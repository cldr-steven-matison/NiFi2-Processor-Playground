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
 * Everything {@link MqttPublisher.Factory#connect} needs to open a Sparkplug node session:
 * broker/identity, optional credentials, and the NDEATH will to register before connecting.
 *
 * @param brokerUri   e.g. {@code tcp://mosquitto:1883} or {@code ssl://broker:8883}
 * @param clientId    stable MQTT client id for this edge node
 * @param username    MQTT username, or {@code null} for anonymous
 * @param password    MQTT password, or {@code null}; {@code char[]} to match the Paho API
 * @param willTopic   the NDEATH topic ({@code spBv1.0/<group>/NDEATH/<node>})
 * @param willPayload the encoded NDEATH certificate
 * @param willQos     QoS for the will message
 */
public record MqttConnectionConfig(
        String brokerUri,
        String clientId,
        String username,
        char[] password,
        String willTopic,
        byte[] willPayload,
        int willQos) {
}
