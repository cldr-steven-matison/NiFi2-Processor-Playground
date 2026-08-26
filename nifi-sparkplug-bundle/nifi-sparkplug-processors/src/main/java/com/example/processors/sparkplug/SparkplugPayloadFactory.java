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

import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.SparkplugBPayloadEncoder;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.SparkplugBPayload.SparkplugBPayloadBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Builds and encodes the three Sparkplug B edge-node payloads — NBIRTH, NDATA, NDEATH — and owns
 * the two sequence counters the spec requires:
 *
 * <ul>
 *   <li><b>{@code bdSeq}</b> — the birth/death sequence. One value per node <em>session</em>
 *       (one MQTT connection). The NBIRTH and its matching NDEATH will carry the same
 *       {@code bdSeq}; {@link #beginSession()} advances it for the next connection.</li>
 *   <li><b>{@code seq}</b> — the per-message sequence, 0–255 wrapping. Reset to 0 by every
 *       NBIRTH (which itself carries {@code seq = 0}); each subsequent NDATA takes the next
 *       value.</li>
 * </ul>
 *
 * <p>The class is not thread-safe by design — {@link PublishSparkplug} is {@code @TriggerSerially}
 * so only one thread advances the counters at a time.</p>
 */
public class SparkplugPayloadFactory {

    static final String BD_SEQ = "bdSeq";
    static final String NODE_CONTROL_REBIRTH = "Node Control/Rebirth";

    private final SparkplugBPayloadEncoder encoder = new SparkplugBPayloadEncoder();

    private long bdSeqCounter = -1L;
    private long sessionBdSeq = 0L;
    private int seq = 0;
    private long lastSeq = -1L;

    /**
     * Advance to a new node session: pick the next {@code bdSeq} and reset {@code seq} to 0.
     * Call once per (re)connect, before building that session's NDEATH will and NBIRTH.
     */
    public void beginSession() {
        bdSeqCounter++;
        sessionBdSeq = bdSeqCounter;
        seq = 0;
    }

    /** @return the {@code bdSeq} of the current session (shared by its NBIRTH and NDEATH). */
    public long sessionBdSeq() {
        return sessionBdSeq;
    }

    /** @return the {@code seq} used by the most recently built NBIRTH/NDATA payload. */
    public long lastSeq() {
        return lastSeq;
    }

    /**
     * NDEATH certificate for the current session — carries only the session {@code bdSeq}.
     * Registered as the MQTT will before connecting, and re-published on a graceful stop.
     */
    public byte[] createDeath() throws SparkplugException, IOException {
        final SparkplugBPayload payload = new SparkplugBPayloadBuilder()
                .setTimestamp(new Date())
                .addMetric(bdSeqMetric())
                .createPayload();
        return encoder.getBytes(payload, false);
    }

    /**
     * NBIRTH certificate: {@code seq = 0}, declaring {@code bdSeq}, the
     * {@code Node Control/Rebirth} control metric, and the supplied application metrics.
     */
    public byte[] createBirth(List<Metric> metrics) throws SparkplugException, IOException {
        final SparkplugBPayloadBuilder builder = new SparkplugBPayloadBuilder()
                .setTimestamp(new Date())
                .setSeq(nextSeq())
                .addMetric(bdSeqMetric())
                .addMetric(new MetricBuilder(NODE_CONTROL_REBIRTH, MetricDataType.Boolean, false).createMetric());
        for (final Metric metric : metrics) {
            builder.addMetric(metric);
        }
        return encoder.getBytes(builder.createPayload(), false);
    }

    /** NDATA message: the next {@code seq}, carrying the supplied application metrics. */
    public byte[] createData(List<Metric> metrics) throws SparkplugException, IOException {
        final SparkplugBPayloadBuilder builder = new SparkplugBPayloadBuilder()
                .setTimestamp(new Date())
                .setSeq(nextSeq());
        for (final Metric metric : metrics) {
            builder.addMetric(metric);
        }
        return encoder.getBytes(builder.createPayload(), false);
    }

    private long nextSeq() {
        final long current = seq;
        lastSeq = current;
        seq = (seq + 1) % 256;
        return current;
    }

    private Metric bdSeqMetric() throws SparkplugException {
        return new MetricBuilder(BD_SEQ, MetricDataType.Int64, sessionBdSeq).createMetric();
    }
}
