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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.tahu.message.model.Metric;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sparkplug", "sparkplug b", "mqtt", "iiot", "publish", "edge", "minifi"})
@CapabilityDescription("Encodes an incoming FlowFile's JSON metrics as a Sparkplug B payload (Eclipse Tahu) and "
        + "publishes it over MQTT (Eclipse Paho). On the first message of a node session the processor publishes an "
        + "NBIRTH certificate declaring the metrics, then an NDATA per FlowFile, managing the bdSeq/seq sequence numbers "
        + "the spec requires; an NDEATH is registered as the MQTT will and re-published on a graceful stop. This is the "
        + "publish counterpart to the consume-only ConsumeMQTTIIoT, built so a MiNiFi Java edge agent can originate "
        + "Sparkplug B without an InvokeHTTP or ExecuteScript shim. FlowFile content must be a JSON object of "
        + "metric-name -> value; the JSON type sets the Sparkplug data type (integral -> Int64, decimal -> Double, "
        + "boolean -> Boolean, text -> String).")
@WritesAttributes({
        @WritesAttribute(attribute = "sparkplug.topic", description = "The MQTT topic the NDATA message was published to."),
        @WritesAttribute(attribute = "sparkplug.message.type", description = "The Sparkplug message type published for this FlowFile (always NDATA; NBIRTH/NDEATH are session-level)."),
        @WritesAttribute(attribute = "sparkplug.seq", description = "The Sparkplug seq number (0-255) carried by this message."),
        @WritesAttribute(attribute = "sparkplug.bdSeq", description = "The birth/death sequence number of the current node session."),
        @WritesAttribute(attribute = "sparkplug.metric.count", description = "The number of metrics encoded into this message."),
        @WritesAttribute(attribute = "sparkplug.error", description = "On the failure relationship, the reason the message could not be encoded or published.")
})
public class PublishSparkplug extends AbstractProcessor {

    static final String NAMESPACE = "spBv1.0";

    static final PropertyDescriptor BROKER_URI = new PropertyDescriptor.Builder()
            .name("broker-uri")
            .displayName("Broker URI")
            .description("The MQTT broker to publish to, e.g. tcp://mosquitto:1883 or ssl://broker:8883.")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("client-id")
            .displayName("Client ID")
            .description("The MQTT client id for this edge node. Should be stable per agent.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("group-id")
            .displayName("Group ID")
            .description("The Sparkplug Group ID (the <group_id> in spBv1.0/<group_id>/<type>/<edge_node_id>).")
            .required(true)
            .defaultValue("FactoryLine1")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor EDGE_NODE_ID = new PropertyDescriptor.Builder()
            .name("edge-node-id")
            .displayName("Edge Node ID")
            .description("The Sparkplug Edge Node ID (the <edge_node_id> in the topic namespace).")
            .required(true)
            .defaultValue("Edge-01")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor QOS = new PropertyDescriptor.Builder()
            .name("qos")
            .displayName("Quality of Service")
            .description("MQTT QoS for the NBIRTH/NDATA/NDEATH messages.")
            .required(true)
            .allowableValues("0", "1", "2")
            .defaultValue("0")
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("MQTT username. Leave blank for an anonymous connection.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("MQTT password. Bind this to a Parameter Context (#{mqtt-password}) rather than typing a "
                    + "literal — a sensitive property read back is masked, and a GET-then-PUT would overwrite the real "
                    + "credential with the mask.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile whose metrics were encoded and published as a Sparkplug B NDATA message.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile whose content could not be parsed as metrics, or that could not be published "
                    + "(connect/encode/publish failure). The connection is dropped so the next FlowFile reconnects "
                    + "and re-sends an NBIRTH.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            List.of(BROKER_URI, CLIENT_ID, GROUP_ID, EDGE_NODE_ID, QOS, USERNAME, PASSWORD);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    private final SparkplugMetricParser parser = new SparkplugMetricParser();

    private volatile SparkplugPayloadFactory payloadFactory;
    private volatile MqttPublisher.Factory publisherFactory = PahoMqttPublisher.FACTORY;
    private volatile MqttPublisher publisher;
    private volatile String deathTopic;
    private volatile int sessionQos;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /** Package-private seam so unit tests can inject an in-memory publisher instead of Paho. */
    void setPublisherFactory(final MqttPublisher.Factory factory) {
        this.publisherFactory = factory;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.payloadFactory = new SparkplugPayloadFactory();
        this.publisher = null;
        this.deathTopic = null;
    }

    @OnStopped
    public void onStopped() {
        final MqttPublisher current = this.publisher;
        final String topic = this.deathTopic;
        if (current != null && topic != null && current.isConnected() && payloadFactory != null) {
            try {
                current.publish(topic, payloadFactory.createDeath(), sessionQos, false);
            } catch (final Exception e) {
                getLogger().warn("Failed to publish NDEATH on stop", e);
            }
        }
        resetConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int qos = context.getProperty(QOS).asInteger();
        final String group = context.getProperty(GROUP_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String node = context.getProperty(EDGE_NODE_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String dataTopic = topic("NDATA", group, node);

        try {
            final List<Metric> metrics = parser.parse(readContent(session, flowFile));
            ensureSession(context, group, node, metrics, qos);

            final byte[] data = payloadFactory.createData(metrics);
            final long seq = payloadFactory.lastSeq();
            publisher.publish(dataTopic, data, qos, false);

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("sparkplug.topic", dataTopic);
            attributes.put("sparkplug.message.type", "NDATA");
            attributes.put("sparkplug.seq", String.valueOf(seq));
            attributes.put("sparkplug.bdSeq", String.valueOf(payloadFactory.sessionBdSeq()));
            attributes.put("sparkplug.metric.count", String.valueOf(metrics.size()));
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().send(flowFile, dataTopic);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to publish Sparkplug B message to {}", dataTopic, e);
            // Drop the (possibly broken) connection so the next FlowFile reconnects and rebirths.
            resetConnection();
            flowFile = session.putAttribute(flowFile, "sparkplug.error",
                    e.getMessage() != null ? e.getMessage() : e.toString());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Ensure a live node session: if not connected, begin a new session (advancing bdSeq),
     * connect with the NDEATH will registered, and publish the NBIRTH declaring these metrics.
     * The metric set of the FlowFile that opens the session defines the birth certificate.
     */
    private void ensureSession(final ProcessContext context, final String group, final String node,
                               final List<Metric> metrics, final int qos) throws Exception {
        if (publisher != null && publisher.isConnected()) {
            return;
        }
        resetConnection();

        payloadFactory.beginSession();
        this.sessionQos = qos;
        this.deathTopic = topic("NDEATH", group, node);
        final String birthTopic = topic("NBIRTH", group, node);

        final byte[] deathPayload = payloadFactory.createDeath();
        // Broker/identity/credentials are ENVIRONMENT-scoped: evaluate without a FlowFile. The
        // Group/Edge-Node IDs (FLOWFILE_ATTRIBUTES-scoped) were already resolved by the caller.
        final String password = context.getProperty(PASSWORD).getValue();
        final MqttConnectionConfig config = new MqttConnectionConfig(
                context.getProperty(BROKER_URI).evaluateAttributeExpressions().getValue(),
                context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue(),
                context.getProperty(USERNAME).evaluateAttributeExpressions().getValue(),
                password == null ? null : password.toCharArray(),
                this.deathTopic, deathPayload, qos);

        publisher = publisherFactory.connect(config);

        final byte[] birthPayload = payloadFactory.createBirth(metrics);
        publisher.publish(birthTopic, birthPayload, qos, false);
        getLogger().info("Published NBIRTH to {} (bdSeq={}, metrics={})",
                birthTopic, payloadFactory.sessionBdSeq(), metrics.size());
    }

    private void resetConnection() {
        final MqttPublisher current = this.publisher;
        this.publisher = null;
        if (current != null) {
            current.close();
        }
    }

    private static byte[] readContent(final ProcessSession session, final FlowFile flowFile) throws java.io.IOException {
        try (final InputStream in = session.read(flowFile)) {
            return in.readAllBytes();
        }
    }

    private static String topic(final String type, final String group, final String node) {
        return NAMESPACE + "/" + group + "/" + type + "/" + node;
    }
}
