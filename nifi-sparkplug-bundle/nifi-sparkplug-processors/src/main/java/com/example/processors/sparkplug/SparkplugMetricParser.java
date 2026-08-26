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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.tahu.SparkplugException;
import org.eclipse.tahu.message.model.Metric;
import org.eclipse.tahu.message.model.Metric.MetricBuilder;
import org.eclipse.tahu.message.model.MetricDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Turns a FlowFile's JSON body into a list of Sparkplug B {@link Metric}s.
 *
 * <p>The accepted shape is a flat JSON object of <em>metric name → value</em>, with the
 * Sparkplug data type inferred from the JSON type:</p>
 * <pre>
 * {
 *   "Sensors/Temperature": 22.5,   // floating point  -> Double
 *   "Sensors/Count":       1013,   // integral number -> Int64
 *   "Sensors/Online":      true,   // boolean          -> Boolean
 *   "Sensors/Label":       "ok"    // text             -> String
 * }
 * </pre>
 *
 * <p>Metric names may contain {@code /} to express the Sparkplug folder convention. A null JSON
 * value, an array, or a nested object is rejected — declare a concrete typed value instead.</p>
 */
public class SparkplugMetricParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public List<Metric> parse(byte[] content) throws IOException, SparkplugException {
        if (content == null || content.length == 0) {
            throw new IOException("FlowFile content is empty; expected a JSON object of metric name -> value");
        }
        final JsonNode root = mapper.readTree(content);
        if (root == null || !root.isObject()) {
            throw new IOException("Sparkplug metric content must be a JSON object of metric name -> value");
        }
        final List<Metric> metrics = new ArrayList<>();
        for (final Map.Entry<String, JsonNode> field : root.properties()) {
            metrics.add(toMetric(field.getKey(), field.getValue()));
        }
        if (metrics.isEmpty()) {
            throw new IOException("Sparkplug metric object contained no metrics");
        }
        return metrics;
    }

    private Metric toMetric(String name, JsonNode value) throws IOException, SparkplugException {
        if (value.isBoolean()) {
            return new MetricBuilder(name, MetricDataType.Boolean, value.booleanValue()).createMetric();
        }
        if (value.isIntegralNumber()) {
            return new MetricBuilder(name, MetricDataType.Int64, value.longValue()).createMetric();
        }
        if (value.isFloatingPointNumber()) {
            return new MetricBuilder(name, MetricDataType.Double, value.doubleValue()).createMetric();
        }
        if (value.isTextual()) {
            return new MetricBuilder(name, MetricDataType.String, value.textValue()).createMetric();
        }
        throw new IOException("Unsupported value type for metric '" + name + "': " + value.getNodeType()
                + " (expected boolean, number, or string)");
    }
}
