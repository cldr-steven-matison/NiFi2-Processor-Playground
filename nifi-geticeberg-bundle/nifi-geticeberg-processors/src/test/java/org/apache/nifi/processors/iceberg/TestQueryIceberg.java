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
package org.apache.nifi.processors.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processors.iceberg.TestGetIceberg.HadoopCatalogServiceStub;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQueryIceberg {

    private static final Schema AIRLINES_SCHEMA = new Schema(
            required(1, "carrier_code", Types.StringType.get()),
            required(2, "airline_name", Types.StringType.get()),
            required(3, "country", Types.StringType.get()),
            required(4, "fleet_size", Types.IntegerType.get()));

    @TempDir
    private Path warehousePath;

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(QueryIceberg.class);
    }

    /**
     * Seeds demo.airlines as TWO separate data files with disjoint carrier_code ranges, so a
     * pushed filter can provably skip a file: file 1 holds AA/DL, file 2 holds BA/UA.
     */
    private void seedAirlinesTable(String warehouse) throws IOException {
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
            final Table table = catalog.createTable(TableIdentifier.of("demo", "airlines"), AIRLINES_SCHEMA, PartitionSpec.unpartitioned());
            appendFile(table, List.of(
                    new Object[]{"AA", "American Airlines", "USA", 900},
                    new Object[]{"DL", "Delta Air Lines", "USA", 750}));
            appendFile(table, List.of(
                    new Object[]{"BA", "British Airways", "UK", 250},
                    new Object[]{"UA", "United Airlines", "USA", 800}));
        }
    }

    private void appendFile(Table table, List<Object[]> rows) throws IOException {
        final OutputFile outputFile = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".parquet");
        final DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
                .schema(AIRLINES_SCHEMA)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .withSpec(PartitionSpec.unpartitioned())
                .overwrite()
                .build();

        for (final Object[] row : rows) {
            final GenericRecord record = GenericRecord.create(AIRLINES_SCHEMA);
            record.setField("carrier_code", row[0]);
            record.setField("airline_name", row[1]);
            record.setField("country", row[2]);
            record.setField("fleet_size", row[3]);
            dataWriter.write(record);
        }
        dataWriter.close();

        table.newAppend().appendFile(dataWriter.toDataFile()).commit();
    }

    private void configureRunner(String warehouse) throws InitializationException {
        final HadoopCatalogServiceStub catalogService = new HadoopCatalogServiceStub(warehouse);
        runner.addControllerService("catalog-service", catalogService);
        runner.enableControllerService(catalogService);

        final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(QueryIceberg.CATALOG, "catalog-service");
        runner.setProperty(QueryIceberg.RECORD_WRITER, "record-writer");
        runner.setProperty(QueryIceberg.CATALOG_NAMESPACE, "demo");
        runner.setProperty(QueryIceberg.TABLE_NAME, "airlines");
    }

    @Test
    public void testSelectAll() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("all", "SELECT * FROM airlines");

        runner.run(1);

        runner.assertTransferCount("all", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("all").get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_QUERY_NAME, "all");
        flowFile.assertAttributeEquals(GetIceberg.ICEBERG_CATALOG_NAMESPACE, "demo");
        flowFile.assertAttributeEquals(GetIceberg.ICEBERG_TABLE_NAME, "airlines");
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_PUSHED_FILTER, "");

        final String content = flowFile.getContent();
        assertTrue(content.contains("American Airlines"));
        assertTrue(content.contains("Delta Air Lines"));
        assertTrue(content.contains("British Airways"));
        assertTrue(content.contains("United Airlines"));
    }

    @Test
    public void testProjectionIsPushed() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("codes", "SELECT carrier_code FROM airlines");

        runner.run(1);

        runner.assertTransferCount("codes", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("codes").get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_PUSHED_COLUMNS, "carrier_code");

        final String content = flowFile.getContent();
        assertTrue(content.contains("AA"));
        assertFalse(content.contains("American Airlines"));
    }

    @Test
    public void testFilterEqualIsPushed() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("aa", "SELECT * FROM airlines WHERE carrier_code = 'AA'");

        runner.run(1);

        runner.assertTransferCount("aa", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("aa").get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        final String pushedFilter = flowFile.getAttribute(QueryIceberg.ATTR_PUSHED_FILTER);
        assertTrue(pushedFilter != null && pushedFilter.contains("carrier_code"),
                "expected the equality predicate to be pushed into the Iceberg scan, got: " + pushedFilter);

        final String content = flowFile.getContent();
        assertTrue(content.contains("American Airlines"));
        assertFalse(content.contains("Delta Air Lines"));
    }

    @Test
    public void testPushdownSkipsDataFiles() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("aa", "SELECT * FROM airlines WHERE carrier_code = 'AA'");

        runner.run(1);

        runner.assertTransferCount("aa", 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("aa").get(0);
        final String skipped = flowFile.getAttribute(QueryIceberg.ATTR_SKIPPED_DATA_FILES);
        assertTrue(skipped != null && Long.parseLong(skipped) >= 1,
                "expected the pushed filter to skip at least one data file, got: " + skipped);
    }

    @Test
    public void testAggregation() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("stats", "SELECT country, COUNT(*) AS airline_count FROM airlines GROUP BY country");

        runner.run(1);

        runner.assertTransferCount("stats", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("stats").get(0);
        flowFile.assertAttributeEquals("record.count", "2");

        final String content = flowFile.getContent();
        assertTrue(content.contains("USA"));
        assertTrue(content.contains("UK"));
        assertTrue(content.contains("3"));
    }

    @Test
    public void testResidualFilterStillCorrect() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("aa", "SELECT * FROM airlines WHERE LOWER(carrier_code) = 'aa'");

        runner.run(1);

        runner.assertTransferCount("aa", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("aa").get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_PUSHED_FILTER, "");

        final String content = flowFile.getContent();
        assertTrue(content.contains("American Airlines"));
        assertFalse(content.contains("United Airlines"));
    }

    @Test
    public void testMultipleQueriesRouteToNamedRelationships() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("aa", "SELECT * FROM airlines WHERE carrier_code = 'AA'");
        runner.setProperty("stats", "SELECT country, COUNT(*) AS airline_count FROM airlines GROUP BY country");
        // catalog overrides must not become relationships (the Hadoop stub simply ignores them)
        runner.setProperty("catalog.client.region", "us-east-1");

        assertFalse(runner.getProcessor().getRelationships().stream().anyMatch(r -> r.getName().equals("catalog.client.region")));

        runner.run(1);

        runner.assertTransferCount("aa", 1);
        runner.assertTransferCount("stats", 1);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        runner.getFlowFilesForRelationship("aa").get(0).assertAttributeEquals(QueryIceberg.ATTR_QUERY_NAME, "aa");
        runner.getFlowFilesForRelationship("stats").get(0).assertAttributeEquals(QueryIceberg.ATTR_QUERY_NAME, "stats");
    }

    @Test
    public void testBadTableRoutesToFailure() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty(QueryIceberg.TABLE_NAME, "does_not_exist");
        runner.setProperty("all", "SELECT * FROM does_not_exist");

        runner.run(1);

        runner.assertTransferCount("all", 0);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(QueryIceberg.REL_FAILURE).get(0);
        failed.assertAttributeExists(QueryIceberg.ATTR_QUERY_ERROR);
        failed.assertAttributeEquals(GetIceberg.ICEBERG_TABLE_NAME, "does_not_exist");
    }
}
