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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.apache.nifi.services.iceberg.IcebergCatalogType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGetIceberg {

    private static final Schema AIRLINES_SCHEMA = new Schema(
            required(1, "carrier_code", Types.StringType.get()),
            required(2, "airline_name", Types.StringType.get()),
            required(3, "country", Types.StringType.get()));

    @TempDir
    private Path warehousePath;

    private TestRunner runner;

    public static class HadoopCatalogServiceStub extends AbstractControllerService implements IcebergCatalogService {

        private final String warehouseLocation;

        public HadoopCatalogServiceStub(String warehouseLocation) {
            this.warehouseLocation = warehouseLocation;
        }

        @Override
        public IcebergCatalogType getCatalogType() {
            return IcebergCatalogType.HADOOP;
        }

        @Override
        public Map<IcebergCatalogProperty, Object> getCatalogProperties() {
            return Map.of(IcebergCatalogProperty.WAREHOUSE_LOCATION, warehouseLocation);
        }

        @Override
        public List<String> getConfigFilePaths() {
            return null;
        }
    }

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(GetIceberg.class);
    }

    private void seedAirlinesTable(String warehouse) throws IOException {
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
            final Table table = catalog.createTable(TableIdentifier.of("demo", "airlines"), AIRLINES_SCHEMA, PartitionSpec.unpartitioned());

            final OutputFile outputFile = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".parquet");
            final DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
                    .schema(AIRLINES_SCHEMA)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .withSpec(PartitionSpec.unpartitioned())
                    .overwrite()
                    .build();

            for (final String[] row : List.of(
                    new String[]{"AA", "American Airlines", "USA"},
                    new String[]{"DL", "Delta Air Lines", "USA"},
                    new String[]{"UA", "United Airlines", "USA"})) {
                final GenericRecord record = GenericRecord.create(AIRLINES_SCHEMA);
                record.setField("carrier_code", row[0]);
                record.setField("airline_name", row[1]);
                record.setField("country", row[2]);
                dataWriter.write(record);
            }
            dataWriter.close();

            table.newAppend().appendFile(dataWriter.toDataFile()).commit();
        }
    }

    private void configureRunner(String warehouse) throws InitializationException {
        final HadoopCatalogServiceStub catalogService = new HadoopCatalogServiceStub(warehouse);
        runner.addControllerService("catalog-service", catalogService);
        runner.enableControllerService(catalogService);

        final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(GetIceberg.CATALOG, "catalog-service");
        runner.setProperty(GetIceberg.RECORD_WRITER, "record-writer");
        runner.setProperty(GetIceberg.CATALOG_NAMESPACE, "demo");
        runner.setProperty(GetIceberg.TABLE_NAME, "airlines");
    }

    @Test
    public void testReadsAllRows() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetIceberg.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "3");
        flowFile.assertAttributeEquals(GetIceberg.ICEBERG_CATALOG_NAMESPACE, "demo");
        flowFile.assertAttributeEquals(GetIceberg.ICEBERG_TABLE_NAME, "airlines");
        flowFile.assertAttributeEquals("mime.type", "application/json");

        final String content = flowFile.getContent();
        assertTrue(content.contains("American Airlines"));
        assertTrue(content.contains("Delta Air Lines"));
        assertTrue(content.contains("United Airlines"));
    }

    @Test
    public void testColumnProjection() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty(GetIceberg.COLUMNS, "carrier_code");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetIceberg.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "3");

        final String content = flowFile.getContent();
        assertTrue(content.contains("AA"));
        assertFalse(content.contains("American Airlines"));
    }

    @Test
    public void testMissingTableRoutesToFailure() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty(GetIceberg.TABLE_NAME, "does_not_exist");

        runner.run(1);

        runner.assertTransferCount(GetIceberg.REL_SUCCESS, 0);
        runner.assertTransferCount(GetIceberg.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(GetIceberg.REL_FAILURE).get(0);
        failed.assertAttributeExists("iceberg.read.error");
        failed.assertAttributeEquals(GetIceberg.ICEBERG_TABLE_NAME, "does_not_exist");
    }
}
