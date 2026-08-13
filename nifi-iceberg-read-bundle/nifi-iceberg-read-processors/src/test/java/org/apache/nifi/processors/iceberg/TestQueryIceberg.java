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
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestQueryIceberg {

    /**
     * A deliberately mixed-type schema so the pushdown translator and the Iceberg→Record converter
     * are exercised across string/int/long/double/float/decimal/boolean columns, plus one
     * nullable (optional) column so the null-sensitive branches (IS NULL, negation on nullable)
     * are covered too.
     */
    private static final Schema AIRLINES_SCHEMA = new Schema(
            required(1, "carrier_code", Types.StringType.get()),
            required(2, "airline_name", Types.StringType.get()),
            required(3, "country", Types.StringType.get()),
            required(4, "fleet_size", Types.IntegerType.get()),
            required(5, "founded", Types.LongType.get()),
            required(6, "revenue", Types.DoubleType.get()),
            required(7, "rating", Types.FloatType.get()),
            required(8, "active", Types.BooleanType.get()),
            required(9, "margin", Types.DecimalType.of(9, 2)),
            optional(10, "hub", Types.StringType.get()));

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
     * BA's hub is left null so the IS NULL / negation-on-nullable paths have data to match.
     */
    private void seedAirlinesTable(String warehouse) throws IOException {
        try (HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse)) {
            final Table table = catalog.createTable(TableIdentifier.of("demo", "airlines"), AIRLINES_SCHEMA, PartitionSpec.unpartitioned());
            appendFile(table, List.of(
                    new Object[]{"AA", "American Airlines", "USA", 900, 1926L, 52.0d, 4.5f, true, new BigDecimal("12.50"), "DFW"},
                    new Object[]{"DL", "Delta Air Lines", "USA", 750, 1928L, 47.0d, 4.2f, true, new BigDecimal("9.75"), "ATL"}));
            appendFile(table, List.of(
                    new Object[]{"BA", "British Airways", "UK", 250, 1974L, 15.0d, 3.8f, false, new BigDecimal("5.25"), null},
                    new Object[]{"UA", "United Airlines", "USA", 800, 1926L, 44.0d, 4.0f, true, new BigDecimal("8.00"), "ORD"}));
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
            record.setField("founded", row[4]);
            record.setField("revenue", row[5]);
            record.setField("rating", row[6]);
            record.setField("active", row[7]);
            record.setField("margin", row[8]);
            record.setField("hub", row[9]);
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

    /** Seeds demo.airlines and wires up the catalog service + record writer on a fresh runner. */
    private void seedAndConfigure() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
    }

    /** Seeds the table, configures the runner, sets one query property and runs a single trigger. */
    private MockFlowFile runSingleQuery(String relationship, String sql) throws Exception {
        seedAndConfigure();
        runner.setProperty(relationship, sql);

        runner.run(1);

        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        runner.assertTransferCount(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    /** Asserts exactly one FlowFile reached the relationship and returns it. */
    private MockFlowFile result(String relationship) {
        runner.assertTransferCount(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    private static int recordCount(MockFlowFile flowFile) {
        return Integer.parseInt(flowFile.getAttribute("record.count"));
    }

    private static void assertPushed(MockFlowFile flowFile, String column) {
        final String pushed = flowFile.getAttribute(QueryIceberg.ATTR_PUSHED_FILTER);
        assertTrue(pushed != null && pushed.contains(column),
                "expected the predicate on '" + column + "' to be pushed into the Iceberg scan, got: " + pushed);
    }

    private static void assertResidual(MockFlowFile flowFile) {
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_PUSHED_FILTER, "");
    }

    @Test
    public void testSelectAll() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("all", "SELECT * FROM airlines");

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
        final MockFlowFile flowFile = runSingleQuery("codes", "SELECT carrier_code FROM airlines");

        flowFile.assertAttributeEquals("record.count", "4");
        flowFile.assertAttributeEquals(QueryIceberg.ATTR_PUSHED_COLUMNS, "carrier_code");

        final String content = flowFile.getContent();
        assertTrue(content.contains("AA"));
        assertFalse(content.contains("American Airlines"));
    }

    @Test
    public void testFilterEqualIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("aa", "SELECT * FROM airlines WHERE carrier_code = 'AA'");

        flowFile.assertAttributeEquals("record.count", "1");
        assertPushed(flowFile, "carrier_code");

        final String content = flowFile.getContent();
        assertTrue(content.contains("American Airlines"));
        assertFalse(content.contains("Delta Air Lines"));
    }

    @Test
    public void testPushdownSkipsDataFiles() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("aa", "SELECT * FROM airlines WHERE carrier_code = 'AA'");

        final String skipped = flowFile.getAttribute(QueryIceberg.ATTR_SKIPPED_DATA_FILES);
        assertTrue(skipped != null && Long.parseLong(skipped) >= 1,
                "expected the pushed filter to skip at least one data file, got: " + skipped);
    }

    @Test
    public void testAggregation() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("stats",
                "SELECT country, COUNT(*) AS airline_count FROM airlines GROUP BY country");

        flowFile.assertAttributeEquals("record.count", "2");

        final String content = flowFile.getContent();
        assertTrue(content.contains("USA"));
        assertTrue(content.contains("UK"));
        assertTrue(content.contains("3"));
    }

    @Test
    public void testResidualFilterStillCorrect() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("aa", "SELECT * FROM airlines WHERE LOWER(carrier_code) = 'aa'");

        flowFile.assertAttributeEquals("record.count", "1");
        assertResidual(flowFile);

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

    // --- pushdown translator coverage: numeric comparison operators over the integer column ---

    @Test
    public void testIntegerComparisonsArePushed() throws Exception {
        seedAndConfigure();
        runner.setProperty("gt", "SELECT * FROM airlines WHERE fleet_size > 700");
        runner.setProperty("ge", "SELECT * FROM airlines WHERE fleet_size >= 900");
        runner.setProperty("lt", "SELECT * FROM airlines WHERE fleet_size < 300");
        runner.setProperty("le", "SELECT * FROM airlines WHERE fleet_size <= 250");

        runner.run(1);

        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        for (final String rel : List.of("gt", "ge", "lt", "le")) {
            assertPushed(result(rel), "fleet_size");
        }
        assertEquals(3, recordCount(result("gt")));
        assertEquals(1, recordCount(result("ge")));
        assertEquals(1, recordCount(result("lt")));
        assertEquals(1, recordCount(result("le")));
    }

    @Test
    public void testNotEqualsOnRequiredColumnIsPushed() throws Exception {
        // carrier_code is a required (non-null) column, so <> is safe to push
        final MockFlowFile flowFile = runSingleQuery("ne", "SELECT * FROM airlines WHERE carrier_code <> 'AA'");
        assertEquals(3, recordCount(flowFile));
        assertPushed(flowFile, "carrier_code");
    }

    @Test
    public void testLongAndDoubleAndDecimalLiteralsArePushed() throws Exception {
        seedAndConfigure();
        runner.setProperty("byFounded", "SELECT * FROM airlines WHERE founded = 1974");
        runner.setProperty("byRevenue", "SELECT * FROM airlines WHERE revenue > 45.0");
        runner.setProperty("byMargin", "SELECT * FROM airlines WHERE margin > 8.00");

        runner.run(1);

        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        assertEquals(1, recordCount(result("byFounded")));
        assertPushed(result("byFounded"), "founded");
        assertEquals(2, recordCount(result("byRevenue")));
        assertPushed(result("byRevenue"), "revenue");
        assertEquals(2, recordCount(result("byMargin")));
        assertPushed(result("byMargin"), "margin");
    }

    @Test
    public void testExactFloatLiteralIsPushedButInexactStaysResidual() throws Exception {
        seedAndConfigure();
        // 4.0 is exactly representable as a float, so it pushes
        runner.setProperty("floatExact", "SELECT * FROM airlines WHERE rating >= 4.0");
        // 4.1 is NOT exactly representable as a float, so the translator declines it and Calcite
        // evaluates it as a residual filter — still the correct rows, just not pushed
        runner.setProperty("floatInexact", "SELECT * FROM airlines WHERE rating > 4.1");

        runner.run(1);

        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile exact = result("floatExact");
        assertEquals(3, recordCount(exact));
        assertPushed(exact, "rating");

        final MockFlowFile inexact = result("floatInexact");
        assertEquals(2, recordCount(inexact));
        assertResidual(inexact);
    }

    @Test
    public void testNonIntegerLiteralAgainstIntegerColumnStaysResidual() throws Exception {
        // 700.5 cannot be represented exactly as the int column's type, so the equality/range
        // translation declines (ArithmeticException path) and the filter stays residual
        final MockFlowFile flowFile = runSingleQuery("fractional", "SELECT * FROM airlines WHERE fleet_size > 700.5");
        assertEquals(3, recordCount(flowFile));
        assertResidual(flowFile);
    }

    // --- SEARCH normalization: IN lists, NOT IN, and BETWEEN ranges ---

    @Test
    public void testInListIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("inList", "SELECT * FROM airlines WHERE carrier_code IN ('AA', 'UA')");
        assertEquals(2, recordCount(flowFile));
        assertPushed(flowFile, "carrier_code");
    }

    @Test
    public void testNotInOnRequiredColumnIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("notInList", "SELECT * FROM airlines WHERE carrier_code NOT IN ('AA', 'DL')");
        assertEquals(2, recordCount(flowFile));
        assertPushed(flowFile, "carrier_code");
    }

    @Test
    public void testBetweenRangeIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("range", "SELECT * FROM airlines WHERE fleet_size BETWEEN 300 AND 800");
        assertEquals(2, recordCount(flowFile));
        assertPushed(flowFile, "fleet_size");
    }

    // --- null checks over the nullable hub column ---

    @Test
    public void testIsNullAndIsNotNullArePushed() throws Exception {
        seedAndConfigure();
        runner.setProperty("noHub", "SELECT * FROM airlines WHERE hub IS NULL");
        runner.setProperty("withHub", "SELECT * FROM airlines WHERE hub IS NOT NULL");

        runner.run(1);

        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
        final MockFlowFile isNull = result("noHub");
        assertEquals(1, recordCount(isNull));
        assertPushed(isNull, "hub");

        final MockFlowFile notNull = result("withHub");
        assertEquals(3, recordCount(notNull));
        assertPushed(notNull, "hub");
    }

    @Test
    public void testNotEqualsOnNullableColumnStaysResidual() throws Exception {
        // hub is nullable: SQL's <> excludes NULL rows while Iceberg's notEqual would include
        // them, so the translator declines and Calcite applies the residual
        final MockFlowFile flowFile = runSingleQuery("notAtl", "SELECT * FROM airlines WHERE hub <> 'ATL'");
        assertEquals(2, recordCount(flowFile));
        assertResidual(flowFile);
    }

    // --- LIKE handling ---

    @Test
    public void testPrefixLikeIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("aNames", "SELECT * FROM airlines WHERE airline_name LIKE 'A%'");
        assertEquals(1, recordCount(flowFile));
        assertPushed(flowFile, "airline_name");
        assertTrue(flowFile.getContent().contains("American Airlines"));
    }

    @Test
    public void testWildcardFreeLikeBecomesEquality() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("exactName", "SELECT * FROM airlines WHERE airline_name LIKE 'Delta Air Lines'");
        assertEquals(1, recordCount(flowFile));
        assertPushed(flowFile, "airline_name");
    }

    @Test
    public void testNonPrefixLikeStaysResidual() throws Exception {
        // a leading-wildcard pattern has no Iceberg equivalent, so it is left as a residual
        final MockFlowFile flowFile = runSingleQuery("suffix", "SELECT * FROM airlines WHERE airline_name LIKE '%Airlines'");
        assertEquals(2, recordCount(flowFile));
        assertResidual(flowFile);
    }

    // --- boolean column predicates ---

    @Test
    public void testBareBooleanColumnIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("activeOnes", "SELECT * FROM airlines WHERE active");
        assertEquals(3, recordCount(flowFile));
        assertPushed(flowFile, "active");
    }

    @Test
    public void testBooleanEqualsLiteralIsCorrect() throws Exception {
        // Calcite rewrites "active = FALSE" into "NOT active" on a nullable-agnostic form that the
        // translator leaves residual; either way the result must be exactly the one inactive row
        final MockFlowFile flowFile = runSingleQuery("inactive", "SELECT * FROM airlines WHERE active = FALSE");
        assertEquals(1, recordCount(flowFile));
        assertTrue(flowFile.getContent().contains("British Airways"));
    }

    // --- AND / OR / NOT composites ---

    @Test
    public void testAndCompositeIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("bigUsa", "SELECT * FROM airlines WHERE fleet_size > 700 AND country = 'USA'");
        assertEquals(3, recordCount(flowFile));
        assertPushed(flowFile, "fleet_size");
        assertPushed(flowFile, "country");
    }

    @Test
    public void testOrCompositeIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("bigOrUk", "SELECT * FROM airlines WHERE fleet_size > 850 OR country = 'UK'");
        assertEquals(2, recordCount(flowFile));
        assertPushed(flowFile, "fleet_size");
    }

    @Test
    public void testCompositeWithOneNonPushableConjunctStaysResidual() throws Exception {
        // the whole AND has no partial Iceberg form (LOWER(country) is not translatable), so the
        // entire predicate stays residual, but the result is still correct
        final MockFlowFile flowFile = runSingleQuery("mixed", "SELECT * FROM airlines WHERE fleet_size > 700 AND LOWER(country) = 'usa'");
        assertEquals(3, recordCount(flowFile));
        assertResidual(flowFile);
    }

    @Test
    public void testNotOnRequiredColumnIsPushed() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("notAa", "SELECT * FROM airlines WHERE NOT (carrier_code = 'AA')");
        assertEquals(3, recordCount(flowFile));
        assertPushed(flowFile, "carrier_code");
    }

    @Test
    public void testNotOnNullableColumnStaysResidual() throws Exception {
        final MockFlowFile flowFile = runSingleQuery("notDfw", "SELECT * FROM airlines WHERE NOT (hub = 'DFW')");
        assertEquals(2, recordCount(flowFile));
        assertResidual(flowFile);
    }

    // testInvalidSql / validation

    @Test
    public void testZeroRecordFlowFileSuppressed() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty(QueryIceberg.INCLUDE_ZERO_RECORD_FLOWFILES, "false");
        runner.setProperty("none", "SELECT * FROM airlines WHERE carrier_code = 'ZZ'");

        runner.run(1);

        runner.assertTransferCount("none", 0);
        runner.assertTransferCount(QueryIceberg.REL_FAILURE, 0);
    }

    @Test
    public void testInvalidSqlFailsValidation() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        runner.setProperty("broken", "SELExT bogus FROM");

        runner.assertNotValid();
    }

    @Test
    public void testNoQueryPropertyFailsValidation() throws Exception {
        final String warehouse = warehousePath.toUri().toString();
        seedAirlinesTable(warehouse);
        configureRunner(warehouse);
        // only a catalog.* override, no actual query property
        runner.setProperty("catalog.client.region", "us-east-1");

        runner.assertNotValid();
    }
}
