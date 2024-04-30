/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;

/** Unit tests for the {@link PostTransformOperator}. */
public class PostTransformOperatorTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col12", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId DATATYPE_TABLEID =
            TableId.tableId("my_company", "my_branch", "data_types");
    private static final Schema DATATYPE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("colString", DataTypes.STRING())
                    .physicalColumn("colBoolean", DataTypes.BOOLEAN())
                    .physicalColumn("colTinyint", DataTypes.TINYINT())
                    .physicalColumn("colSmallint", DataTypes.SMALLINT())
                    .physicalColumn("colInt", DataTypes.INT())
                    .physicalColumn("colBigint", DataTypes.BIGINT())
                    .physicalColumn("colDate", DataTypes.DATE())
                    .physicalColumn("colTime", DataTypes.TIME())
                    .physicalColumn("colTimestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("colFloat", DataTypes.FLOAT())
                    .physicalColumn("colDouble", DataTypes.DOUBLE())
                    .physicalColumn("colDecimal", DataTypes.DECIMAL(6, 2))
                    .primaryKey("colString")
                    .build();

    private static final TableId METADATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_table");
    private static final Schema METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECTED_METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("identifier_name", DataTypes.STRING())
                    .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                    .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                    .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                    .primaryKey("col1")
                    .build();

    private static final TableId METADATA_AS_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_as_table");
    private static final Schema METADATA_AS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("sid", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("name_upper", DataTypes.STRING())
                    .physicalColumn("tbname", DataTypes.STRING())
                    .primaryKey("sid")
                    .build();

    private static final TableId TIMESTAMP_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestamp_table");
    private static final Schema TIMESTAMP_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("time_equal", DataTypes.INT())
                    .physicalColumn("timestamp_equal", DataTypes.INT())
                    .physicalColumn("date_equal", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMESTAMPDIFF_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestampdiff_table");
    private static final Schema TIMESTAMPDIFF_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("second_diff", DataTypes.INT())
                    .physicalColumn("minute_diff", DataTypes.INT())
                    .physicalColumn("hour_diff", DataTypes.INT())
                    .physicalColumn("day_diff", DataTypes.INT())
                    .physicalColumn("month_diff", DataTypes.INT())
                    .physicalColumn("year_diff", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId CONDITION_TABLEID =
            TableId.tableId("my_company", "my_branch", "condition_table");
    private static final Schema CONDITION_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("condition_result", DataTypes.BOOLEAN())
                    .primaryKey("col1")
                    .build();

    private static final TableId REDUCE_TABLEID =
            TableId.tableId("my_company", "my_branch", "reduce_table");

    private static final Schema REDUCE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("ref2", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_REDUCE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("uid", DataTypes.STRING())
                    .physicalColumn("newage", DataTypes.INT())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("seventeen", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final TableId WILDCARD_TABLEID =
            TableId.tableId("my_company", "my_branch", "wildcard_table");

    private static final Schema WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("newage", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    @Test
    void testDataChangeEventTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1,col2) col12",
                                "col1 = '1'")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("12")
                                }));
        // Insert will be ignored
        DataChangeEvent insertEventIgnored =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), new BinaryStringData("2"), null
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("3"), null
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("12")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    new BinaryStringData("13")
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }

    @Test
    void testDataChangeEventTransformTwice() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1, '1') col12",
                                "col1 = '1'")
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1, '2') col12",
                                "col1 = '2'")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("11")
                                }));
        // Insert
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEvent2Expect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("22")
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("3"), null
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("11")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    new BinaryStringData("11")
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEvent2Expect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }

    @Test
    void testDataChangeEventTransformProjectionDataTypeConvert() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(DATATYPE_TABLEID.identifier(), "*", null, null, null, null)
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(DATATYPE_TABLEID, DATATYPE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) DATATYPE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        DATATYPE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3.14"),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Integer(1),
                                    new Long(1),
                                    new Integer(1704471599),
                                    new Integer(1704471599),
                                    TimestampData.fromMillis(1704471599),
                                    new Float(3.14f),
                                    new Double(3.14d),
                                    DecimalData.fromBigDecimal(new BigDecimal(3.14), 6, 2),
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(DATATYPE_TABLEID, DATATYPE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEvent));
    }

    @Test
    void testMetadataTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_TABLEID.identifier(),
                                "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name, __namespace_name__, __schema_name__, __table_name__",
                                " __table_name__ = 'metadata_table' ")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(METADATA_TABLEID, METADATA_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) METADATA_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_METADATA_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        recordDataGenerator.generate(new Object[] {new BinaryStringData("1")}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("my_company.my_branch.metadata_table"),
                                    new BinaryStringData("my_company"),
                                    new BinaryStringData("my_branch"),
                                    new BinaryStringData("metadata_table")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(METADATA_TABLEID, EXPECTED_METADATA_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testMetadataASTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_AS_TABLEID.identifier(),
                                "sid, name, UPPER(name) as name_upper, __table_name__ as tbname",
                                "sid < 3")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(METADATA_AS_TABLEID, METADATA_AS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) METADATA_AS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        METADATA_AS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {1, new BinaryStringData("abc"), null, null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_AS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    new BinaryStringData("abc"),
                                    new BinaryStringData("ABC"),
                                    new BinaryStringData("metadata_as_table")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(METADATA_AS_TABLEID, METADATA_AS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testTimestampTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMP_TABLEID.identifier(),
                                "col1, IF(LOCALTIME = CURRENT_TIME, 1, 0) as time_equal,"
                                        + " IF(LOCALTIMESTAMP = CURRENT_TIMESTAMP, 1, 0) as timestamp_equal,"
                                        + " IF(TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) = CURRENT_DATE, 1, 0) as date_equal",
                                "LOCALTIMESTAMP = CURRENT_TIMESTAMP")
                        .addTimezone("GMT")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMP_TABLEID, TIMESTAMP_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMP_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), null, null, null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), 1, 1, 1}));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMESTAMP_TABLEID, TIMESTAMP_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testTimestampDiffTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMPDIFF_TABLEID.identifier(),
                                "col1, TIMESTAMP_DIFF('SECOND', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as second_diff,"
                                        + " TIMESTAMP_DIFF('MINUTE', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as minute_diff,"
                                        + " TIMESTAMP_DIFF('HOUR', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as hour_diff,"
                                        + " TIMESTAMP_DIFF('DAY', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as day_diff,"
                                        + " TIMESTAMP_DIFF('MONTH', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as month_diff,"
                                        + " TIMESTAMP_DIFF('YEAR', LOCALTIMESTAMP, CURRENT_TIMESTAMP) as year_diff",
                                null)
                        .addTimezone("GMT-8:00")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMPDIFF_TABLEID, TIMESTAMPDIFF_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMPDIFF_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), null, null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), -28800, -480, -8, 0, 0, 0
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMESTAMPDIFF_TABLEID, TIMESTAMPDIFF_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testBuildInFunctionTransform() throws Exception {
        testExpressionConditionTransform(
                "TO_TIMESTAMP('1970-01-01 00:00:00') = TO_TIMESTAMP('1970-01-01', 'yyyy-MM-dd')");
        testExpressionConditionTransform(
                "TIMESTAMP_DIFF('DAY', TO_TIMESTAMP('1970-01-01 00:00:00'), TO_TIMESTAMP('1970-01-02 00:00:00')) = 1");
        testExpressionConditionTransform("2 between 1 and 3");
        testExpressionConditionTransform("4 not between 1 and 3");
        testExpressionConditionTransform("2 in (1, 2, 3)");
        testExpressionConditionTransform("4 not in (1, 2, 3)");
        testExpressionConditionTransform("CHAR_LENGTH('abc') = 3");
        testExpressionConditionTransform("trim(' abc ') = 'abc'");
        testExpressionConditionTransform("REGEXP_REPLACE('123abc', '[a-zA-Z]', '') = '123'");
        testExpressionConditionTransform("concat('123', 'abc') = '123abc'");
        testExpressionConditionTransform("upper('abc') = 'ABC'");
        testExpressionConditionTransform("lower('ABC') = 'abc'");
        testExpressionConditionTransform("SUBSTR('ABC', 1, 1) = 'B'");
        testExpressionConditionTransform("'ABC' like '^[a-zA-Z]'");
        testExpressionConditionTransform("'123' not like '^[a-zA-Z]'");
        testExpressionConditionTransform("abs(2) = 2");
        testExpressionConditionTransform("ceil(2.4) = 3.0");
        testExpressionConditionTransform("floor(2.5) = 2.0");
        testExpressionConditionTransform("round(3.1415926,2) = 3.14");
        testExpressionConditionTransform("IF(2>0,1,0) = 1");
        testExpressionConditionTransform("COALESCE(null,1,2) = 1");
        testExpressionConditionTransform("1 + 1 = 2");
        testExpressionConditionTransform("1 - 1 = 0");
        testExpressionConditionTransform("1 * 1 = 1");
        testExpressionConditionTransform("3 % 2 = 1");
        testExpressionConditionTransform("1 < 2");
        testExpressionConditionTransform("1 <= 1");
        testExpressionConditionTransform("1 > 0");
        testExpressionConditionTransform("1 >= 1");
        testExpressionConditionTransform(
                "case 1 when 1 then 'a' when 2 then 'b' else 'c' end = 'a'");
        testExpressionConditionTransform("case col1 when '1' then true else false end");
        testExpressionConditionTransform("case when col1 = '1' then true else false end");
    }

    private void testExpressionConditionTransform(String expression) throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CONDITION_TABLEID.identifier(),
                                "col1, IF(" + expression + ", true, false) as condition_result",
                                expression)
                        .addTimezone("GMT")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CONDITION_TABLEID, CONDITION_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CONDITION_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CONDITION_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CONDITION_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), true}));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CONDITION_TABLEID, CONDITION_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    public void testReduceSchemaTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                REDUCE_TABLEID.identifier(),
                                "id, upper(id) as uid, age + 1 as newage, lower(ref1) as ref1, 17 as seventeen",
                                "newage > 17 and ref2 > 17")
                        .addTimezone("GMT")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(REDUCE_TABLEID, REDUCE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) REDUCE_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_REDUCE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        REDUCE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }));

        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        REDUCE_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    18,
                                    new BinaryStringData("reference"),
                                    17
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        REDUCE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("UpdatedReference"),
                                    41
                                }));

        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        REDUCE_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    18,
                                    new BinaryStringData("reference"),
                                    17
                                }),
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    19,
                                    new BinaryStringData("updatedreference"),
                                    17
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(REDUCE_TABLEID, EXPECTED_REDUCE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }

    @Test
    public void testWildcardSchemaTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                WILDCARD_TABLEID.identifier(),
                                "*, age + 1 as newage",
                                "newage > 17")
                        .addTimezone("GMT")
                        .build();
        EventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        new EventOperatorTestHarness<>(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(WILDCARD_TABLEID, WILDCARD_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) WILDCARD_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_WILDCARD_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        WILDCARD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }));

        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        WILDCARD_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    18
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        WILDCARD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                }));

        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        WILDCARD_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    18
                                }),
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                    19
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(WILDCARD_TABLEID, EXPECTED_WILDCARD_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }
}
