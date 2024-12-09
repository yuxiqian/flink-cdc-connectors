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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.RecordDataTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;

/** IT case for MySQL event source. */
public class MySqlFullTypesITCase extends MySqlSourceTestBase {

    private final UniqueDatabase fullTypesMySqlDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    "column_type_test",
                    MySqSourceTestUtils.TEST_USER,
                    MySqSourceTestUtils.TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @Before
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testMysqlCommonDataTypes() throws Throwable {
        testCommonDataTypes(fullTypesMySqlDatabase);
    }

    @Test
    public void testMysqlTimeDataTypes() throws Throwable {
        RowType recordType =
                RowType.of(
                        DataTypes.DECIMAL(20, 0).notNull(),
                        DataTypes.INT(),
                        DataTypes.DATE(),
                        DataTypes.TIME(0),
                        DataTypes.TIME(3),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(0));

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    // TIME(6) will lose precision for microseconds.
                    // Because Flink's BinaryWriter force write int value for TIME(6).
                    // See BinaryWriter#write for detail.
                    64822123,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    null
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    2021,
                    18460,
                    64822000,
                    64822123,
                    null,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22.123456")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2000-01-01 00:00:00"))
                };

        testTimeDataTypes(
                fullTypesMySqlDatabase, recordType, expectedSnapshot, expectedStreamRecord);
    }

    @Test
    public void testMysqlPrecisionTypes() throws Throwable {
        testMysqlPrecisionTypes(fullTypesMySqlDatabase);
    }

    public void testMysqlPrecisionTypes(UniqueDatabase database) throws Throwable {
        RowType recordType =
                RowType.of(
                        DataTypes.DECIMAL(20, 0).notNull(),
                        DataTypes.DECIMAL(6, 2),
                        DataTypes.DECIMAL(9, 4),
                        DataTypes.DECIMAL(20, 4),
                        DataTypes.TIME(0),
                        DataTypes.TIME(3),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE(),
                        DataTypes.DOUBLE());

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.5"), 9, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 20, 4),
                    64800000,
                    64822100,
                    64822100,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:00")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:00")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    2d,
                    3d,
                    5d,
                    7d,
                    11d,
                    13d,
                    17d,
                    19d,
                    23d,
                    29d,
                    31d,
                    37d
                };

        Object[] expectedStreamRecord =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.5"), 9, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 20, 4),
                    64800000,
                    64822100,
                    null,
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:00")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    TimestampData.fromTimestamp(Timestamp.valueOf("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:00")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    LocalZonedTimestampData.fromInstant(toInstant("2020-07-17 18:00:22")),
                    2d,
                    3d,
                    5d,
                    7d,
                    11d,
                    13d,
                    17d,
                    19d,
                    23d,
                    29d,
                    31d,
                    37d
                };

        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"precision_types"}, database)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        // skip CreateTableEvent
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, recordType))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE precision_types SET time_6_c = null WHERE id = 1;");
        }

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, recordType))
                .isEqualTo(expectedStreamRecord);
    }

    private void testCommonDataTypes(UniqueDatabase database) throws Exception {
        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"common_types"}, database)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        String expectedPointJsonText = "{\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}";
        String expectedGeometryJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectLinestringJsonText =
                "{\"coordinates\":[[3,0],[3,3],[3,5]],\"type\":\"LineString\",\"srid\":0}";
        String expectPolygonJsonText =
                "{\"coordinates\":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],\"type\":\"Polygon\",\"srid\":0}";
        String expectMultipointJsonText =
                "{\"coordinates\":[[1,1],[2,2]],\"type\":\"MultiPoint\",\"srid\":0}";
        String expectMultilineJsonText =
                "{\"coordinates\":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],\"type\":\"MultiLineString\",\"srid\":0}";
        String expectMultipolygonJsonText =
                "{\"coordinates\":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],\"type\":\"MultiPolygon\",\"srid\":0}";
        String expectGeometryCollectionJsonText =
                "{\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10,10]},{\"type\":\"Point\",\"coordinates\":[30,30]},{\"type\":\"LineString\",\"coordinates\":[[15,15],[20,20]]}],\"type\":\"GeometryCollection\",\"srid\":0}";

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 20, 0),
                    (byte) 127,
                    (short) 255,
                    (short) 255,
                    (short) 32767,
                    65535,
                    65535,
                    8388607,
                    16777215,
                    16777215,
                    2147483647,
                    4294967295L,
                    4294967295L,
                    2147483647L,
                    9223372036854775807L,
                    DecimalData.fromBigDecimal(new BigDecimal("18446744073709551615"), 20, 0),
                    DecimalData.fromBigDecimal(new BigDecimal("18446744073709551615"), 20, 0),
                    BinaryStringData.fromString("Hello World"),
                    BinaryStringData.fromString("abc"),
                    123.102d,
                    123.102f,
                    123.103f,
                    123.104f,
                    404.4443d,
                    404.4444d,
                    404.4445d,
                    DecimalData.fromBigDecimal(new BigDecimal("123.4567"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4568"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("123.4569"), 8, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("346"), 6, 0),
                    // Decimal precision larger than 38 will be treated as string.
                    BinaryStringData.fromString("34567892.1"),
                    false,
                    new byte[] {3},
                    true,
                    true,
                    parseHexBinary("651aed08-390f-4893-b2f1-36923e7b7400".replace("-", "")),
                    new byte[] {4, 4, 4, 4, 4, 4, 4, 4},
                    BinaryStringData.fromString("text"),
                    new byte[] {16},
                    new byte[] {16},
                    new byte[] {16},
                    new byte[] {16},
                    2021,
                    BinaryStringData.fromString("red"),
                    BinaryStringData.fromString("{\"key1\": \"value1\"}"),
                    BinaryStringData.fromString(expectedPointJsonText),
                    BinaryStringData.fromString(expectedGeometryJsonText),
                    BinaryStringData.fromString(expectLinestringJsonText),
                    BinaryStringData.fromString(expectPolygonJsonText),
                    BinaryStringData.fromString(expectMultipointJsonText),
                    BinaryStringData.fromString(expectMultilineJsonText),
                    BinaryStringData.fromString(expectMultipolygonJsonText),
                    BinaryStringData.fromString(expectGeometryCollectionJsonText)
                };

        // skip CreateTableEvent
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, COMMON_TYPES))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE common_types SET big_decimal_c = null WHERE id = 1;");
        }

        expectedSnapshot[30] = null;
        // The json string from binlog will remove useless space
        expectedSnapshot[44] = BinaryStringData.fromString("{\"key1\":\"value1\"}");
        Object[] expectedStreamRecord = expectedSnapshot;

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, COMMON_TYPES))
                .isEqualTo(expectedStreamRecord);
    }

    private Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
    }

    private void testTimeDataTypes(
            UniqueDatabase database,
            RowType recordType,
            Object[] expectedSnapshot,
            Object[] expectedStreamRecord)
            throws Exception {
        database.createAndInitialize();
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"time_types"}, database)
                                        .getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        // skip CreateTableEvents
        List<Event> snapshotResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(RecordDataTestUtils.recordFields(snapshotRecord, recordType))
                .isEqualTo(expectedSnapshot);

        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE time_types SET time_6_c = null, timestamp_def_c = default WHERE id = 1;");
        }

        List<Event> streamResults =
                MySqSourceTestUtils.fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData streamRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Assertions.assertThat(RecordDataTestUtils.recordFields(streamRecord, recordType))
                .isEqualTo(expectedStreamRecord);
    }

    private FlinkSourceProvider getFlinkSourceProvider(
            String[] captureTables, UniqueDatabase database) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList(database.getDatabaseName())
                        .tableList(captureTableIds)
                        .includeSchemaChanges(false)
                        .hostname(database.getHost())
                        .port(database.getDatabasePort())
                        .splitSize(10)
                        .fetchSize(2)
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .serverId(MySqSourceTestUtils.getServerId(env.getParallelism()));
        return (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
    }

    private static final RowType COMMON_TYPES =
            RowType.of(
                    DataTypes.DECIMAL(20, 0).notNull(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.DECIMAL(20, 0),
                    DataTypes.DECIMAL(20, 0),
                    DataTypes.VARCHAR(255),
                    DataTypes.CHAR(3),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(8, 4),
                    DataTypes.DECIMAL(6, 0),
                    // Decimal precision larger than 38 will be treated as string.
                    DataTypes.STRING(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BINARY(1),
                    DataTypes.BOOLEAN(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BINARY(16),
                    DataTypes.BINARY(8),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.INT(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING());
}
