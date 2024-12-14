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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.connectors.stimps.factory.StimpsDataFactory;
import org.apache.flink.cdc.connectors.stimps.source.StimpsSourceOptions;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;

/** Integration test for {@link FlinkPipelineComposer} in parallelized schema evolution cases. */
@Timeout(value = 600, unit = java.util.concurrent.TimeUnit.SECONDS)
class FlinkParallelizedPipelineITCase {

    private static final int MAX_PARALLELISM = 4;
    private static final int UPSTREAM_TABLE_COUNT = 4;
    private static final List<RouteDef> ROUTING_RULES;

    static {
        ROUTING_RULES =
                IntStream.range(0, UPSTREAM_TABLE_COUNT)
                        .mapToObj(
                                i ->
                                        new RouteDef(
                                                "default_namespace_subtask_\\d.default_database.table_"
                                                        + i,
                                                "default_namespace.default_database.table_" + i,
                                                null,
                                                null))
                        .collect(Collectors.toList());
    }

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void init() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
        System.out.println(
                "NOTICE: This is a fuzzy test. Please check if value sink prints expected events:");
        System.out.println("================================");
        System.out.print(outCaptor);
        System.out.println("================================");
        outCaptor.reset();
    }

    @ParameterizedTest
    @EnumSource
    void testDistributedTablesSourceInSingleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, true);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Validate generated downstream schema
        for (int idx = 0; idx < UPSTREAM_TABLE_COUNT; idx++) {
            Schema schema =
                    ValuesDatabase.getTableSchema(
                            TableId.tableId(
                                    "default_namespace", "default_database", "table_" + idx));

            // The order of result is determined.
            Assertions.assertThat(schema.getColumns())
                    .containsExactly(
                            Column.physicalColumn("id", DataTypes.STRING()),
                            Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_0_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_0_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_0_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn("col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_0_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_0_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_0_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_0_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_0_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_0_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_0_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_0_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)));
        }

        String outputStr = outCaptor.toString();

        IntStream.range(0, 168)
                .forEach(
                        seqNum ->
                                Assertions.assertThat(outputStr)
                                        .contains(String.format("__$0$%d$__", seqNum)));

        // In single-parallelism mode, orderliness of output is determined.
        String[] dataLines = outputStr.split(System.lineSeparator());
        Assertions.assertThat(dataLines)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_database.table_0, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$0$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$1$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$2$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$3$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$4$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$5$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$6$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$7$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$8$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$9$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_1, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$10$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$11$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$12$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$13$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$14$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$15$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$16$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$17$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$18$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$19$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_2, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$20$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$21$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$22$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$23$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$24$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$25$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$26$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$27$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$28$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$29$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_3, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$30$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$31$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$32$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$33$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$34$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$35$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$36$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$37$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$38$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$39$__], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$40$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$41$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$42$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$43$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$44$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$45$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$46$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$47$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$48$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$49$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$50$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$51$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$52$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$53$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$54$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$55$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$56$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$57$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$58$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$59$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$60$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$61$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$62$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$63$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$64$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$65$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$66$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$67$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$68$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$69$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$70$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$71$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$72$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$73$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$74$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$75$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$76$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$77$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$78$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$79$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$80$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$81$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$82$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$83$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$84$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$85$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$86$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$87$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$88$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$89$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$90$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$91$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$92$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$93$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$94$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$95$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$96$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$97$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$98$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$99$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$100$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$101$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$102$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$103$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$104$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$105$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$106$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$107$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$108$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$109$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$110$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$111$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$112$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$113$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$114$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$115$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$116$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$117$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$118$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$119$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$120$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$121$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$122$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$123$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$124$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$125$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$126$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$127$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$128$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$129$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$130$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$131$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$132$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$133$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$134$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$135$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$136$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$137$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$138$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$139$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$140$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$141$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$142$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$143$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$144$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$145$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$146$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$147$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$148$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$149$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$150$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$151$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$152$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$153$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$154$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$155$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$156$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$157$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$158$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$159$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$160$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$161$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$162$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$163$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$164$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$165$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$166$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$167$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testDistributedTablesSourceInMultipleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, true);
        sourceConfig.set(StimpsSourceOptions.TABLE_COUNT, UPSTREAM_TABLE_COUNT);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, MAX_PARALLELISM);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();

        // Validate generated downstream schema
        for (int idx = 0; idx < UPSTREAM_TABLE_COUNT; idx++) {
            Schema schema =
                    ValuesDatabase.getTableSchema(
                            TableId.tableId(
                                    "default_namespace", "default_database", "table_" + idx));

            // The order of result schema is uncertain.
            Assertions.assertThat(schema.getColumns())
                    .containsExactlyInAnyOrder(
                            Column.physicalColumn("id", DataTypes.STRING()),
                            Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_0_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_1_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_2_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_3_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_0_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_1_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_2_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_3_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_0_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_1_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_2_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_3_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn("col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_0_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_1_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_2_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_3_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_0_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_1_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_2_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_3_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_0_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_1_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_2_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_3_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_0_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_1_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_2_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_3_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_0_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_1_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_2_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_3_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_0_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_1_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_2_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_3_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_1_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_2_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_3_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_0_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_1_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_2_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_3_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_1_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_2_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_3_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_0_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_1_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_2_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_3_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)));
        }

        String outputStr = outCaptor.toString();
        IntStream.range(0, MAX_PARALLELISM)
                .forEach(
                        subTaskId ->
                                IntStream.range(0, 168)
                                        .forEach(
                                                seqNum ->
                                                        Assertions.assertThat(outputStr)
                                                                .contains(
                                                                        String.format(
                                                                                "__$%d$%d$__",
                                                                                subTaskId,
                                                                                seqNum))));

        String[] dataLines = outputStr.split(System.lineSeparator());
        String[] expectedTokens = {
            "true",
            "17",
            "34",
            "68",
            "136",
            "272.0",
            "544.0",
            "1088.00000000000",
            "Alice",
            "Bob",
            "Q2ljYWRh",
            "RGVycmlkYQ==",
            "64800000",
            "2019-12-31T18:00",
            "2020-07-17T18:00",
            "1970-01-05T05:20:00.000123456+08:00"
        };

        Stream.of(expectedTokens)
                .forEach(
                        token ->
                                Assertions.assertThat(
                                                Stream.of(dataLines)
                                                        .filter(line -> line.contains(token))
                                                        .count())
                                        .as("Checking presence of %s", token)
                                        .isGreaterThanOrEqualTo(
                                                UPSTREAM_TABLE_COUNT * MAX_PARALLELISM));
    }

    @ParameterizedTest
    @EnumSource
    void testRegularTablesSourceInSingleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, false);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.LENIENT);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Validate generated downstream schema
        for (int idx = 0; idx < UPSTREAM_TABLE_COUNT; idx++) {
            Schema schema =
                    ValuesDatabase.getTableSchema(
                            TableId.tableId(
                                    "default_namespace_subtask_0",
                                    "default_database",
                                    "table_" + idx));

            // The order of result is determined.
            Assertions.assertThat(schema.getColumns())
                    .containsExactly(
                            Column.physicalColumn("id", DataTypes.STRING()),
                            Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_0_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_0_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_0_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn("col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_0_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_0_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_0_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_0_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_0_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_0_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_0_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_0_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)));
        }

        String outputStr = outCaptor.toString();

        IntStream.range(0, 168)
                .forEach(
                        seqNum ->
                                Assertions.assertThat(outputStr)
                                        .contains(String.format("__$0$%d$__", seqNum)));

        // In single-parallelism mode, orderliness of output is determined.
        String[] dataLines = outputStr.split(System.lineSeparator());
        Assertions.assertThat(dataLines)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace_subtask_0.default_database.table_0, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$0$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$1$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$2$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$3$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$4$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$5$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$6$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$7$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$8$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$9$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace_subtask_0.default_database.table_1, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$10$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$11$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$12$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$13$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$14$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$15$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$16$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$17$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$18$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$19$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace_subtask_0.default_database.table_2, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$20$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$21$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$22$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$23$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$24$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$25$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$26$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$27$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$28$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$29$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace_subtask_0.default_database.table_3, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$30$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$31$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$32$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$33$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$34$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$35$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$36$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$37$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$38$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$39$__], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$40$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$41$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$42$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$43$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$44$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$45$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$46$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$47$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$48$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$49$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$50$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$51$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$52$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$53$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$54$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$55$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$56$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$57$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$58$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$59$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$60$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$61$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$62$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$63$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$64$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$65$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$66$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$67$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$68$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$69$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$70$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$71$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$72$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$73$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$74$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$75$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$76$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$77$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$78$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$79$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$80$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$81$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$82$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$83$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$84$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$85$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$86$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$87$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$88$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$89$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$90$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$91$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$92$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$93$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$94$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$95$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$96$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$97$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$98$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$99$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$100$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$101$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$102$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$103$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$104$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$105$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$106$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$107$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$108$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$109$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$110$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$111$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$112$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$113$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$114$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$115$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$116$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$117$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$118$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$119$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$120$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$121$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$122$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$123$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$124$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$125$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$126$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$127$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$128$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$129$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$130$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$131$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$132$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$133$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$134$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$135$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$136$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$137$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$138$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$139$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$140$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$141$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$142$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$143$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$144$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$145$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$146$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$147$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$148$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$149$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$150$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$151$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$152$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$153$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$154$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$155$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$156$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$157$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$158$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$159$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$160$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$161$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$162$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$163$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_0, before=[], after=[__$0$164$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_1, before=[], after=[__$0$165$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_2, before=[], after=[__$0$166$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace_subtask_0.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace_subtask_0.default_database.table_3, before=[], after=[__$0$167$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testRegularTablesSourceInMultipleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, false);
        sourceConfig.set(StimpsSourceOptions.TABLE_COUNT, UPSTREAM_TABLE_COUNT);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, MAX_PARALLELISM);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.LENIENT);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();

        // Validate generated downstream schema

        for (int taskIdx = 0; taskIdx < MAX_PARALLELISM; taskIdx++) {
            for (int tableIdx = 0; tableIdx < UPSTREAM_TABLE_COUNT; tableIdx++) {
                Schema schema =
                        ValuesDatabase.getTableSchema(
                                TableId.tableId(
                                        "default_namespace_subtask_" + taskIdx,
                                        "default_database",
                                        "table_" + tableIdx));

                // The order of result schema is uncertain.
                Assertions.assertThat(schema.getColumns())
                        .containsExactlyInAnyOrder(
                                Column.physicalColumn("id", DataTypes.STRING()),
                                Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_booleantype",
                                        DataTypes.BOOLEAN()),
                                Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_tinyinttype",
                                        DataTypes.TINYINT()),
                                Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_smallinttype",
                                        DataTypes.SMALLINT()),
                                Column.physicalColumn("col_inttype", DataTypes.INT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_inttype", DataTypes.INT()),
                                Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_biginttype",
                                        DataTypes.BIGINT()),
                                Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_decimaltype",
                                        DataTypes.DECIMAL(17, 11)),
                                Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_floattype", DataTypes.FLOAT()),
                                Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_doubletype",
                                        DataTypes.DOUBLE()),
                                Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_chartype", DataTypes.CHAR(17)),
                                Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_varchartype",
                                        DataTypes.VARCHAR(17)),
                                Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_binarytype",
                                        DataTypes.BINARY(17)),
                                Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_varbinarytype",
                                        DataTypes.VARBINARY(17)),
                                Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_timetype", DataTypes.TIME(9)),
                                Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_timestamptype",
                                        DataTypes.TIMESTAMP(9)),
                                Column.physicalColumn(
                                        "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_zonedtimestamptype",
                                        DataTypes.TIMESTAMP_TZ(9)),
                                Column.physicalColumn(
                                        "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                                Column.physicalColumn(
                                        "subtask_" + taskIdx + "_col_localzonedtimestamptype",
                                        DataTypes.TIMESTAMP_LTZ(9)));
            }
        }

        String outputStr = outCaptor.toString();
        IntStream.range(0, MAX_PARALLELISM)
                .forEach(
                        subTaskId ->
                                IntStream.range(0, 168)
                                        .forEach(
                                                seqNum ->
                                                        Assertions.assertThat(outputStr)
                                                                .contains(
                                                                        String.format(
                                                                                "__$%d$%d$__",
                                                                                subTaskId,
                                                                                seqNum))));

        String[] dataLines = outputStr.split(System.lineSeparator());
        String[] expectedTokens = {
            "true",
            "17",
            "34",
            "68",
            "136",
            "272.0",
            "544.0",
            "1088.00000000000",
            "Alice",
            "Bob",
            "Q2ljYWRh",
            "RGVycmlkYQ==",
            "64800000",
            "2019-12-31T18:00",
            "2020-07-17T18:00",
            "1970-01-05T05:20:00.000123456+08:00"
        };

        Stream.of(expectedTokens)
                .forEach(
                        token ->
                                Assertions.assertThat(
                                                Stream.of(dataLines)
                                                        .filter(line -> line.contains(token))
                                                        .count())
                                        .as("Checking presence of %s", token)
                                        .isGreaterThanOrEqualTo(
                                                UPSTREAM_TABLE_COUNT * MAX_PARALLELISM));
    }

    @ParameterizedTest
    @EnumSource
    void testRegularTablesSourceMergedInSingleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, false);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        ROUTING_RULES,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Validate generated downstream schema
        for (int idx = 0; idx < UPSTREAM_TABLE_COUNT; idx++) {
            Schema schema =
                    ValuesDatabase.getTableSchema(
                            TableId.tableId(
                                    "default_namespace", "default_database", "table_" + idx));

            // The order of result is determined.
            Assertions.assertThat(schema.getColumns())
                    .containsExactly(
                            Column.physicalColumn("id", DataTypes.STRING()),
                            Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_0_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_0_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_0_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn("col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_0_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_0_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_0_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_0_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_0_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_0_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_0_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_0_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)));
        }

        String outputStr = outCaptor.toString();

        IntStream.range(0, 168)
                .forEach(
                        seqNum ->
                                Assertions.assertThat(outputStr)
                                        .contains(String.format("__$0$%d$__", seqNum)));

        // In single-parallelism mode, orderliness of output is determined.
        String[] dataLines = outputStr.split(System.lineSeparator());
        Assertions.assertThat(dataLines)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_database.table_0, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$0$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$1$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$2$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$3$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$4$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$5$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$6$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$7$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$8$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$9$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_1, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$10$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$11$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$12$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$13$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$14$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$15$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$16$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$17$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$18$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$19$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_2, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$20$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$21$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$22$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$23$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$24$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$25$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$26$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$27$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$28$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$29$__], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_database.table_3, schema=columns={`id` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$30$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$31$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$32$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$33$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$34$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$35$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$36$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$37$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$38$__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$39$__], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$40$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$41$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$42$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$43$__, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$44$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$45$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$46$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_booleantype` BOOLEAN, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$47$__, true, true], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$48$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$49$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$50$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$51$__, true, true, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$52$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$53$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$54$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_tinyinttype` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$55$__, true, true, 17, 17], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$56$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$57$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$58$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$59$__, true, true, 17, 17, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$60$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$61$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$62$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_smallinttype` SMALLINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$63$__, true, true, 17, 17, 34, 34], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$64$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$65$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$66$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$67$__, true, true, 17, 17, 34, 34, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$68$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$69$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$70$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_inttype` INT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$71$__, true, true, 17, 17, 34, 34, 68, 68], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$72$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$73$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$74$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$75$__, true, true, 17, 17, 34, 34, 68, 68, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$76$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$77$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$78$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_biginttype` BIGINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$79$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$80$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$81$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$82$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$83$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$84$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$85$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$86$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_floattype` FLOAT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$87$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$88$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$89$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$90$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$91$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$92$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$93$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$94$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_doubletype` DOUBLE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$95$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$96$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$97$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$98$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$99$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$100$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$101$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$102$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_decimaltype` DECIMAL(17, 11), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$103$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$104$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$105$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$106$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$107$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$108$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$109$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$110$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_chartype` CHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$111$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$112$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$113$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$114$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$115$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$116$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$117$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$118$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varchartype` VARCHAR(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$119$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$120$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$121$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$122$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$123$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$124$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$125$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$126$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_binarytype` BINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$127$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$128$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$129$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$130$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$131$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$132$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$133$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$134$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_varbinarytype` VARBINARY(17), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$135$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$136$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$137$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$138$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$139$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$140$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$141$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$142$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timetype` TIME(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$143$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$144$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$145$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$146$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$147$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$148$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$149$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$150$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_timestamptype` TIMESTAMP(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$151$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$152$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$153$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$154$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$155$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$156$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$157$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$158$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_zonedtimestamptype` TIMESTAMP(9) WITH TIME ZONE, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$159$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$160$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$161$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$162$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$163$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_0, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_0, before=[], after=[__$0$164$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_1, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_1, before=[], after=[__$0$165$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_2, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_2, before=[], after=[__$0$166$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_database.table_3, addedColumns=[ColumnWithPosition{column=`subtask_0_col_localzonedtimestamptype` TIMESTAMP_LTZ(9), position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_database.table_3, before=[], after=[__$0$167$__, true, true, 17, 17, 34, 34, 68, 68, 136, 136, 272.0, 272.0, 544.0, 544.0, 1088.00000000000, 1088.00000000000, Alice, Alice, Bob, Bob, Q2ljYWRh, Q2ljYWRh, RGVycmlkYQ==, RGVycmlkYQ==, 64800000, 64800000, 2020-07-17T18:00, 2020-07-17T18:00, 1970-01-05T05:20:00.000123456+08:00, 1970-01-05T05:20:00.000123456+08:00, 2019-12-31T18:00, 2019-12-31T18:00], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testRegularTablesSourceMergedInMultipleParallelism(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(StimpsSourceOptions.DISTRIBUTED_TABLES, false);
        sourceConfig.set(StimpsSourceOptions.TABLE_COUNT, UPSTREAM_TABLE_COUNT);
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, MAX_PARALLELISM);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        ROUTING_RULES,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();

        // Validate generated downstream schema
        for (int idx = 0; idx < UPSTREAM_TABLE_COUNT; idx++) {
            Schema schema =
                    ValuesDatabase.getTableSchema(
                            TableId.tableId(
                                    "default_namespace", "default_database", "table_" + idx));

            // The order of result schema is uncertain.
            Assertions.assertThat(schema.getColumns())
                    .containsExactlyInAnyOrder(
                            Column.physicalColumn("id", DataTypes.STRING()),
                            Column.physicalColumn("col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_0_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_1_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_2_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("subtask_3_col_booleantype", DataTypes.BOOLEAN()),
                            Column.physicalColumn("col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_0_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_1_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_2_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("subtask_3_col_tinyinttype", DataTypes.TINYINT()),
                            Column.physicalColumn("col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_0_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_1_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_2_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn(
                                    "subtask_3_col_smallinttype", DataTypes.SMALLINT()),
                            Column.physicalColumn("col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_0_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_1_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_2_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("subtask_3_col_inttype", DataTypes.INT()),
                            Column.physicalColumn("col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_0_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_1_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_2_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("subtask_3_col_biginttype", DataTypes.BIGINT()),
                            Column.physicalColumn("col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_0_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_1_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_2_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn(
                                    "subtask_3_col_decimaltype", DataTypes.DECIMAL(17, 11)),
                            Column.physicalColumn("col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_0_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_1_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_2_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("subtask_3_col_floattype", DataTypes.FLOAT()),
                            Column.physicalColumn("col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_0_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_1_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_2_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("subtask_3_col_doubletype", DataTypes.DOUBLE()),
                            Column.physicalColumn("col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_0_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_1_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_2_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("subtask_3_col_chartype", DataTypes.CHAR(17)),
                            Column.physicalColumn("col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_1_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_2_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn(
                                    "subtask_3_col_varchartype", DataTypes.VARCHAR(17)),
                            Column.physicalColumn("col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_0_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_1_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_2_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("subtask_3_col_binarytype", DataTypes.BINARY(17)),
                            Column.physicalColumn("col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_0_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_1_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_2_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn(
                                    "subtask_3_col_varbinarytype", DataTypes.VARBINARY(17)),
                            Column.physicalColumn("col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_0_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_1_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_2_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("subtask_3_col_timetype", DataTypes.TIME(9)),
                            Column.physicalColumn("col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_timestamptype", DataTypes.TIMESTAMP(9)),
                            Column.physicalColumn(
                                    "col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_zonedtimestamptype", DataTypes.TIMESTAMP_TZ(9)),
                            Column.physicalColumn(
                                    "col_localzonedtimestamptype", DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_0_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_1_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_2_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)),
                            Column.physicalColumn(
                                    "subtask_3_col_localzonedtimestamptype",
                                    DataTypes.TIMESTAMP_LTZ(9)));
        }

        String outputStr = outCaptor.toString();
        IntStream.range(0, MAX_PARALLELISM)
                .forEach(
                        subTaskId ->
                                IntStream.range(0, 168)
                                        .forEach(
                                                seqNum ->
                                                        Assertions.assertThat(outputStr)
                                                                .contains(
                                                                        String.format(
                                                                                "__$%d$%d$__",
                                                                                subTaskId,
                                                                                seqNum))));

        String[] dataLines = outputStr.split(System.lineSeparator());
        String[] expectedTokens = {
            "true",
            "17",
            "34",
            "68",
            "136",
            "272.0",
            "544.0",
            "1088.00000000000",
            "Alice",
            "Bob",
            "Q2ljYWRh",
            "RGVycmlkYQ==",
            "64800000",
            "2019-12-31T18:00",
            "2020-07-17T18:00",
            "1970-01-05T05:20:00.000123456+08:00"
        };

        Stream.of(expectedTokens)
                .forEach(
                        token ->
                                Assertions.assertThat(
                                                Stream.of(dataLines)
                                                        .filter(line -> line.contains(token))
                                                        .count())
                                        .as("Checking presence of %s", token)
                                        .isGreaterThanOrEqualTo(
                                                UPSTREAM_TABLE_COUNT * MAX_PARALLELISM));
    }
}
