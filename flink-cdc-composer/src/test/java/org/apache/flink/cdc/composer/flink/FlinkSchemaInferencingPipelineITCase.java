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
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.connectors.stimps.factory.StimpsDataFactory;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.stimps.source.StimpsSourceFunction.TABLE_ID;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkPipelineComposer} in schema inferencing cases. */
class FlinkSchemaInferencingPipelineITCase {

    private static final int MAX_PARALLELISM = 4;

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
    }

    @ParameterizedTest
    @EnumSource
    void testSingleTableInMultiplePartitionsSourceInSingleParallelism(
            ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
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
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
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

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactlyElementsOf(
                        Stream.of(
                                        "CreateTableEvent{tableId=%s, schema=columns={`id` BIGINT,`name` VARCHAR(17)}, primaryKeys=id, partitionKeys=id, options=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[1, Alice #0], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[1, Alice #0], after=[1, Bob #0], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[1, Bob #0], after=[], op=DELETE, meta=()}",
                                        "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`foo` TINYINT, position=LAST, existedColumnName=null}]}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[2, Cecily #0, 17], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[2, Cecily #0, 17], after=[2, Derrida #0, 17], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[2, Derrida #0, 17], after=[], op=DELETE, meta=()}",
                                        "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`bar_0` STRING, position=LAST, existedColumnName=null}]}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[3, Edgeworth #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[3, Edgeworth #0, 17, Before before], after=[3, Edgeworth #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[3, Edgeworth #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[4, Ferris #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[4, Ferris #0, 17, Before before], after=[4, Ferris #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[4, Ferris #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[5, Gumshoe #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[5, Gumshoe #0, 17, Before before], after=[5, Gumshoe #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[5, Gumshoe #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[6, Harry #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[6, Harry #0, 17, Before before], after=[6, Harry #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[6, Harry #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[7, IINA #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[7, IINA #0, 17, Before before], after=[7, IINA #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[7, IINA #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[8, Juliet #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[8, Juliet #0, 17, Before before], after=[8, Juliet #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[8, Juliet #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[9, Kio #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[9, Kio #0, 17, Before before], after=[9, Kio #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[9, Kio #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[10, Lilith #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[10, Lilith #0, 17, Before before], after=[10, Lilith #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[10, Lilith #0, 17, After after], after=[], op=DELETE, meta=()}")
                                .map(s -> String.format(s, TABLE_ID))
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest
    @EnumSource
    void testSingleTableInMultiplePartitionsSourceInMultipleParallelism(
            ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        SourceDef sourceDef =
                new SourceDef(StimpsDataFactory.IDENTIFIER, "STIMP Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 4);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
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

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactlyElementsOf(
                        Stream.of(
                                        "CreateTableEvent{tableId=%s, schema=columns={`id` BIGINT,`name` VARCHAR(17)}, primaryKeys=id, partitionKeys=id, options=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[1, Alice #0], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[1, Alice #0], after=[1, Bob #0], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[1, Bob #0], after=[], op=DELETE, meta=()}",
                                        "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`foo` TINYINT, position=LAST, existedColumnName=null}]}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[2, Cecily #0, 17], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[2, Cecily #0, 17], after=[2, Derrida #0, 17], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[2, Derrida #0, 17], after=[], op=DELETE, meta=()}",
                                        "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`bar_0` STRING, position=LAST, existedColumnName=null}]}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[3, Edgeworth #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[3, Edgeworth #0, 17, Before before], after=[3, Edgeworth #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[3, Edgeworth #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[4, Ferris #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[4, Ferris #0, 17, Before before], after=[4, Ferris #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[4, Ferris #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[5, Gumshoe #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[5, Gumshoe #0, 17, Before before], after=[5, Gumshoe #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[5, Gumshoe #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[6, Harry #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[6, Harry #0, 17, Before before], after=[6, Harry #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[6, Harry #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[7, IINA #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[7, IINA #0, 17, Before before], after=[7, IINA #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[7, IINA #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[8, Juliet #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[8, Juliet #0, 17, Before before], after=[8, Juliet #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[8, Juliet #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[9, Kio #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[9, Kio #0, 17, Before before], after=[9, Kio #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[9, Kio #0, 17, After after], after=[], op=DELETE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[], after=[10, Lilith #0, 17, Before before], op=INSERT, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[10, Lilith #0, 17, Before before], after=[10, Lilith #0, 17, After after], op=UPDATE, meta=()}",
                                        "DataChangeEvent{tableId=%s, before=[10, Lilith #0, 17, After after], after=[], op=DELETE, meta=()}")
                                .map(s -> String.format(s, TABLE_ID))
                                .collect(Collectors.toList()));
    }
}
