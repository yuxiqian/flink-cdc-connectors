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

package org.apache.flink.cdc.runtime.operators.reducer;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEventWithSchema;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.util.Collector;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Unit tests for the {@link RouterProcessor}. */
class RouterProcessorTest {
    private static final Schema DUMMY_SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.BIGINT()).build();

    private static final BinaryRecordDataGenerator generator =
            new BinaryRecordDataGenerator(
                    DUMMY_SCHEMA.getColumnDataTypes().toArray(new DataType[0]));

    private static DataChangeEventWithSchema dummyInsert(TableId tableId) {
        return new DataChangeEventWithSchema(
                DUMMY_SCHEMA,
                DataChangeEvent.insertEvent(tableId, generator.generate(new Object[] {17L})));
    }

    private static DataChangeEventWithSchema dummyUpdate(TableId tableId) {
        return new DataChangeEventWithSchema(
                DUMMY_SCHEMA,
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(new Object[] {18L}),
                        generator.generate(new Object[] {19L})));
    }

    private static DataChangeEventWithSchema dummyDelete(TableId tableId) {
        return new DataChangeEventWithSchema(
                DUMMY_SCHEMA,
                DataChangeEvent.deleteEvent(tableId, generator.generate(new Object[] {20L})));
    }

    @Test
    void testDefaultRouting() {
        List<RouteRule> routingRules = Collections.emptyList();
        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testIrrelevantRouting() {
        // Routing on something irrelevant
        List<RouteRule> routingRules =
                Collections.singletonList(new RouteRule("foo.bar.baz", "bar.boo.faz", null));

        // Should be equivalent to an empty routing
        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testMerging() {
        // Route-merging two tables and leave the other one untouched
        List<RouteRule> routingRules =
                Collections.singletonList(
                        new RouteRule("ns.schema.tbl\\.", "ns.schema.merged", null));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.merged, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testPartialRouting() {
        // Just route one table and leave others untouched
        List<RouteRule> routingRules =
                Collections.singletonList(
                        new RouteRule("ns.schema.tbl2", "ns.schema.routed_tbl2", null));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testPartialMerging() {
        // Route-merging two tables and leave the other one untouched
        List<RouteRule> routingRules =
                Collections.singletonList(
                        new RouteRule("ns.schema.tbl[1|3]", "ns.schema.routed_tbl13", null));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.routed_tbl13, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testReplacingSymbol() {
        List<RouteRule> routingRules =
                Collections.singletonList(
                        new RouteRule("ns.schema.tbl\\.", "sink_ns.sink_schema.<>", "<>"));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testPartialReplacingSymbol() {
        List<RouteRule> routingRules =
                Collections.singletonList(
                        new RouteRule("ns.schema.tbl[1|3]", "sink_ns.sink_schema.<>", "<>"));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl1, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=sink_ns.sink_schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testMultipleRoutes() {
        List<RouteRule> routingRules =
                Arrays.asList(
                        new RouteRule("ns.schema.tbl1", "ns.schema.Alice", null),
                        new RouteRule("ns.schema.tbl2", "ns.schema.Bob", null),
                        new RouteRule("ns.schema.tbl3", "ns.schema.Cicada", null));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.Alice, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Alice, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Alice, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Bob, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Bob, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Bob, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Cicada, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Cicada, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Cicada, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    @Test
    void testBroadcastRoutes() {
        List<RouteRule> routingRules =
                Arrays.asList(
                        new RouteRule("ns.schema.tbl1", "ns.schema.Xray", null),
                        new RouteRule("ns.schema.tbl1", "ns.schema.Yankee", null),
                        new RouteRule("ns.schema.tbl1", "ns.schema.Zulu", null));

        Assertions.assertThat(testGenericRoutingWith(routingRules))
                .map(Event::toString)
                .containsExactlyInAnyOrder(
                        "DataChangeEventWithSchema{tableId=ns.schema.Xray, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Xray, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Xray, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Yankee, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Yankee, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Yankee, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Zulu, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Zulu, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.Zulu, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl2, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[], after=[17], op=INSERT, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[18], after=[19], op=UPDATE, meta={}}",
                        "DataChangeEventWithSchema{tableId=ns.schema.tbl3, schema=columns={`id` BIGINT}, primaryKeys=, options=(), before=[20], after=[], op=DELETE, meta={}}");
    }

    List<Event> testGenericRoutingWith(List<RouteRule> routingRules) {
        RouterProcessor processor = new RouterProcessor(routingRules);

        List<Event> routedEvents = new ArrayList<>();

        IntStream.rangeClosed(1, 3)
                .mapToObj(idx -> TableId.tableId("ns", "schema", "tbl" + idx))
                .flatMap(tbl -> Stream.of(dummyInsert(tbl), dummyUpdate(tbl), dummyDelete(tbl)))
                .forEach(
                        evt ->
                                processor.flatMap(
                                        evt,
                                        new Collector<Event>() {
                                            @Override
                                            public void collect(Event event) {
                                                routedEvents.add(event);
                                            }

                                            @Override
                                            public void close() {}
                                        }));

        return routedEvents;
    }
}
