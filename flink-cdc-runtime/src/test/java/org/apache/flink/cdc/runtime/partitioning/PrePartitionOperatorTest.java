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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DefaultDataChangeEventHashFunctionProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaReducerGateway;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PrePartitionOperator}. */
class PrePartitionOperatorTest {
    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();
    private static final int DOWNSTREAM_PARALLELISM = 5;

    @Test
    void testBroadcastingSchemaChangeEvent() throws Exception {
        try (EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> testHarness =
                createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // CreateTableEvent
            PrePartitionOperator operator = testHarness.getOperator();
            CreateTableEvent createTableEvent = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
            operator.processElement(new StreamRecord<>(createTableEvent));
            assertThat(testHarness.getOutputRecords()).hasSize(DOWNSTREAM_PARALLELISM);
            for (int i = 0; i < DOWNSTREAM_PARALLELISM; i++) {
                assertThat(testHarness.getOutputRecords().poll())
                        .isEqualTo(
                                new StreamRecord<>(new PartitioningEvent(createTableEvent, 0, i)));
            }
        }
    }

    @Test
    void testPartitioningDataChangeEvent() throws Exception {
        try (EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> testHarness =
                createTestHarness()) {
            // Initialization
            testHarness.open();
            testHarness.registerTableSchema(CUSTOMERS, CUSTOMERS_SCHEMA);

            // DataChangeEvent
            PrePartitionOperator operator = testHarness.getOperator();
            BinaryRecordDataGenerator recordDataGenerator =
                    new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));

            CreateTableEvent eventCte = new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA);
            DataChangeEvent eventA =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS,
                            recordDataGenerator.generate(
                                    new Object[] {1, new BinaryStringData("Alice"), 12345678L}));
            DataChangeEvent eventB =
                    DataChangeEvent.insertEvent(
                            CUSTOMERS,
                            recordDataGenerator.generate(
                                    new Object[] {2, new BinaryStringData("Bob"), 12345689L}));

            operator.processElement(new StreamRecord<>(eventCte));
            operator.processElement(new StreamRecord<>(eventA));
            operator.processElement(new StreamRecord<>(eventB));

            assertThat(testHarness.getOutputRecords())
                    .contains(
                            new StreamRecord<>(
                                    new PartitioningEvent(
                                            eventA,
                                            0,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventA))),
                            new StreamRecord<>(
                                    new PartitioningEvent(
                                            eventB,
                                            0,
                                            getPartitioningTarget(CUSTOMERS_SCHEMA, eventB))))
                    .containsAll(
                            IntStream.range(0, DOWNSTREAM_PARALLELISM)
                                    .mapToObj(
                                            i ->
                                                    new StreamRecord<>(
                                                            new PartitioningEvent(eventCte, 0, i)))
                                    .collect(Collectors.toList()));
        }
    }

    private int getPartitioningTarget(Schema schema, DataChangeEvent dataChangeEvent) {
        return new DefaultDataChangeEventHashFunctionProvider()
                        .getHashFunction(null, schema)
                        .hashcode(dataChangeEvent)
                % DOWNSTREAM_PARALLELISM;
    }

    private EventOperatorTestHarness<PrePartitionOperator, PartitioningEvent> createTestHarness() {
        PrePartitionOperator operator =
                new PrePartitionOperator(
                        TestingSchemaReducerGateway.SCHEMA_OPERATOR_ID,
                        DOWNSTREAM_PARALLELISM,
                        new DefaultDataChangeEventHashFunctionProvider());
        return new EventOperatorTestHarness<>(operator, DOWNSTREAM_PARALLELISM);
    }
}
