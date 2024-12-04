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

package org.apache.flink.cdc.runtime.testutils.operators;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.reducer.SchemaReducer;
import org.apache.flink.cdc.runtime.operators.reducer.events.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.events.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaReducerGateway;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.apache.flink.cdc.runtime.operators.reducer.events.CoordinationResponseUtils.unwrap;

/**
 * Harness for testing customized operators handling {@link Event}s in CDC pipeline.
 *
 * <p>In addition to regular operator context and lifecycle management, this test harness also wraps
 * {@link TestingSchemaReducerGateway} into the context of tested operator, in order to support
 * testing operators that have interaction with {@link SchemaReducer} via {@link
 * SchemaEvolutionClient}.
 *
 * @param <OP> Type of the operator
 * @param <E> Type of the event emitted by the operator
 */
public class EventOperatorTestHarness<OP extends AbstractStreamOperator<E>, E extends Event>
        implements AutoCloseable {
    public static final OperatorID SCHEMA_OPERATOR_ID = new OperatorID(15213L, 15513L);

    public static final OperatorID SINK_OPERATOR_ID = new OperatorID(15214L, 15514L);

    private final OP operator;
    private final int numOutputs;
    private final SchemaReducer schemaReducer;
    private final TestingSchemaReducerGateway schemaReducerGateway;
    private final LinkedList<StreamRecord<E>> outputRecords = new LinkedList<>();
    private final MockedOperatorCoordinatorContext mockedContext;

    public EventOperatorTestHarness(OP operator, int numOutputs) {
        this(operator, numOutputs, null);
    }

    public EventOperatorTestHarness(OP operator, int numOutputs, Duration duration) {
        this(operator, numOutputs, duration, SchemaChangeBehavior.EVOLVE);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior schemaChangeBehavior) {
        this(operator, numOutputs, duration, SchemaChangeBehavior.EVOLVE, true);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior behavior,
            boolean guaranteesSchemaChangeIsolation) {
        this.operator = operator;
        this.numOutputs = numOutputs;
        this.mockedContext =
                new MockedOperatorCoordinatorContext(
                        SCHEMA_OPERATOR_ID, Thread.currentThread().getContextClassLoader());
        schemaReducer =
                new SchemaReducer(
                        "SchemaReducer",
                        mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(duration),
                        behavior,
                        new ArrayList<>(),
                        guaranteesSchemaChangeIsolation);
        schemaReducerGateway = new TestingSchemaReducerGateway(schemaReducer);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior behavior,
            Set<SchemaChangeEventType> enabledEventTypes) {
        this(operator, numOutputs, duration, behavior, enabledEventTypes, true);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior behavior,
            Set<SchemaChangeEventType> enabledEventTypes,
            boolean guaranteesSchemaChangeIsolation) {
        this.operator = operator;
        this.numOutputs = numOutputs;
        this.mockedContext =
                new MockedOperatorCoordinatorContext(
                        SCHEMA_OPERATOR_ID, Thread.currentThread().getContextClassLoader());
        schemaReducer =
                new SchemaReducer(
                        "SchemaReducer",
                        mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(duration, enabledEventTypes),
                        behavior,
                        new ArrayList<>(),
                        guaranteesSchemaChangeIsolation);
        schemaReducerGateway = new TestingSchemaReducerGateway(schemaReducer);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior behavior,
            Set<SchemaChangeEventType> enabledEventTypes,
            Set<SchemaChangeEventType> errorsOnEventTypes) {
        this(operator, numOutputs, duration, behavior, enabledEventTypes, errorsOnEventTypes, true);
    }

    public EventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration duration,
            SchemaChangeBehavior behavior,
            Set<SchemaChangeEventType> enabledEventTypes,
            Set<SchemaChangeEventType> errorsOnEventTypes,
            boolean guaranteesSchemaChangeIsolation) {
        this.operator = operator;
        this.numOutputs = numOutputs;
        this.mockedContext =
                new MockedOperatorCoordinatorContext(
                        SCHEMA_OPERATOR_ID, Thread.currentThread().getContextClassLoader());
        schemaReducer =
                new SchemaReducer(
                        "SchemaReducer",
                        mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(
                                duration, enabledEventTypes, errorsOnEventTypes),
                        behavior,
                        new ArrayList<>(),
                        guaranteesSchemaChangeIsolation);
        schemaReducerGateway = new TestingSchemaReducerGateway(schemaReducer);
    }

    public void open() throws Exception {
        schemaReducer.start();
        initializeOperator();
        operator.open();
    }

    public LinkedList<StreamRecord<E>> getOutputRecords() {
        return outputRecords;
    }

    public void clearOutputRecords() {
        outputRecords.clear();
    }

    public OP getOperator() {
        return operator;
    }

    public void registerTableSchema(TableId tableId, Schema schema) {
        schemaReducer.emplaceUpstreamSchema(tableId, 0, schema);
        schemaReducer.emplaceEvolvedSchema(tableId, schema);
    }

    public Schema getLatestEvolvedSchema(TableId tableId) throws Exception {
        return ((GetEvolvedSchemaResponse)
                        unwrap(
                                schemaReducer
                                        .handleCoordinationRequest(
                                                new GetEvolvedSchemaRequest(
                                                        tableId,
                                                        GetEvolvedSchemaRequest
                                                                .LATEST_SCHEMA_VERSION))
                                        .get()))
                .getSchema()
                .orElse(null);
    }

    public boolean isJobFailed() {
        return mockedContext.isJobFailed();
    }

    public Throwable getJobFailureCause() {
        return mockedContext.getFailureCause();
    }

    @Override
    public void close() throws Exception {
        operator.close();
    }

    // -------------------------------------- Helper functions -------------------------------

    private void initializeOperator() throws Exception {
        operator.setup(
                new MockStreamTask(schemaReducerGateway),
                new MockStreamConfig(new Configuration(), numOutputs),
                new EventCollectingOutput<>(outputRecords, schemaReducerGateway));
        schemaReducerGateway.sendOperatorEventToCoordinator(
                SINK_OPERATOR_ID, new SerializedValue<>(new SinkWriterRegisterEvent(0)));
    }

    // ---------------------------------------- Helper classes ---------------------------------

    private static class EventCollectingOutput<E extends Event> implements Output<StreamRecord<E>> {
        private final LinkedList<StreamRecord<E>> outputRecords;

        private final TestingSchemaReducerGateway schemaRegistryGateway;

        public EventCollectingOutput(
                LinkedList<StreamRecord<E>> outputRecords,
                TestingSchemaReducerGateway schemaRegistryGateway) {
            this.outputRecords = outputRecords;
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public void collect(StreamRecord<E> record) {
            outputRecords.add(record);
            Event event = record.getValue();
            if (event instanceof FlushEvent) {
                try {
                    schemaRegistryGateway.sendOperatorEventToCoordinator(
                            SINK_OPERATOR_ID, new SerializedValue<>(new FlushSuccessEvent(0)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void close() {}
    }

    private static class MockStreamTask extends StreamTask<Event, AbstractStreamOperator<Event>> {
        protected MockStreamTask(TestingSchemaReducerGateway schemaRegistryGateway)
                throws Exception {
            super(new SchemaRegistryCoordinatingEnvironment(schemaRegistryGateway));
        }

        @Override
        protected void init() {}
    }

    private static class SchemaRegistryCoordinatingEnvironment extends DummyEnvironment {
        private final TestingSchemaReducerGateway schemaRegistryGateway;

        public SchemaRegistryCoordinatingEnvironment(
                TestingSchemaReducerGateway schemaRegistryGateway) {
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
            return schemaRegistryGateway;
        }
    }
}
