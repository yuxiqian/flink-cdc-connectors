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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEventWithSchema;
import org.apache.flink.cdc.common.event.EmplaceTableSchemaEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaInferencingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.reducer.events.BlockUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** This operator merges upstream inferred schema into a centralized Schema Registry. */
public class SchemaMapper extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, OperatorEventHandler, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMapper.class);

    // Final fields that are set upon construction
    private final String timezone;

    public SchemaMapper(Duration rpcTimeOut, String timezone) {
        this.timezone = timezone;
    }

    // Transient fields that are set when operator is running
    private transient TaskOperatorEventGateway toCoordinator;
    private transient int subTaskId;
    private transient volatile Map<TableId, Schema> schemaCacheMap;
    private transient volatile int schemaMapperSeqNum;

    @Override
    public void open() throws Exception {
        super.open();
        subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        schemaCacheMap = new ConcurrentHashMap<>();
        schemaMapperSeqNum = 0;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.toCoordinator = containingTask.getEnvironment().getOperatorCoordinatorEventGateway();
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        // 0. Casting here is safe since we have an assertion check in RouterProcessor
        DataChangeEventWithSchema eventWithSchema =
                (DataChangeEventWithSchema) streamRecord.getValue();
        LOG.info("{}> Schema Mapper received event: {}", subTaskId, eventWithSchema);

        // 1. Check if schema is compatible
        Optional<DataChangeEvent> coercedEvent;

        // 2. Keep trying to coerce event until schema reducer has inferred a wide enough schema
        // that could hold the event.
        while (!(coercedEvent =
                        tryCoerceEvent(
                                eventWithSchema, schemaCacheMap.get(eventWithSchema.tableId())))
                .isPresent()) {
            requestSchemaReduce(eventWithSchema.tableId(), eventWithSchema.getSchema(), false);
        }

        LOG.info(
                "{}> Sending coerced event (with arity: {}/{}) to downstream. Latest schema: {}",
                subTaskId,
                Optional.ofNullable(coercedEvent.get().before()).map(RecordData::getArity),
                Optional.ofNullable(coercedEvent.get().after()).map(RecordData::getArity),
                schemaCacheMap.get(eventWithSchema.tableId()));

        // 3. Safely emit coerced event.
        output.collect(new StreamRecord<>(coercedEvent.get()));
    }

    /**
     * Try to coerce upcoming data change event to current schema. Returns coerced DataChangeEvent
     * if given {@code dataChangeEventWithSchema} can be converted to {@code currentSchema} without
     * loss. Returns {@code Optional#empty} otherwise and a schema reducing might be required.
     */
    private Optional<DataChangeEvent> tryCoerceEvent(
            DataChangeEventWithSchema dataChangeEventWithSchema, @Nullable Schema currentSchema) {
        // It's the first time we this, can't coerce event now
        if (currentSchema == null) {
            LOG.info("{}> current schema is NULL, needs schema reduce.", subTaskId);
            return Optional.empty();
        }
        Schema upcomingSchema = dataChangeEventWithSchema.getSchema();

        // Schema is identical, no need to coerce anything
        if (Objects.equals(currentSchema, upcomingSchema)) {
            return Optional.of(dataChangeEventWithSchema.getDataChangeEvent());
        }

        LOG.info(
                "{}> current schema is {}, upcomingSchema is {}.",
                subTaskId,
                currentSchema,
                upcomingSchema);

        // Schema isn't identical but compatible, we can try to coerce it
        if (SchemaInferencingUtils.isSchemaCompatible(currentSchema, upcomingSchema)) {
            LOG.info("{}> Schema is compatible.", subTaskId);
            BinaryRecordDataGenerator currentSchemaGenerator =
                    new BinaryRecordDataGenerator(
                            currentSchema.getColumnDataTypes().toArray(new DataType[0]));
            List<RecordData.FieldGetter> upcomingFieldGetters =
                    SchemaUtils.createFieldGetters(upcomingSchema);
            DataChangeEvent dataChangeEvent = dataChangeEventWithSchema.getDataChangeEvent();

            List<Object> before =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.before(), upcomingFieldGetters);
            if (before != null) {
                dataChangeEvent =
                        DataChangeEvent.projectBefore(
                                dataChangeEvent,
                                currentSchemaGenerator.generate(
                                        SchemaInferencingUtils.coerceRow(
                                                timezone, currentSchema, upcomingSchema, before)));
            }

            List<Object> after =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.after(), upcomingFieldGetters);
            if (after != null) {
                dataChangeEvent =
                        DataChangeEvent.projectAfter(
                                dataChangeEvent,
                                currentSchemaGenerator.generate(
                                        SchemaInferencingUtils.coerceRow(
                                                timezone, currentSchema, upcomingSchema, after)));
            }

            return Optional.of(dataChangeEvent);
        }

        // Schema is neither identical nor compatible, we need to block upstream and request
        // reducing schema.
        LOG.info("{}> Schema is incompatible.", subTaskId);
        return Optional.empty();
    }

    private void requestSchemaReduce(TableId tableId, Schema schema, boolean isAlign) {
        LOG.info("{}> Sent FlushEvent {} for downstream...", subTaskId, tableId);
        output.collect(new StreamRecord<>(new FlushEvent(tableId)));

        LOG.info("{}> Sending reduce request...", subTaskId);
        ReduceSchemaResponse response =
                sendRequestToCoordinator(
                        isAlign
                                ? ReduceSchemaRequest.alignRequest(subTaskId)
                                : new ReduceSchemaRequest(subTaskId, tableId, schema));

        LOG.info("{}> Reduce request response: {}", subTaskId, response);

        response.getLatestReducedSchema()
                .forEach(
                        (reducedTableId, reducedSchema) -> {
                            schemaCacheMap.put(reducedTableId, reducedSchema);
                            output.collect(
                                    new StreamRecord<>(
                                            new EmplaceTableSchemaEvent(
                                                    reducedTableId, reducedSchema)));
                        });

        schemaMapperSeqNum = response.getReduceSeqNum();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof BlockUpstreamRequest) {
            LOG.info("{}> Received emit flush event request {}.", subTaskId, event);
            BlockUpstreamRequest blockUpstreamRequest = (BlockUpstreamRequest) event;

            if (blockUpstreamRequest.getReduceSeqNum() < schemaMapperSeqNum) {
                LOG.info(
                        "{}> Stale block upstream request, it might have been requested in processElement. Drop it.",
                        subTaskId);
                return;
            }

            // Request a dummy schema change request (though it's not necessary now) to align all
            // schema mappers by ensuring they are all blocked and sent flush events.
            requestSchemaReduce(
                    blockUpstreamRequest.getTableId(),
                    schemaCacheMap.get(blockUpstreamRequest.getTableId()),
                    true);
        } else {
            throw new IllegalArgumentException("Unexpected operator event: " + event);
        }
    }

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(responseFuture.get());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }
}
