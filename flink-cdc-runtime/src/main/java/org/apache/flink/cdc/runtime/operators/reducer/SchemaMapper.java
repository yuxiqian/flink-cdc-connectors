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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaInferencingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.reducer.events.BlockUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.utils.TableIdRouter;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
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

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** This operator merges upstream inferred schema into a centralized Schema Registry. */
public class SchemaMapper extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<PartitioningEvent, Event>,
                OperatorEventHandler,
                Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMapper.class);

    // Final fields that are set upon construction
    private final Duration rpcTimeOut;
    private final String timezone;
    private final List<RouteRule> routingRules;

    public SchemaMapper(List<RouteRule> routingRules, Duration rpcTimeOut, String timezone) {
        this.routingRules = routingRules;
        this.rpcTimeOut = rpcTimeOut;
        this.timezone = timezone;
    }

    // Transient fields that are set when operator is running
    private transient TaskOperatorEventGateway toCoordinator;
    private transient int subTaskId;

    // Records TableId and its integer.
    private transient volatile Table<TableId, Integer, Schema> upstreamSchemaTable;
    private transient volatile Map<TableId, Schema> evolvedSchemaMap;
    private transient TableIdRouter tableIdRouter;
    private transient volatile int schemaMapperSeqNum;

    @Override
    public void open() throws Exception {
        super.open();
        subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        upstreamSchemaTable = HashBasedTable.create();
        evolvedSchemaMap = new HashMap<>();
        tableIdRouter = new TableIdRouter(routingRules);
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
    public void processElement(StreamRecord<PartitioningEvent> streamRecord) throws Exception {
        // Unpack partitioned events
        PartitioningEvent partitioningEvent = streamRecord.getValue();
        System.out.printf(
                "%d> Schema Mapper ::: Received a partitioning event %s\n",
                subTaskId, partitioningEvent);
        Event event = partitioningEvent.getPayload();
        int sourcePartition = partitioningEvent.getSourcePartition();

        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();

            // First, update upstream schema map unconditionally and it will never fail
            Schema beforeSchema = upstreamSchemaTable.get(tableId, sourcePartition);
            Schema afterSchema =
                    SchemaUtils.applySchemaChangeEvent(beforeSchema, schemaChangeEvent);
            upstreamSchemaTable.put(tableId, sourcePartition, afterSchema);

            // Then, notify this information to the reducer
            requestSchemaReduce(
                    new ReduceSchemaRequest(sourcePartition, subTaskId, schemaChangeEvent));
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();

            // First, we obtain the upstream schema corresponding to this data change event
            Schema upstreamSchema =
                    upstreamSchemaTable.get(dataChangeEvent.tableId(), sourcePartition);

            // Then, for each routing terminus, coerce data records to the expected schema
            for (TableId sinkTableId : tableIdRouter.route(tableId)) {
                Schema evolvedSchema = evolvedSchemaMap.get(sinkTableId);
                coerceDataRecord(
                                DataChangeEvent.route(dataChangeEvent, sinkTableId),
                                upstreamSchema,
                                evolvedSchema)
                        .ifPresent(evt -> output.collect(new StreamRecord<>(evt)));
            }
        } else {
            throw new IllegalStateException(
                    subTaskId + "> SchemaMapper received an unexpected event: " + event);
        }
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

            // Request a dummy schema change request (though no schema change events are received
            // from upstream) to align all schema mappers by ensuring they are all blocked and sent
            // flush events.
            requestSchemaReduce(ReduceSchemaRequest.createAlignRequest(subTaskId));
        } else {
            throw new IllegalArgumentException("Unexpected operator event: " + event);
        }
    }

    private void requestSchemaReduce(ReduceSchemaRequest reduceSchemaRequest) {
        LOG.info("{}> Sent FlushEvent to downstream...", subTaskId);
        output.collect(new StreamRecord<>(new FlushEvent()));

        LOG.info("{}> Sending reduce request...", subTaskId);
        ReduceSchemaResponse response = sendRequestToCoordinator(reduceSchemaRequest);

        LOG.info("{}> Reduce request response: {}", subTaskId, response);

        // Update local evolved schema cache
        response.getReducedSchemaResult()
                .forEach(
                        schemaChangeEvent ->
                                evolvedSchemaMap.compute(
                                        schemaChangeEvent.tableId(),
                                        (tableId, schema) ->
                                                SchemaUtils.applySchemaChangeEvent(
                                                        schema, schemaChangeEvent)));

        // And emit schema change events to downstream
        response.getReducedSchemaResult().forEach(evt -> output.collect(new StreamRecord<>(evt)));

        schemaMapperSeqNum = response.getReduceSeqNum();
        LOG.info(
                "{}> Successfully updated evolved schema cache. Current state: {} at version {}",
                subTaskId,
                evolvedSchemaMap,
                schemaMapperSeqNum);
    }

    private Optional<DataChangeEvent> coerceDataRecord(
            DataChangeEvent dataChangeEvent,
            Schema upstreamSchema,
            @Nullable Schema evolvedSchema) {
        if (evolvedSchema == null) {
            // Sink does not recognize this tableId, might have been dropped, just ignore it.
            return Optional.empty();
        }

        if (upstreamSchema.equals(evolvedSchema)) {
            // If there's no schema difference, just return the original event.
            return Optional.of(dataChangeEvent);
        }

        // TODO: We may cache these accessors later for better performance.
        List<RecordData.FieldGetter> upstreamSchemaReader =
                SchemaUtils.createFieldGetters(upstreamSchema);
        BinaryRecordDataGenerator evolvedSchemaWriter =
                new BinaryRecordDataGenerator(
                        evolvedSchema.getColumnDataTypes().toArray(new DataType[0]));

        // Coerce binary data records
        if (dataChangeEvent.before() != null) {
            List<Object> upstreamFields =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.before(), upstreamSchemaReader);
            Object[] coercedRow =
                    SchemaInferencingUtils.coerceRow(
                            timezone, evolvedSchema, upstreamSchema, upstreamFields);

            dataChangeEvent =
                    DataChangeEvent.projectBefore(
                            dataChangeEvent, evolvedSchemaWriter.generate(coercedRow));
        }

        if (dataChangeEvent.after() != null) {
            List<Object> upstreamFields =
                    SchemaUtils.restoreOriginalData(dataChangeEvent.after(), upstreamSchemaReader);
            Object[] coercedRow =
                    SchemaInferencingUtils.coerceRow(
                            timezone, evolvedSchema, upstreamSchema, upstreamFields);

            dataChangeEvent =
                    DataChangeEvent.projectAfter(
                            dataChangeEvent, evolvedSchemaWriter.generate(coercedRow));
        }

        return Optional.of(dataChangeEvent);
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
