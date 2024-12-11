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

package org.apache.flink.cdc.runtime.operators.schema.distributed;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.TableIdRouter;
import org.apache.flink.cdc.runtime.operators.schema.common.event.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.event.distributed.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.distributed.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
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

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** This operator merges upstream inferred schema into a centralized Schema Registry. */
public class SchemaOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<PartitioningEvent, Event>,
                OperatorEventHandler,
                Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOperator.class);

    // Final fields that are set upon construction
    private final Duration rpcTimeOut;
    private final String timezone;
    private final List<RouteRule> routingRules;

    public SchemaOperator(List<RouteRule> routingRules, Duration rpcTimeOut, String timezone) {
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
                    new SchemaChangeRequest(sourcePartition, subTaskId, schemaChangeEvent));
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();

            // First, we obtain the upstream schema corresponding to this data change event
            Schema upstreamSchema =
                    upstreamSchemaTable.get(dataChangeEvent.tableId(), sourcePartition);

            // Then, for each routing terminus, coerce data records to the expected schema
            for (TableId sinkTableId : tableIdRouter.route(tableId)) {
                Schema evolvedSchema = evolvedSchemaMap.get(sinkTableId);

                DataChangeEvent coercedDataRecord =
                        SchemaDerivator.coerceDataRecord(
                                        timezone,
                                        DataChangeEvent.route(dataChangeEvent, sinkTableId),
                                        upstreamSchema,
                                        evolvedSchema)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        String.format(
                                                                "Unable to coerce data record from %s (schema: %s) to %s (schema: %s)",
                                                                tableId,
                                                                upstreamSchema,
                                                                sinkTableId,
                                                                evolvedSchema)));
                output.collect(new StreamRecord<>(coercedDataRecord));
            }
        } else {
            throw new IllegalStateException(
                    subTaskId + "> SchemaMapper received an unexpected event: " + event);
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        throw new IllegalArgumentException("Unexpected operator event: " + event);
    }

    private void requestSchemaReduce(SchemaChangeRequest reduceSchemaRequest) {
        LOG.info("{}> Sent FlushEvent to downstream...", subTaskId);
        output.collect(new StreamRecord<>(FlushEvent.ofAll()));

        LOG.info("{}> Sending reduce request...", subTaskId);
        SchemaChangeResponse response = sendRequestToCoordinator(reduceSchemaRequest);

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

    private <REQUEST extends CoordinationRequest, RESPONSE extends CoordinationResponse>
            RESPONSE sendRequestToCoordinator(REQUEST request) {
        try {
            CompletableFuture<CoordinationResponse> responseFuture =
                    toCoordinator.sendRequestToCoordinator(
                            getOperatorID(), new SerializedValue<>(request));
            return CoordinationResponseUtils.unwrap(
                    responseFuture.get(rpcTimeOut.toMillis(), TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to send request to coordinator: " + request.toString(), e);
        }
    }
}
