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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaInferencingUtils;
import org.apache.flink.cdc.runtime.operators.reducer.events.BlockUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava31.com.google.common.collect.ArrayListMultimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/** Coordinator node for {@link SchemaMapper}. */
public class SchemaReducer implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaReducer.class);

    private final OperatorCoordinator.Context context;
    private final String operatorName;
    private final ExecutorService coordinatorExecutor;
    private final MetadataApplier metadataApplier;

    public SchemaReducer(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier) {
        this.context = context;
        this.coordinatorExecutor = coordinatorExecutor;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
    }

    private transient Set<Integer> activeSinkWriters;
    private transient Set<Integer> flushedSinkWriters;
    private transient Map<Integer, SubtaskGateway> subtaskGatewayMap;
    private transient Map<Integer, Throwable> failedReasons;
    private transient int currentParallelism;
    private transient AtomicReference<RequestStatus> reducerStatus;
    private transient AtomicInteger blockedUpstreamCount;
    private transient Map<
                    Integer, Tuple2<ReduceSchemaRequest, CompletableFuture<CoordinationResponse>>>
            pendingRequests;

    // This number was kept in-sync to indicate the number of global schema reducing requests that
    // have been processed.
    // Used for filtering out late-coming BlockUpstreamRequest if requestSchemaReduce was already
    // emitted in `processElement` method.
    private transient AtomicInteger schemaMapperSeqNum;

    // We only use SchemaManager to store "evolved" schemas that always keeps in sync with sink
    // database.
    // Upstream schema is meaningless since in schema inferencing mode, a unified upstream schema
    // does not exist.
    private transient SchemaManager schemaManager;

    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaReducer for {}.", operatorName);
        this.schemaManager = new SchemaManager(SchemaChangeBehavior.LENIENT);
        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.flushedSinkWriters = ConcurrentHashMap.newKeySet();
        this.subtaskGatewayMap = new ConcurrentHashMap<>();
        this.failedReasons = new ConcurrentHashMap<>();
        this.currentParallelism = context.currentParallelism();
        this.reducerStatus = new AtomicReference<>(RequestStatus.IDLE);
        this.blockedUpstreamCount = new AtomicInteger(0);
        this.pendingRequests = new HashMap<>();
        this.schemaMapperSeqNum = new AtomicInteger(0);
        LOG.info(
                "Started SchemaRegistry for {}. Parallelism: {}", operatorName, currentParallelism);
    }

    @Override
    public void close() throws Exception {
        LOG.info("SchemaRegistry for {} closed.", operatorName);
        coordinatorExecutor.shutdown();
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    try {
                        if (request instanceof GetEvolvedSchemaRequest) {
                            handleGetReducedSchemaRequest(
                                    ((GetEvolvedSchemaRequest) request), responseFuture);
                        } else if (request instanceof ReduceSchemaRequest) {
                            handleReduceSchemaRequest(
                                    (ReduceSchemaRequest) request, responseFuture);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unrecognized CoordinationRequest type: " + request);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling coordination request %s",
                request);
        return responseFuture;
    }

    @Override
    public void handleEventFromOperator(int subTaskId, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    try {
                        if (event instanceof FlushSuccessEvent) {
                            handleFlushSuccessEvent((FlushSuccessEvent) event);
                        } else if (event instanceof SinkWriterRegisterEvent) {
                            activeSinkWriters.add(((SinkWriterRegisterEvent) event).getSubtask());
                        } else {
                            throw new FlinkRuntimeException(
                                    "Unrecognized Operator Event: " + event);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling event %s from subTask %d",
                event,
                subTaskId);
    }

    @Override
    public void subtaskReset(int subTaskId, long checkpointId) {
        Throwable rootCause = failedReasons.get(subTaskId);
        LOG.error("Subtask {} reset at checkpoint {}.", subTaskId, checkpointId, rootCause);
        subtaskGatewayMap.remove(subTaskId);
    }

    @Override
    public void executionAttemptFailed(
            int subTaskId, int attemptNumber, @Nullable Throwable reason) {
        failedReasons.put(subTaskId, reason);
    }

    @Override
    public void executionAttemptReady(int subTaskId, int attemptNumber, SubtaskGateway gateway) {
        subtaskGatewayMap.put(subTaskId, gateway);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        runInEventLoop(
                () -> {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            DataOutputStream out = new DataOutputStream(baos)) {
                        // Serialize SchemaManager
                        int schemaManagerSerializerVersion = SchemaManager.SERIALIZER.getVersion();
                        out.writeInt(schemaManagerSerializerVersion);
                        byte[] serializedSchemaManager =
                                SchemaManager.SERIALIZER.serialize(schemaManager);
                        out.writeInt(serializedSchemaManager.length);
                        out.write(serializedSchemaManager);
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        if (checkpointData == null) {
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();
            int length = in.readInt();
            byte[] serializedSchemaManager = new byte[length];
            in.readFully(serializedSchemaManager);
            schemaManager =
                    SchemaManager.SERIALIZER.deserialize(
                            schemaManagerSerializerVersion, serializedSchemaManager);
        } catch (Throwable t) {
            context.failJob(t);
            throw t;
        }
    }

    protected void handleReduceSchemaRequest(
            ReduceSchemaRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        LOG.info("Reducer received schema reduce request {}...", request);
        pendingRequests.put(request.getSubTaskId(), Tuple2.of(request, responseFuture));

        int blockCnt = blockedUpstreamCount.incrementAndGet();

        if (blockCnt == 1) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(RequestStatus.IDLE, RequestStatus.BROADCASTING),
                    "Unexpected reducer status: " + reducerStatus.get());
            LOG.info(
                    "Received the very-first schema reduce request {}. Switching from IDLE to BROADCASTING.",
                    request);
            broadcastFlushEventRequest(request.getTableId());
        }

        // No else if, since currentParallelism might be == 1
        if (blockCnt == currentParallelism) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(RequestStatus.BROADCASTING, RequestStatus.EVOLVING),
                    "Unexpected reducer status: " + reducerStatus.get());
            reduceSchema();
        }
    }

    protected void handleFlushSuccessEvent(FlushSuccessEvent event) {
        LOG.info(
                "Sink subtask {} succeed flushing for table {}.",
                event.getSubtask(),
                event.getTableId().toString());
        flushedSinkWriters.add(event.getSubtask());
    }

    protected void handleGetReducedSchemaRequest(
            GetEvolvedSchemaRequest getEvolvedSchemaRequest,
            CompletableFuture<CoordinationResponse> response) {
        LOG.info("Handling evolved schema request: {}", getEvolvedSchemaRequest);
        int schemaVersion = getEvolvedSchemaRequest.getSchemaVersion();
        TableId tableId = getEvolvedSchemaRequest.getTableId();
        if (schemaVersion == GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION) {
            response.complete(
                    wrap(
                            new GetEvolvedSchemaResponse(
                                    schemaManager.getLatestEvolvedSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getEvolvedSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed evolved schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                response.complete(wrap(new GetEvolvedSchemaResponse(null)));
            }
        }
    }

    private void broadcastFlushEventRequest(TableId tableId) {
        // We must wait for all subTasks being successfully registered before broadcasting anything.
        loopWhen(
                () -> subtaskGatewayMap.size() < currentParallelism,
                () ->
                        LOG.info(
                                "Not all subTasks have been registered. Expected {}, actual {}",
                                currentParallelism,
                                subtaskGatewayMap.keySet()));
        subtaskGatewayMap.forEach(
                (subTaskId, gateway) -> {
                    LOG.info("Try to broadcast FlushEventRequest for {} to {}", tableId, subTaskId);
                    gateway.sendEvent(new BlockUpstreamRequest(tableId, schemaMapperSeqNum.get()));
                });
    }

    private void reduceSchema() {
        LOG.info("Starting to reduce schema. ");
        loopWhen(
                () -> flushedSinkWriters.size() < currentParallelism,
                () ->
                        LOG.info(
                                "Not all sink writers have successfully flushed. Expected {}, actual {}",
                                currentParallelism,
                                flushedSinkWriters));

        LOG.info("All flushed. Going to reduce schema for pending requests: {}", pendingRequests);

        // Organize reduced schemas by TableId
        Multimap<TableId, Schema> groupedPendingSchemas = ArrayListMultimap.create();
        pendingRequests.values().stream()
                .map(e -> e.f0)
                .filter(request -> !request.isAlignRequest())
                .forEach(
                        request ->
                                groupedPendingSchemas.put(
                                        request.getTableId(), request.getSchema()));

        // Remembering tables whose schema got changed. Used to broadcast it to SchemaMappers later.
        Map<TableId, Schema> updatedSchemasMap = new HashMap<>();

        for (TableId tableId : groupedPendingSchemas.keySet()) {
            Tuple2<Schema, List<SchemaChangeEvent>> inferResults =
                    inferWiderSchemaAndChanges(
                            tableId,
                            schemaManager.getLatestEvolvedSchema(tableId).orElse(null),
                            groupedPendingSchemas.get(tableId));

            // The widest inferred schema
            Schema newSchema = inferResults.f0;

            // The corresponding schema change events that required to make current schema to the
            // newSchema.
            List<SchemaChangeEvent> schemaChangeEvents = inferResults.f1;

            // First we apply the schema change events to external DBMS via Metadata Applier...
            schemaChangeEvents.forEach(metadataApplier::applySchemaChange);

            // Then, update schema manager's cache...
            schemaManager.emplaceEvolvedSchema(tableId, newSchema);

            // Mark this table's schema has changed, will broadcast it to mapper later...
            updatedSchemasMap.put(tableId, newSchema);
        }

        int nextSeqNum = schemaMapperSeqNum.incrementAndGet();
        pendingRequests.forEach(
                (subTaskId, tuple) -> {
                    LOG.info("Reducer finishes pending future from {}", subTaskId);
                    tuple.f1.complete(
                            wrap(new ReduceSchemaResponse(updatedSchemasMap, nextSeqNum)));
                });

        flushedSinkWriters.clear();
        blockedUpstreamCount.set(0);
        pendingRequests.clear();

        LOG.info("Finished schema evolving. Switching from EVOLVING to IDLE.");
        reducerStatus.compareAndSet(RequestStatus.EVOLVING, RequestStatus.IDLE);
    }

    /**
     * Given {@code currentSchema} and {@code upcomingSchema}, this function infers the wider schema
     * that is compatible with {@code currentSchema} (which means we only alter column types wider
     * and adds NULLABLE columns at last), and generates corresponding {@link SchemaChangeEvent}s
     * for {@link MetadataApplier} to evolve actual downstream DBMS schema. Notice that there will
     * be at most 2 events in the returning {@code List}: An {@link AddColumnEvent} and an {@link
     * AlterColumnTypeEvent}, or a {@link CreateTableEvent}.
     */
    private Tuple2<Schema, List<SchemaChangeEvent>> inferWiderSchemaAndChanges(
            TableId tableId, @Nullable Schema currentSchema, Schema upcomingSchema) {
        Schema commonSchema =
                SchemaInferencingUtils.getLeastCommonSchema(currentSchema, upcomingSchema);
        List<SchemaChangeEvent> requiredSchemaChanges =
                SchemaInferencingUtils.getSchemaDifference(tableId, currentSchema, commonSchema);
        return Tuple2.of(commonSchema, requiredSchemaChanges);
    }

    /** Similar to the previous one, but allows more schemas. */
    private Tuple2<Schema, List<SchemaChangeEvent>> inferWiderSchemaAndChanges(
            TableId tableId, @Nullable Schema currentSchema, Collection<Schema> schemas) {
        Schema commonSchema = currentSchema;
        for (Schema upcomingSchema : schemas) {
            commonSchema =
                    SchemaInferencingUtils.getLeastCommonSchema(commonSchema, upcomingSchema);
        }
        List<SchemaChangeEvent> requiredSchemaChanges =
                SchemaInferencingUtils.getSchemaDifference(tableId, currentSchema, commonSchema);
        return Tuple2.of(commonSchema, requiredSchemaChanges);
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {
        coordinatorExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // if we have a JVM critical error, promote it immediately, there is a good
                        // chance the logging or job failing will not succeed anymore
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the SchemaReducer for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    /**
     * {@code IDLE}: Initial idling state, ready for requests. <br>
     * {@code BROADCASTING}: Trying to block all mappers before they blocked upstream & collect
     * FlushEvents. <br>
     * {@code EVOLVING}: Applying schema change to sink.
     */
    private enum RequestStatus {
        IDLE,
        BROADCASTING,
        EVOLVING
    }

    private void loopWhen(Supplier<Boolean> conditionChecker) {
        while (conditionChecker.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private void loopWhen(Supplier<Boolean> conditionChecker, Runnable message) {
        while (conditionChecker.get()) {
            message.run();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
