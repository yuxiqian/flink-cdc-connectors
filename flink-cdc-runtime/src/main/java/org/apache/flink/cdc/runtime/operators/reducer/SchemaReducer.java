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
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaReducingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.reducer.events.BlockUpstreamRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.events.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.operators.reducer.utils.SchemaNormalizer;
import org.apache.flink.cdc.runtime.operators.reducer.utils.TableIdRouter;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.LinkedHashMultimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Multimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.runtime.operators.reducer.events.CoordinationResponseUtils.wrap;

/** Coordinator node for {@link SchemaMapper}. */
public class SchemaReducer implements OperatorCoordinator, CoordinationRequestHandler {

    // -------------------------
    // Statically initialized fields
    // -------------------------

    private static final Logger LOG = LoggerFactory.getLogger(SchemaReducer.class);
    private final OperatorCoordinator.Context context;
    private final String operatorName;
    private final ExecutorService coordinatorExecutor;
    private final MetadataApplier metadataApplier;
    private final SchemaChangeBehavior schemaChangeBehavior;
    private final List<RouteRule> routingRules;
    private final boolean guaranteesSchemaChangeIsolation;

    public SchemaReducer(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            SchemaChangeBehavior schemaChangeBehavior,
            List<RouteRule> routingRules,
            boolean guaranteesSchemaChangeIsolation) {
        this.context = context;
        this.coordinatorExecutor = coordinatorExecutor;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.routingRules = routingRules;
        this.guaranteesSchemaChangeIsolation = guaranteesSchemaChangeIsolation;
    }

    // -------------------------
    // Dynamically initialized fields (after coordinator starts)
    // -------------------------

    private transient Set<Integer> activeSinkWriters;
    private transient Set<Integer> flushedSinkWriters;
    private transient Map<Integer, SubtaskGateway> subtaskGatewayMap;
    private transient Map<Integer, Throwable> failedReasons;
    private transient int currentParallelism;
    private transient AtomicReference<RequestStatus> reducerStatus;
    private transient Map<
                    Integer, Tuple2<ReduceSchemaRequest, CompletableFuture<CoordinationResponse>>>
            pendingRequests;
    private transient Table<TableId, Integer, Schema> upstreamSchemaTable;
    private transient TableIdRouter tableIdRouter;
    // We only use SchemaManager to store "evolved" schemas that always keeps in sync with sink
    // database.
    // It is not possible to hold reliable upstream schema from different source subTasks.
    private transient SchemaManager schemaManager;

    // This number was kept in-sync to indicate the number of global schema reducing requests that
    // have been processed.
    // Used for filtering out late-coming BlockUpstreamRequest if requestSchemaReduce was already
    // emitted in `processElement` method.
    private transient AtomicInteger schemaMapperSeqNum;

    // -------------------------
    // Lifecycle methods
    // -------------------------
    @Override
    public void start() throws Exception {
        LOG.info("Starting SchemaReducer for {}.", operatorName);
        this.schemaManager = new SchemaManager(schemaChangeBehavior);
        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.flushedSinkWriters = ConcurrentHashMap.newKeySet();
        this.subtaskGatewayMap = new ConcurrentHashMap<>();
        this.failedReasons = new ConcurrentHashMap<>();
        this.currentParallelism = context.currentParallelism();
        this.reducerStatus = new AtomicReference<>(RequestStatus.IDLE);
        this.pendingRequests = new ConcurrentHashMap<>();
        this.upstreamSchemaTable = HashBasedTable.create();
        this.tableIdRouter = new TableIdRouter(routingRules);
        this.schemaMapperSeqNum = new AtomicInteger(0);
        LOG.info(
                "Started SchemaRegistry for {}. Parallelism: {}", operatorName, currentParallelism);
    }

    @Override
    public void close() throws Exception {
        LOG.info("SchemaRegistry for {} closed.", operatorName);
        coordinatorExecutor.shutdown();
    }

    // -------------------------
    // Event handler entrances (for schema mappers and sink operators)
    // -------------------------

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

    // -------------------------
    // Schema reducing logic
    // -------------------------

    private void handleReduceSchemaRequest(
            ReduceSchemaRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        LOG.info("Reducer received schema reduce request {}.", request);
        pendingRequests.put(request.getSinkSubTaskId(), Tuple2.of(request, responseFuture));

        if (!request.isAlignRequest()) {
            SchemaChangeEvent schemaChangeEvent = request.getSchemaChangeEvent();
            updateUpstreamSchemaTable(
                    schemaChangeEvent.tableId(), request.getSourceSubTaskId(), schemaChangeEvent);
        }

        if (pendingRequests.size() == 1) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(RequestStatus.IDLE, RequestStatus.BROADCASTING),
                    "Unexpected reducer status: " + reducerStatus.get());
            LOG.info(
                    "Received the very-first schema reduce request {}. Switching from IDLE to BROADCASTING.",
                    request);
            broadcastBlockUpstreamRequest();
        }

        // No else if, since currentParallelism might be == 1
        if (pendingRequests.size() == currentParallelism) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(RequestStatus.BROADCASTING, RequestStatus.EVOLVING),
                    "Unexpected reducer status: " + reducerStatus.get());
            LOG.info(
                    "Received the last required schema reduce request {}. Switching from BROADCASTING to EVOLVING.",
                    request);
            startSchemaChangesReduce();
        }
    }

    /**
     * Tries to apply schema change event {@code schemaChangeEvent} to the combination of {@code
     * tableId} and {@code sourcePartition}. Returns {@code true} if schema got changed, or {@code
     * false} if nothing gets touched.
     */
    private boolean updateUpstreamSchemaTable(
            TableId tableId, int sourcePartition, SchemaChangeEvent schemaChangeEvent) {
        Schema oldSchema = upstreamSchemaTable.get(tableId, sourcePartition);
        if (SchemaReducingUtils.isSchemaChangeEventRedundant(oldSchema, schemaChangeEvent)) {
            return false;
        }
        upstreamSchemaTable.put(
                tableId,
                sourcePartition,
                SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));
        return true;
    }

    private void startSchemaChangesReduce() {
        LOG.info("Starting to reduce schema. ");
        loopWhen(
                () -> flushedSinkWriters.size() < currentParallelism,
                () ->
                        LOG.info(
                                "Not all sink writers have successfully flushed. Expected {}, actual {}",
                                currentParallelism,
                                flushedSinkWriters));

        LOG.info("All flushed. Going to reduce schema for pending requests: {}", pendingRequests);
        flushedSinkWriters.clear();

        // Deduce what schema change events should be applied to sink table
        List<SchemaChangeEvent> deducedSchemaChangeEvents = deduceEvolvedSchemaChanges();

        // And tries to apply it to external system
        List<SchemaChangeEvent> successfullyAppliedSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent appliedSchemaChangeEvent : deducedSchemaChangeEvents) {
            if (applyAndUpdateEvolvedSchemaChange(appliedSchemaChangeEvent)) {
                successfullyAppliedSchemaChangeEvents.add(appliedSchemaChangeEvent);
            }
        }

        // Then, we increment the seqNum, broadcast affected schema changes to mapper, and release
        // upstream
        int nextSeqNum = schemaMapperSeqNum.incrementAndGet();
        pendingRequests.forEach(
                (subTaskId, tuple) -> {
                    LOG.info("Reducer finishes pending future from {}", subTaskId);
                    tuple.f1.complete(
                            wrap(
                                    new ReduceSchemaResponse(
                                            successfullyAppliedSchemaChangeEvents, nextSeqNum)));
                });

        pendingRequests.clear();

        LOG.info("Finished schema evolving. Switching from EVOLVING to IDLE.");
        reducerStatus.compareAndSet(RequestStatus.EVOLVING, RequestStatus.IDLE);
    }

    private List<SchemaChangeEvent> deduceEvolvedSchemaChanges() {
        List<ReduceSchemaRequest> validSchemaReduceRequests =
                pendingRequests.values().stream()
                        .map(e -> e.f0)
                        .filter(
                                request ->
                                        !request.isAlignRequest()) // Ignore alignment only requests
                        .collect(Collectors.toList());

        // Sink tables that got affected by current schema reduce
        Set<TableId> affectedSinkTableIds = new HashSet<>();
        Multimap<TableId, SchemaChangeEvent> affectedSinkTableIdsCause =
                LinkedHashMultimap.create();

        List<SchemaChangeEvent> evolvedSchemaChanges = new ArrayList<>();

        for (ReduceSchemaRequest schemaReduceRequest : validSchemaReduceRequests) {
            SchemaChangeEvent schemaChangeEvent = schemaReduceRequest.getSchemaChangeEvent();
            TableId tableId = schemaChangeEvent.tableId();

            tableIdRouter
                    .route(tableId)
                    .forEach(
                            sinkTableId -> {
                                affectedSinkTableIds.add(sinkTableId);
                                affectedSinkTableIdsCause.put(sinkTableId, schemaChangeEvent);
                            });
        }

        for (TableId sinkTableId : affectedSinkTableIds) {
            Schema oldEvolvedSchema =
                    schemaManager.getLatestEvolvedSchema(sinkTableId).orElse(null);

            // Local schema changes, before being lenientized and applied to sink
            List<SchemaChangeEvent> localSinkTableSchemaChanges = new ArrayList<>();

            // Collect all upstream schemas that merges into this sink table
            Set<TableId> upstreamMergingTableIds =
                    retrieveUpstreamTableIdsFromEvolvedTableId(sinkTableId);
            Set<Schema> upstreamMergingSchemas =
                    retrieveUpstreamSchemasFromEvolvedTableId(sinkTableId);

            Preconditions.checkState(
                    !upstreamMergingTableIds.isEmpty(),
                    "Upstream merging tableIds should never be empty.");
            Preconditions.checkState(
                    !upstreamMergingSchemas.isEmpty(),
                    "Upstream merging schemas should never be empty.");

            // Indicating if we can forward upstream schema change events directly to sink.
            // This could be true if:
            // a) We're using a schema change isolated source, and there's no table merging route
            // rules.
            // or b), when we're using a non-isolated source, and it only presents in one single
            // subTask, it could be safely forwarded, too.
            boolean isSchemaChangeForwardable;
            if (guaranteesSchemaChangeIsolation) {
                isSchemaChangeForwardable = upstreamMergingTableIds.size() == 1;
            } else {
                isSchemaChangeForwardable = upstreamMergingSchemas.size() == 1;
            }

            if (isSchemaChangeForwardable) {
                // This is a one-to-one relationship, we can simply forward the upstream schema
                // changes to sink table.
                List<SchemaChangeEvent> causeSchemaChangeEvents =
                        new ArrayList<>(affectedSinkTableIdsCause.get(sinkTableId));

                for (SchemaChangeEvent forwardedSchemaChangeEvent : causeSchemaChangeEvents) {
                    if (!SchemaReducingUtils.isSchemaChangeEventRedundant(
                            oldEvolvedSchema, forwardedSchemaChangeEvent)) {
                        localSinkTableSchemaChanges.add(
                                forwardedSchemaChangeEvent.copy(sinkTableId));
                    } else {
                        LOG.info(
                                "Received a duplicate forwarded schema change event {}, ignoring it.",
                                forwardedSchemaChangeEvent);
                    }
                }
            } else {
                Schema newEvolvedSchema = oldEvolvedSchema;

                // Otherwise, we try to merge entire upstream schemas
                for (Schema upstreamSchema : upstreamMergingSchemas) {
                    newEvolvedSchema =
                            SchemaReducingUtils.getLeastCommonSchema(
                                    newEvolvedSchema, upstreamSchema);
                }

                localSinkTableSchemaChanges.addAll(
                        SchemaReducingUtils.getSchemaDifference(
                                sinkTableId, oldEvolvedSchema, newEvolvedSchema));
            }

            // Then, we rewrite schema change events based on current schema change behavior
            evolvedSchemaChanges.addAll(
                    SchemaNormalizer.normalizeSchemaChangeEvents(
                            oldEvolvedSchema,
                            localSinkTableSchemaChanges,
                            schemaChangeBehavior,
                            metadataApplier));
        }
        return evolvedSchemaChanges;
    }

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            metadataApplier.applySchemaChange(schemaChangeEvent);
            schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
            return true;
        } catch (Throwable t) {
            if (shouldIgnoreException(t)) {
                LOG.warn(
                        "Failed to apply event {} to external system, but keeps running in TRY_EVOLVE mode. Caused by: {}",
                        schemaChangeEvent,
                        t);
                return false;
            } else {
                context.failJob(
                        new FlinkRuntimeException(
                                "Failed to apply schema change event "
                                        + schemaChangeEvent
                                        + ". Caused by: ",
                                t));
                throw t;
            }
        }
    }

    private void broadcastBlockUpstreamRequest() {
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
                    if (!pendingRequests.containsKey(subTaskId)) {
                        LOG.info("Try to broadcast BlockUpstreamRequest to {}", subTaskId);
                        gateway.sendEvent(new BlockUpstreamRequest(schemaMapperSeqNum.get()));
                    }
                });
    }

    // -------------------------
    // Services handlers for sink registration and schema retrieval
    // -------------------------

    private void handleFlushSuccessEvent(FlushSuccessEvent event) {
        LOG.info("Sink subtask {} succeed flushing.", event.getSubtask());
        flushedSinkWriters.add(event.getSubtask());
    }

    private void handleGetReducedSchemaRequest(
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

    // -------------------------
    // Flink coordinator related chores
    // -------------------------

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
                        resultFuture.complete(baos.toByteArray());
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

    // -------------------------
    // Utilities
    // -------------------------

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

    private boolean shouldIgnoreException(Throwable throwable) {
        // In IGNORE mode, will never try to apply schema change events
        // In EVOLVE and LENIENT mode, such failure will not be tolerated
        // In EXCEPTION mode, an exception will be thrown once captured
        return (throwable instanceof UnsupportedSchemaChangeEventException)
                && (schemaChangeBehavior == SchemaChangeBehavior.TRY_EVOLVE);
    }

    private Set<TableId> retrieveUpstreamTableIdsFromEvolvedTableId(TableId evolvedTableId) {
        return upstreamSchemaTable.rowKeySet().stream()
                .filter(
                        upstreamTableId ->
                                tableIdRouter.route(upstreamTableId).contains(evolvedTableId))
                .collect(Collectors.toSet());
    }

    private Set<Schema> retrieveUpstreamSchemasFromEvolvedTableId(TableId evolvedTableId) {
        return retrieveUpstreamTableIdsFromEvolvedTableId(evolvedTableId).stream()
                .flatMap(
                        upstreamTableId ->
                                upstreamSchemaTable.row(upstreamTableId).values().stream())
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    public void emplaceUpstreamSchema(
            @Nonnull TableId tableId, int sourcePartition, @Nonnull Schema schema) {
        upstreamSchemaTable.put(tableId, sourcePartition, schema);
    }

    @VisibleForTesting
    public void emplaceEvolvedSchema(@Nonnull TableId tableId, @Nonnull Schema schema) {
        schemaManager.emplaceEvolvedSchema(tableId, schema);
    }
}
