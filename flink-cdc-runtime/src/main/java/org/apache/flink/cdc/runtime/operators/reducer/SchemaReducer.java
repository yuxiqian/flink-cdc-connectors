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
import org.apache.flink.cdc.common.event.CreateTableEvent;
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
import org.apache.flink.cdc.runtime.operators.reducer.events.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaRequest;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReduceSchemaResponse;
import org.apache.flink.cdc.runtime.operators.reducer.events.SinkWriterRegisterEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava31.com.google.common.collect.ArrayListMultimap;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava31.com.google.common.collect.HashMultimap;
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
import java.util.Collection;
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

    private transient Multimap<Tuple2<Integer, SchemaChangeEvent>, Integer>
            alreadyHandledSchemaChangeEvents;

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
        this.alreadyHandledSchemaChangeEvents = HashMultimap.create();
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
        if (!request.isNoOpRequest()) {
            LOG.info("It's not an align request, will try to deduplicate.");
            int eventSourcePartitionId = request.getSourceSubTaskId();
            int handlingSinkSubTaskId = request.getSinkSubTaskId();
            SchemaChangeEvent schemaChangeEvent = request.getSchemaChangeEvent();
            Tuple2<Integer, SchemaChangeEvent> uniqueKey =
                    Tuple2.of(eventSourcePartitionId, schemaChangeEvent);
            // Due to the existence of Partitioning Operator, any upstream event will be broadcast
            // to sink N (N = sink parallelism) times.
            // Only the first one should take effect, so we rewrite any other duplicated requests as
            // a no-op align request.
            alreadyHandledSchemaChangeEvents.put(uniqueKey, handlingSinkSubTaskId);
            Collection<Integer> reportedSinkSubTasks =
                    alreadyHandledSchemaChangeEvents.get(uniqueKey);
            if (reportedSinkSubTasks.size() == 1) {
                LOG.info("It's a new request for {}, will handle it", uniqueKey);
                updateUpstreamSchemaTable(
                        schemaChangeEvent.tableId(),
                        request.getSourceSubTaskId(),
                        schemaChangeEvent);
            } else {
                LOG.info(
                        "It's an already handled event {}. It has been handled by {}",
                        uniqueKey,
                        reportedSinkSubTasks);
                request = ReduceSchemaRequest.createNoOpRequest(handlingSinkSubTaskId);
            }
            // Moreover, if we've collected all sink subTasks' request, remove it from memory since
            // no more will be possible.
            if (reportedSinkSubTasks.size() == currentParallelism) {
                LOG.info(
                        "All sink subTasks ({}) have already reported request {}. Remove it out of tracking.",
                        reportedSinkSubTasks,
                        uniqueKey);
                alreadyHandledSchemaChangeEvents.removeAll(request);
            }
        }

        pendingRequests.put(request.getSinkSubTaskId(), Tuple2.of(request, responseFuture));

        if (pendingRequests.size() == 1) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(
                            RequestStatus.IDLE, RequestStatus.WAITING_FOR_FLUSH),
                    "Unexpected reducer status: " + reducerStatus.get());
            LOG.info(
                    "Received the very-first schema reduce request {}. Switching from IDLE to WAITING_FOR_FLUSH.",
                    request);
        }

        // No else if, since currentParallelism might be == 1
        if (pendingRequests.size() == currentParallelism) {
            Preconditions.checkState(
                    reducerStatus.compareAndSet(
                            RequestStatus.WAITING_FOR_FLUSH, RequestStatus.EVOLVING),
                    "Unexpected reducer status: " + reducerStatus.get());
            LOG.info(
                    "Received the last required schema reduce request {}. Switching from WAITING_FOR_FLUSH to EVOLVING.",
                    request);
            startSchemaChangesReduce();
        }
    }

    /**
     * Tries to apply schema change event {@code schemaChangeEvent} to the combination of {@code
     * tableId} and {@code sourcePartition}. Returns {@code true} if schema got changed, or {@code
     * false} if nothing gets touched.
     */
    private void updateUpstreamSchemaTable(
            TableId tableId, int sourcePartition, SchemaChangeEvent schemaChangeEvent) {
        Schema oldSchema = upstreamSchemaTable.get(tableId, sourcePartition);
        upstreamSchemaTable.put(
                tableId,
                sourcePartition,
                SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));
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
                                        !request.isNoOpRequest()) // Ignore alignment only requests
                        .collect(Collectors.toList());

        // Preparation: Group original schema change events by upstream table ID, in case if we need
        // to forward them later.
        Multimap<TableId, SchemaChangeEvent> schemaChangesGroupedByUpstreamTableIds =
                ArrayListMultimap.create();
        validSchemaReduceRequests.forEach(
                rsr ->
                        schemaChangesGroupedByUpstreamTableIds.put(
                                rsr.getSchemaChangeEvent().tableId(), rsr.getSchemaChangeEvent()));

        // Step 1: Based on changed upstream tables, infer a set of sink tables that might be
        // affected by this event. Schema changes will be derived individually for each sink table.
        Set<TableId> affectedSinkTableIds =
                SchemaDerivator.getAffectedEvolvedTables(
                        tableIdRouter,
                        validSchemaReduceRequests.stream()
                                .map(rsr -> rsr.getSchemaChangeEvent().tableId())
                                .collect(Collectors.toSet()));

        List<SchemaChangeEvent> evolvedSchemaChanges = new ArrayList<>();

        // For each affected sink table, we may...
        for (TableId affectedSinkTableId : affectedSinkTableIds) {
            List<SchemaChangeEvent> localEvolvedSchemaChanges = new ArrayList<>();

            Schema currentSinkSchema =
                    schemaManager.getLatestSchema(affectedSinkTableId).orElse(null);

            // Step 2: Reversely lookup this affected sink table's upstream dependency.
            Set<TableId> upstreamDependencies =
                    SchemaDerivator.reverseLookupDependingUpstreamTables(
                            tableIdRouter, affectedSinkTableId, upstreamSchemaTable);

            Preconditions.checkState(
                    !upstreamDependencies.isEmpty(),
                    "An affected sink table's upstream dependency cannot be empty.");

            // We can forward upstream schema change, iff this is a one-by-one routing rule and
            // source could guarantee schema changes' isolation.
            boolean isForwardable =
                    guaranteesSchemaChangeIsolation && upstreamDependencies.size() == 1;

            if (isForwardable) {
                // Step 3a) Obtain the only dependency from upstream...
                TableId upstreamDependencyTable = upstreamDependencies.iterator().next();
                // ...and forward its schema change events to sink, sequentially.

                // Notice that even we're using schema isolation-guaranteed sources, identical
                // CreateTableEvent might be emitted from all source subTasks before starting
                // snapshot phase. This is tolerable, and we don't need to apply it multiple times.
                for (SchemaChangeEvent schemaChangeEvent :
                        schemaChangesGroupedByUpstreamTableIds.get(upstreamDependencyTable)) {
                    if (schemaChangeEvent instanceof CreateTableEvent
                            && schemaManager.schemaExists(affectedSinkTableId)) {
                        LOG.info(
                                "Refused to forward duplicate CreateTableEvent {} since it already exists.",
                                schemaChangeEvent);
                    } else {
                        localEvolvedSchemaChanges.add(schemaChangeEvent.copy(affectedSinkTableId));
                    }
                }
            } else {
                // Step 3b) For non-forwardable cases, we need to grab all upstream schemas from all
                // known partitions and merge them.
                Set<Schema> toBeMergedSchemas =
                        SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                                tableIdRouter, affectedSinkTableId, upstreamSchemaTable);

                // In this mode, schema will never be narrowed because current schema is always one
                // of the merging base. Notice that current schema might be NULL if it's the first
                // time we met a CreateTableEvent.
                Schema mergedSchema = currentSinkSchema;
                for (Schema toBeMergedSchema : toBeMergedSchemas) {
                    mergedSchema =
                            SchemaReducingUtils.getLeastCommonSchema(
                                    mergedSchema, toBeMergedSchema);
                }

                localEvolvedSchemaChanges.addAll(
                        SchemaReducingUtils.getSchemaDifference(
                                affectedSinkTableId, currentSinkSchema, mergedSchema));
            }

            // Step 4) Normalize schema change events, including rewriting events by current schema
            // change behavior configuration, dropping explicitly excluded schema change event
            // types.
            evolvedSchemaChanges.addAll(
                    SchemaDerivator.normalizeSchemaChangeEvents(
                            currentSinkSchema,
                            localEvolvedSchemaChanges,
                            schemaChangeBehavior,
                            metadataApplier));
        }

        return evolvedSchemaChanges;
    }

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            metadataApplier.applySchemaChange(schemaChangeEvent);
            schemaManager.applySchemaChange(schemaChangeEvent);
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
                                    schemaManager.getLatestSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getSchema(tableId, schemaVersion))));
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
     * {@code WAITING_FOR_FLUSH}: Waiting for all mappers to block & collecting enough FlushEvents.
     * <br>
     * {@code EVOLVING}: Applying schema change to sink.
     */
    private enum RequestStatus {
        IDLE,
        WAITING_FOR_FLUSH,
        EVOLVING
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

    @VisibleForTesting
    public void emplaceUpstreamSchema(
            @Nonnull TableId tableId, int sourcePartition, @Nonnull Schema schema) {
        upstreamSchemaTable.put(tableId, sourcePartition, schema);
    }

    @VisibleForTesting
    public void emplaceEvolvedSchema(@Nonnull TableId tableId, @Nonnull Schema schema) {
        schemaManager.emplaceSchema(tableId, schema);
    }
}
