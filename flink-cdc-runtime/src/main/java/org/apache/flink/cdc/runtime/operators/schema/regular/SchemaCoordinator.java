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

package org.apache.flink.cdc.runtime.operators.schema.regular;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushSchemaEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaDerivator;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.common.event.common.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.regular.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.regular.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.runtime.operators.schema.common.event.common.CoordinationResponseUtils.wrap;

/** Coordinator node for {@link SchemaOperator}. Registry actor in Operator-Coordinator Topology. */
public class SchemaCoordinator extends SchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaCoordinator.class);

    /** Globally configured schema change behavior. */
    private final SchemaChangeBehavior behavior;

    /** Lock used for race controlling during schema change request handling. */
    private final Object schemaChangeRequestLock;

    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    /**
     * Atomic flag indicating if current RequestHandler could accept more schema changes for now.
     */
    private volatile RequestStatus schemaChangeStatus;

    /** Sink writers which have sent flush success events for the request. */
    private volatile ConcurrentHashMap<Long, Set<Integer>> flushedSinkWriters;

    /** Currently handling request's completable future. */
    private volatile CompletableFuture<CoordinationResponse> pendingResponseFuture;

    // Static fields
    public SchemaCoordinator(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routes,
            SchemaChangeBehavior schemaChangeBehavior,
            Duration rpcTimeout) {
        super(context, operatorName, coordinatorExecutor, metadataApplier, routes, rpcTimeout);
        this.behavior = schemaChangeBehavior;
        this.schemaChangeRequestLock = new Object();
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();
    }

    @Override
    public void start() throws Exception {
        super.start();
        this.flushedSinkWriters = new ConcurrentHashMap<>();
        this.schemaChangeStatus = RequestStatus.IDLE;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (schemaChangeThreadPool != null && !schemaChangeThreadPool.isShutdown()) {
            schemaChangeThreadPool.shutdownNow();
        }
    }

    @Override
    protected void snapshot(CompletableFuture<byte[]> resultFuture) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            // Serialize SchemaManager
            int schemaManagerSerializerVersion = SchemaManager.SERIALIZER.getVersion();
            out.writeInt(schemaManagerSerializerVersion);
            byte[] serializedSchemaManager = SchemaManager.SERIALIZER.serialize(schemaManager);
            out.writeInt(serializedSchemaManager.length);
            out.write(serializedSchemaManager);

            // Length-bit for SchemaDerivation, which is no longer necessary.
            out.writeInt(0);
            resultFuture.complete(baos.toByteArray());
        }
    }

    @Override
    protected void restore(byte[] checkpointData) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();

            switch (schemaManagerSerializerVersion) {
                case 0:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        break;
                    }
                case 1:
                case 2:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                SchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        consumeUnusedSchemaDerivationBytes(in);
                        break;
                    }
                default:
                    throw new IOException(
                            "Unrecognized serialization version " + schemaManagerSerializerVersion);
            }
        }
    }

    @Override
    protected void handleCustomCoordinationRequest(
            CoordinationRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        if (request instanceof SchemaChangeRequest) {
            handleSchemaChangeRequest((SchemaChangeRequest) request, responseFuture);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown coordination request type: " + request);
        }
    }

    @Override
    protected void handleFlushSuccessEvent(FlushSuccessEvent event) {
        int sinkSubtask = event.getSubtask();
        long nonce = event.getNonce();
        LOG.info("Sink subtask {} succeed flushing with nonce {}.", sinkSubtask, nonce);
        synchronized (schemaChangeRequestLock) {
            if (!flushedSinkWriters.containsKey(nonce)) {
                flushedSinkWriters.put(nonce, ConcurrentHashMap.newKeySet());
            }
            flushedSinkWriters.get(nonce).add(sinkSubtask);
            LOG.info(
                    "Currently flushed sink writers for nonce {} are: {}",
                    nonce,
                    flushedSinkWriters.get(nonce));
        }
    }

    @Override
    protected void handleUnrecoverableError(String taskDescription, Throwable t) {
        super.handleUnrecoverableError(taskDescription, t);

        // There's a pending future, release it exceptionally before quitting
        if (pendingResponseFuture != null) {
            pendingResponseFuture.completeExceptionally(t);
        }
    }

    /**
     * Handle the {@link SchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received SchemaChangeRequest
     */
    public void handleSchemaChangeRequest(
            SchemaChangeRequest request, CompletableFuture<CoordinationResponse> responseFuture) {

        // We use nonce to identify each schema change request
        long nonce = request.getNonce();

        synchronized (schemaChangeRequestLock) {
            if (schemaChangeStatus == RequestStatus.IDLE) {
                if (activeSinkWriters.size() < currentParallelism) {
                    LOG.info(
                            "Not all active sink writers have been registered. Current {}, expected {}.",
                            activeSinkWriters.size(),
                            currentParallelism);
                    responseFuture.complete(wrap(SchemaChangeResponse.waitingForFlush()));
                    return;
                }

                if (!activeSinkWriters.equals(flushedSinkWriters.get(nonce))) {
                    LOG.info(
                            "Not all active sink writers have completed flush (nonce: {}). Flushed writers: {}, expected: {}.",
                            nonce,
                            flushedSinkWriters.get(nonce),
                            activeSinkWriters);
                    responseFuture.complete(wrap(SchemaChangeResponse.waitingForFlush()));
                    return;
                }

                LOG.info(
                        "All sink writers have flushed for nonce {}. Switching to APPLYING state and starting schema evolution...",
                        nonce);
                flushedSinkWriters.remove(nonce);
                schemaChangeStatus = RequestStatus.APPLYING;
                pendingResponseFuture = responseFuture;
                startSchemaChangesEvolve(request, responseFuture);
            } else {
                responseFuture.complete(wrap(SchemaChangeResponse.busy()));
            }
        }
    }

    private void startSchemaChangesEvolve(
            SchemaChangeRequest request, CompletableFuture<CoordinationResponse> responseFuture) {
        SchemaChangeEvent originalEvent = request.getSchemaChangeEvent();
        TableId originalTableId = originalEvent.tableId();
        Schema currentUpstreamSchema =
                schemaManager.getLatestOriginalSchema(originalTableId).orElse(null);

        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();

        // For redundant schema change events (possibly coming from duplicate emitted
        // CreateTableEvents in snapshot stage), we just skip them.
        if (!SchemaUtils.isSchemaChangeEventRedundant(currentUpstreamSchema, originalEvent)) {
            schemaManager.applyOriginalSchemaChange(originalEvent);
            deducedSchemaChangeEvents.addAll(deduceEvolvedSchemaChanges(originalEvent));
        } else {
            LOG.info(
                    "Schema change event {} is redundant for current schema {}, just skip it.",
                    originalEvent,
                    currentUpstreamSchema);
        }

        LOG.info(
                "All sink subtask have flushed for table {}. Start to apply schema change request: \n\t{}\nthat extracts to:\n\t{}",
                request.getTableId().toString(),
                request,
                deducedSchemaChangeEvents.stream()
                        .map(SchemaChangeEvent::toString)
                        .collect(Collectors.joining("\n\t")));
        schemaChangeThreadPool.submit(
                () -> {
                    try {
                        applySchemaChange(originalEvent, deducedSchemaChangeEvents);
                    } catch (Throwable t) {
                        failJob(
                                "Schema change applying task",
                                new FlinkRuntimeException(
                                        "Failed to apply schema change event.", t));
                        throw t;
                    }
                });
    }

    private List<SchemaChangeEvent> deduceEvolvedSchemaChanges(SchemaChangeEvent event) {
        TableId originalTableId = event.tableId();

        List<SchemaChangeEvent> deducedSchemaChangeEvents = new ArrayList<>();

        Set<TableId> originalTables = schemaManager.getAllOriginalTables();

        // First, grab all affected evolved tables.
        Set<TableId> affectedEvolvedTables =
                SchemaDerivator.getAffectedEvolvedTables(
                        router, Collections.singleton(originalTableId));

        // For each affected table, we need to...
        for (TableId evolvedTableId : affectedEvolvedTables) {
            Schema currentEvolvedSchema =
                    schemaManager.getLatestEvolvedSchema(evolvedTableId).orElse(null);

            // ... reversely look up this affected sink table's upstream dependency
            Set<TableId> upstreamDependencies =
                    SchemaDerivator.reverseLookupDependingUpstreamTables(
                            router, evolvedTableId, originalTables);
            Preconditions.checkArgument(
                    !upstreamDependencies.isEmpty(),
                    "An affected sink table's upstream dependency cannot be empty.");

            List<SchemaChangeEvent> rawSchemaChangeEvents = new ArrayList<>();
            if (upstreamDependencies.size() == 1) {
                // If it's a one-by-one routing rule, we can simply forward it to downstream sink.
                rawSchemaChangeEvents.add(event.copy(evolvedTableId));
            } else {
                Set<Schema> toBeMergedSchemas =
                        SchemaDerivator.reverseLookupDependingUpstreamSchemas(
                                router, evolvedTableId, schemaManager);

                // We're in a table routing mode now, so we need to infer a widest schema for all
                // upstream tables.
                Schema mergedSchema = currentEvolvedSchema;
                for (Schema toBeMergedSchema : toBeMergedSchemas) {
                    mergedSchema =
                            SchemaMergingUtils.getLeastCommonSchema(mergedSchema, toBeMergedSchema);
                }

                // Detect what schema changes we need to apply to get expected sink table.
                rawSchemaChangeEvents.addAll(
                        SchemaMergingUtils.getSchemaDifference(
                                evolvedTableId, currentEvolvedSchema, mergedSchema));
            }

            // Finally, we normalize schema change events, including rewriting events by current
            // schema change behavior configuration, dropping explicitly excluded schema change
            // event types.
            deducedSchemaChangeEvents.addAll(
                    SchemaDerivator.normalizeSchemaChangeEvents(
                            currentEvolvedSchema,
                            rawSchemaChangeEvents,
                            behavior,
                            metadataApplier));
        }

        return deducedSchemaChangeEvents;
    }

    /** Applies the schema change to the external system. */
    private void applySchemaChange(
            SchemaChangeEvent originalEvent, List<SchemaChangeEvent> deducedSchemaChangeEvents) {
        if (SchemaChangeBehavior.EXCEPTION.equals(behavior)) {
            if (deducedSchemaChangeEvents.stream()
                    .anyMatch(evt -> !(evt instanceof CreateTableEvent))) {
                SchemaChangeEvent unacceptableSchemaChangeEvent =
                        deducedSchemaChangeEvents.stream()
                                .filter(evt -> !(evt instanceof CreateTableEvent))
                                .findAny()
                                .get();
                throw new SchemaEvolveException(
                        unacceptableSchemaChangeEvent,
                        "Unexpected schema change events occurred in EXCEPTION mode. Job will fail now.");
            }
        }

        // Tries to apply it to external system
        List<SchemaChangeEvent> appliedSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent event : deducedSchemaChangeEvents) {
            if (applyAndUpdateEvolvedSchemaChange(event)) {
                appliedSchemaChangeEvents.add(event);
            }
        }

        Map<TableId, Schema> refreshedEvolvedSchemas = new HashMap<>();
        Set<TableId> unforwardableTableIds = new HashSet<>();
        // We need to retrieve all possibly modified evolved schemas and refresh SchemaOperator's
        // local cache since it might have been altered by another SchemaOperator instance.
        // SchemaChangeEvents doesn't need to be emitted to downstream (since it might be broadcast
        // from other SchemaOperators) though.
        for (TableId tableId : router.route(originalEvent.tableId())) {
            refreshedEvolvedSchemas.put(
                    tableId, schemaManager.getLatestEvolvedSchema(tableId).orElse(null));
            if (SchemaDerivator.reverseLookupDependingUpstreamTables(
                                    router, tableId, schemaManager.getAllOriginalTables())
                            .size()
                    > 1) {
                unforwardableTableIds.add(tableId);
            }
        }

        // And returns all successfully applied schema change events to SchemaOperator.
        // For regular one-by-one schema change, just emit actual changes to downstream.
        // For table-merging routes, we must let downstream operators to retrieve latest schema from
        // registry.
        // Emitting SchemaChangeEvents directly is not feasible, since events from various Schema
        // Operators might be mixed together after being partitioned, and could cause unpredictable
        // schema change event applying sequence.
        // By requiring downstream operators to retrieve latest evolved schema from centralized
        // registry, we could ensure a globally consistent schema view for all operators.

        List<Event> finalSchemaChanges =
                appliedSchemaChangeEvents.stream()
                        .filter(evt -> !unforwardableTableIds.contains(evt.tableId()))
                        .collect(Collectors.toList());
        if (!unforwardableTableIds.isEmpty()) {
            finalSchemaChanges.add(new FlushSchemaEvent(unforwardableTableIds));
        }

        pendingResponseFuture.complete(
                wrap(SchemaChangeResponse.success(finalSchemaChanges, refreshedEvolvedSchemas)));
        pendingResponseFuture = null;

        Preconditions.checkState(
                schemaChangeStatus == RequestStatus.APPLYING,
                "Illegal schemaChangeStatus state: should be APPLYING before applySchemaChange finishes, not "
                        + schemaChangeStatus);
        schemaChangeStatus = RequestStatus.IDLE;
        LOG.info("SchemaChangeStatus switched from APPLYING to IDLE.");
    }

    private boolean applyAndUpdateEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            metadataApplier.applySchemaChange(schemaChangeEvent);
            schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
            LOG.info(
                    "Successfully applied schema change event {} to external system.",
                    schemaChangeEvent);
            return true;
        } catch (Throwable t) {
            if (shouldIgnoreException(t)) {
                LOG.warn(
                        "Failed to apply schema change {}, but keeps running in tolerant mode. Caused by: {}",
                        schemaChangeEvent,
                        t);
                return false;
            } else {
                throw t;
            }
        }
    }

    // -------------------------
    // Utilities
    // -------------------------

    private boolean shouldIgnoreException(Throwable throwable) {
        // In IGNORE mode, will never try to apply schema change events
        // In EVOLVE and LENIENT mode, such failure will not be tolerated
        // In EXCEPTION mode, an exception will be thrown once captured
        return (throwable instanceof UnsupportedSchemaChangeEventException)
                && (SchemaChangeBehavior.TRY_EVOLVE.equals(behavior));
    }

    /**
     * {@code IDLE}: Initial idling state, ready for requests. <br>
     * {@code APPLYING}: When schema change application finishes (successfully or with exceptions)
     */
    private enum RequestStatus {
        IDLE,
        APPLYING
    }

    /**
     * Before Flink CDC 3.3, we store routing rules into {@link SchemaCoordinator}'s state, which
     * turns out to be unnecessary since data stream topology might change after stateful restarts,
     * and stale routing status is both unnecessary and erroneous. This function consumes these
     * bytes from the state, but never returns them.
     */
    private void consumeUnusedSchemaDerivationBytes(DataInputStream in) throws IOException {
        TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        int derivationMappingSize = in.readInt();
        Map<TableId, Set<TableId>> derivationMapping = new HashMap<>(derivationMappingSize);
        for (int i = 0; i < derivationMappingSize; i++) {
            // Routed table ID
            TableId routedTableId =
                    tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
            // Original table IDs
            int numOriginalTables = in.readInt();
            Set<TableId> originalTableIds = new HashSet<>(numOriginalTables);
            for (int j = 0; j < numOriginalTables; j++) {
                TableId originalTableId =
                        tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                originalTableIds.add(originalTableId);
            }
            derivationMapping.put(routedTableId, originalTableIds);
        }
    }
}
