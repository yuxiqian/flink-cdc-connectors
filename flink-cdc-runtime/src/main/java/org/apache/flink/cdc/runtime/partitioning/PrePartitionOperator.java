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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEventWithSchema;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Operator for processing events from {@link SchemaOperator} before {@link EventPartitioner}. */
@Internal
public class PrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Duration CACHE_EXPIRE_DURATION = Duration.ofDays(1);

    private final OperatorID schemaOperatorId;
    private final int downstreamParallelism;
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;
    private final boolean needsSchemaInferencing;

    private transient SchemaEvolutionClient schemaEvolutionClient;

    // Schema and HashFunctionMap used in schema inferencing mode.
    private transient Map<TableId, Schema> schemaMap;
    private transient Map<TableId, HashFunction<DataChangeEvent>> hashFunctionMap;

    // Reloadable cache used in non-schema inferencing mode.
    private transient LoadingCache<TableId, HashFunction<DataChangeEvent>> cachedHashFunctions;

    @VisibleForTesting
    public PrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider) {
        this(schemaOperatorId, downstreamParallelism, hashFunctionProvider, false);
    }

    public PrePartitionOperator(
            OperatorID schemaOperatorId,
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider,
            boolean needsSchemaInferencing) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.schemaOperatorId = schemaOperatorId;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
        this.needsSchemaInferencing = needsSchemaInferencing;
    }

    @Override
    public void open() throws Exception {
        super.open();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);

        if (needsSchemaInferencing) {
            schemaMap = new HashMap<>();
            hashFunctionMap = new HashMap<>();
        } else {
            cachedHashFunctions = createCache();
        }
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            if (needsSchemaInferencing) {
                // Update local cached schema map
                schemaMap.compute(
                        tableId,
                        (tId, oldSchema) ->
                                SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));

                hashFunctionMap.put(tableId, recreateHashFunction(tableId));
            } else {
                // Update hash function
                cachedHashFunctions.put(tableId, recreateHashFunction(tableId));

                // Broadcast SchemaChangeEvent
                broadcastEvent(event);
            }
        } else if (event instanceof FlushEvent) {
            Preconditions.checkArgument(
                    !needsSchemaInferencing,
                    "No FlushEvent should be passed through PrePartitionOperator.");
            // Broadcast FlushEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            if (needsSchemaInferencing) {
                partitionByAndWrap(
                        new DataChangeEventWithSchema(
                                schemaMap.get(dataChangeEvent.tableId()), dataChangeEvent));
            } else {
                // Partition DataChangeEvent by table ID and primary keys
                partitionBy(dataChangeEvent);
            }
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) throws Exception {
        output.collect(
                new StreamRecord<>(
                        new PartitioningEvent(
                                dataChangeEvent,
                                cachedHashFunctions
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void partitionByAndWrap(DataChangeEventWithSchema event) {
        output.collect(
                new StreamRecord<>(
                        new PartitioningEvent(
                                event,
                                hashFunctionMap
                                                .get(event.tableId())
                                                .hashcode(event.getDataChangeEvent())
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(new StreamRecord<>(new PartitioningEvent(copiedEvent, i)));
        }
    }

    private Schema getLatestSchema(TableId tableId) throws Exception {
        Schema schema;
        if (needsSchemaInferencing) {
            schema = schemaMap.get(tableId);
        } else {
            schema =
                    schemaEvolutionClient
                            .getLatestEvolvedSchema(tableId)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Failed to request latest schema for table "
                                                            + tableId));
        }
        return Objects.requireNonNull(schema);
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) throws Exception {
        return hashFunctionProvider.getHashFunction(tableId, getLatestSchema(tableId));
    }

    private LoadingCache<TableId, HashFunction<DataChangeEvent>> createCache() {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(CACHE_EXPIRE_DURATION)
                .build(
                        new CacheLoader<TableId, HashFunction<DataChangeEvent>>() {
                            @Override
                            public @Nonnull HashFunction<DataChangeEvent> load(@Nonnull TableId key)
                                    throws Exception {
                                return recreateHashFunction(key);
                            }
                        });
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
