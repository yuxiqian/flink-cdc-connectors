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

import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/** Coordinator node for {@link SchemaMapper}. */
public class SchemaReducer implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaReducer.class);

    private final OperatorCoordinator.Context context;
    private final String operatorName;
    private final ExecutorService coordinatorExecutor;
    private final MetadataApplier metadataApplier;
    private final List<RouteRule> routes;

    public SchemaReducer(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            List<RouteRule> routes) {
        this.context = context;
        this.coordinatorExecutor = coordinatorExecutor;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.routes = routes;
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
                                "Uncaught exception in the SchemaEvolutionCoordinator for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    // TODO: Handle events from SchemaMapper
                },
                "handling coordination request %s",
                request);
        return responseFuture;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subTaskId, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    try {
                        if (event instanceof FlushSuccessEvent) {
                            FlushSuccessEvent flushSuccessEvent = (FlushSuccessEvent) event;
                            LOG.info(
                                    "Sink subtask {} succeed flushing for table {}.",
                                    flushSuccessEvent.getSubtask(),
                                    flushSuccessEvent.getTableId().toString());
                            // TODO: Receive Flush Success Event
                        } else if (event instanceof SinkWriterRegisterEvent) {
                            // TODO: Register Sink Writers
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
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {}

    @Override
    public void subtaskReset(int subTask, long checkpointId) {}

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable reason) {}

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}
}
