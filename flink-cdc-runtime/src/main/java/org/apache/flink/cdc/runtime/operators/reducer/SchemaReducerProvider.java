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

import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistryProvider;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Provider for {@link SchemaReducer}. */
public class SchemaReducerProvider implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;

    private final OperatorID operatorID;
    private final String operatorName;
    private final MetadataApplier metadataApplier;

    public SchemaReducerProvider(
            OperatorID operatorID, String operatorName, MetadataApplier metadataApplier) {
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        SchemaRegistryProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new SchemaRegistryProvider.CoordinatorExecutorThreadFactory(
                        "schema-evolution-coordinator", context.getUserCodeClassloader());
        ExecutorService coordinatorExecutor =
                Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        return new SchemaReducer(operatorName, context, coordinatorExecutor, metadataApplier);
    }
}
