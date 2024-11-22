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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.time.Duration;

/** Factory to create {@link SchemaMapper}. */
public class SchemaMapperFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {
    private static final long serialVersionUID = 1L;

    private final MetadataApplier metadataApplier;

    public SchemaMapperFactory(
            MetadataApplier metadataApplier, Duration rpcTimeOut, String timezone) {
        super(new SchemaMapper(rpcTimeOut, timezone));
        this.metadataApplier = metadataApplier;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Event>> T createStreamOperator(
            StreamOperatorParameters<Event> parameters) {
        SchemaMapper mapper = super.createStreamOperator(parameters);
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
        eventDispatcher.registerEventHandler(operatorId, mapper);
        return (T) mapper;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SchemaReducerProvider(operatorID, operatorName, metadataApplier);
    }
}
