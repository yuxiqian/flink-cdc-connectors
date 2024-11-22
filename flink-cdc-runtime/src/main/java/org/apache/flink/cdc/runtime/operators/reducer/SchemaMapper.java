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

import org.apache.flink.cdc.common.event.DataChangeEventWithSchema;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.reducer.events.ReleaseUpstreamEvent;
import org.apache.flink.cdc.runtime.operators.reducer.events.RequestEmitFlushEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** This operator merges upstream inferred schema into a centralized Schema Registry. */
public class SchemaMapper extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, OperatorEventHandler, Serializable {

    // Final fields that are set upon construction
    private final List<RouteRule> routingRules;
    private final Duration rpcTimeout;
    private final String timezone;

    public SchemaMapper(List<RouteRule> routingRules, Duration rpcTimeOut, String timezone) {
        this.routingRules = routingRules;
        this.rpcTimeout = rpcTimeOut;
        this.timezone = timezone;
    }

    // Volatile fields that are set when operator is running
    private volatile Map<TableId, Schema> schemaCacheMap;
    private volatile AtomicBoolean shouldBlockUpstream;
    private volatile AtomicBoolean alreadyEmitsFlushEvent;

    @Override
    public void open() throws Exception {
        super.open();
        schemaCacheMap = new HashMap<>();
        shouldBlockUpstream = new AtomicBoolean(false);
        alreadyEmitsFlushEvent = new AtomicBoolean(false);
    }

    @Override
    public void processElement(StreamRecord<Event> streamRecord) throws Exception {
        while (shouldBlockUpstream.get()) {
            Thread.sleep(100L);
        }
        Event event = streamRecord.getValue();
        if (!(event instanceof DataChangeEventWithSchema)) {
            throw new IllegalArgumentException(
                    "Schema mapper received unexpected stream record. Expected DataChangeEventWithSchema, actual: "
                            + event);
        }
        DataChangeEventWithSchema eventWithSchema = (DataChangeEventWithSchema) event;

        // 1. Check if schema is compatible
        while (!isSchemaCompatible(null)) {}

        // 2. Migrate to
        output.collect(streamRecord);

        if (shouldBlockUpstream.get()) {
            if (alreadyEmitsFlushEvent.compareAndSet(false, true)) {
                output.collect(new StreamRecord<>(new FlushEvent(null)));
            }
        }
    }

    private boolean isSchemaCompatible(Schema comingSchema) {
        return true;
    }

    private static void routeEvent() {}

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof RequestEmitFlushEvent) {
            RequestEmitFlushEvent requestEmitFlushEvent = (RequestEmitFlushEvent) event;
            shouldBlockUpstream.set(true);
            if (alreadyEmitsFlushEvent.compareAndSet(false, true)) {
                output.collect(
                        new StreamRecord<>(new FlushEvent((requestEmitFlushEvent.getTableId()))));
            }
        } else if (event instanceof ReleaseUpstreamEvent) {
            ReleaseUpstreamEvent releaseUpstreamEvent = (ReleaseUpstreamEvent) event;
            schemaCacheMap.put(releaseUpstreamEvent.getTableId(), releaseUpstreamEvent.getSchema());
            alreadyEmitsFlushEvent.set(false);
            shouldBlockUpstream.set(false);
        }
    }
}
