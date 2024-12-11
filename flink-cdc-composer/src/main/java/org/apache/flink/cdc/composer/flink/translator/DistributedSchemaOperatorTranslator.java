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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.runtime.operators.schema.distributed.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.distributed.SchemaOperatorFactory;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Translator used to build {@link SchemaOperator} for schema event process. */
public class DistributedSchemaOperatorTranslator {
    private final String schemaOperatorUid;
    private final Duration rpcTimeOut;
    private final String timezone;

    public DistributedSchemaOperatorTranslator(
            String schemaOperatorUid, Duration rpcTimeOut, String timezone) {
        this.schemaOperatorUid = schemaOperatorUid;
        this.rpcTimeOut = rpcTimeOut;
        this.timezone = timezone;
    }

    public DataStream<Event> translate(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes) {
        return addSchemaOperator(input, parallelism, metadataApplier, routes, timezone);
    }

    private DataStream<Event> addSchemaOperator(
            DataStream<PartitioningEvent> input,
            int parallelism,
            MetadataApplier metadataApplier,
            List<RouteDef> routes,
            String timezone) {
        List<RouteRule> routingRules = new ArrayList<>();
        for (RouteDef route : routes) {
            routingRules.add(
                    new RouteRule(
                            route.getSourceTable(),
                            route.getSinkTable(),
                            route.getReplaceSymbol().orElse(null)));
        }
        return input.transform(
                        "SchemaMapper",
                        new EventTypeInfo(),
                        new SchemaOperatorFactory(
                                metadataApplier, routingRules, rpcTimeOut, timezone))
                .uid(schemaOperatorUid)
                .setParallelism(parallelism);
    }
}
