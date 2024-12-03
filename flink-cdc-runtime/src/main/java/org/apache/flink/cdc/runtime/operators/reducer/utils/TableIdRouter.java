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

package org.apache.flink.cdc.runtime.operators.reducer.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.schema.Selectors;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Simply routes upstream data change events to correct downstream tables (could be multiple). In
 * inference mode we don't need to worry about routing schema merging stuff. Just deliver it.
 */
public class TableIdRouter {

    private final List<Tuple3<Selectors, String, String>> routes;

    public TableIdRouter(List<RouteRule> routingRules) {
        this.routes =
                routingRules.stream()
                        .map(
                                rule -> {
                                    String tableInclusions = rule.sourceTable;
                                    Selectors selectors =
                                            new Selectors.SelectorsBuilder()
                                                    .includeTables(tableInclusions)
                                                    .build();
                                    return new Tuple3<>(
                                            selectors, rule.sinkTable, rule.replaceSymbol);
                                })
                        .collect(Collectors.toList());
    }

    public List<TableId> route(TableId sourceTableId) {
        List<TableId> routedTableIds =
                routes.stream()
                        .filter(route -> route.f0.isMatch(sourceTableId))
                        .map(route -> resolveReplacement(sourceTableId, route))
                        .collect(Collectors.toList());
        if (routedTableIds.isEmpty()) {
            routedTableIds.add(sourceTableId);
        }
        return routedTableIds;
    }

    private TableId resolveReplacement(
            TableId originalTable, Tuple3<Selectors, String, String> route) {
        if (route.f2 != null) {
            return TableId.parse(route.f1.replace(route.f2, originalTable.getTableName()));
        }
        return TableId.parse(route.f1);
    }
}
