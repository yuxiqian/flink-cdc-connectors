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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.reducer.utils.TableIdRouter;

import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import java.util.Set;
import java.util.stream.Collectors;

/** A utility class to derive how evolved schemas should change. */
public class SchemaDerivator {

    /** Get affected evolved table IDs based on changed upstream tables. */
    static Set<TableId> getAffectedEvolvedTables(
            final TableIdRouter tableIdRouter, final Set<TableId> changedUpstreamTables) {
        return changedUpstreamTables.stream()
                .flatMap(cut -> tableIdRouter.route(cut).stream())
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream tables that it depends on. */
    static Set<TableId> reverseLookupDependingUpstreamTables(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final Table<TableId, Integer, Schema> upstreamSchemaTable) {
        return upstreamSchemaTable.rowKeySet().stream()
                .filter(kut -> tableIdRouter.route(kut).contains(evolvedTableId))
                .collect(Collectors.toSet());
    }

    /** For an evolved table ID, reverse lookup all upstream schemas that needs to be fit in. */
    static Set<Schema> reverseLookupDependingUpstreamSchemas(
            final TableIdRouter tableIdRouter,
            final TableId evolvedTableId,
            final Table<TableId, Integer, Schema> upstreamSchemaTable) {
        return reverseLookupDependingUpstreamTables(
                        tableIdRouter, evolvedTableId, upstreamSchemaTable)
                .stream()
                .flatMap(utid -> upstreamSchemaTable.row(utid).values().stream())
                .collect(Collectors.toSet());
    }
}
