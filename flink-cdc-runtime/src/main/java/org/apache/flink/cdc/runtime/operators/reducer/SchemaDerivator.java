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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventWithPreSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A utility class to derive how evolved schemas should change. */
public class SchemaDerivator {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaDerivator.class);

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

    /**
     * Rewrite {@link SchemaChangeEvent}s by current {@link SchemaChangeBehavior} and include /
     * exclude them by fine-grained schema change event configurations.
     */
    static List<SchemaChangeEvent> normalizeSchemaChangeEvents(
            Schema oldSchema,
            List<SchemaChangeEvent> schemaChangeEvents,
            SchemaChangeBehavior schemaChangeBehavior,
            MetadataApplier metadataApplier) {
        List<SchemaChangeEvent> rewrittenSchemaChangeEvents =
                rewriteSchemaChangeEvents(oldSchema, schemaChangeEvents, schemaChangeBehavior);
        rewrittenSchemaChangeEvents.forEach(
                evt -> {
                    if (evt instanceof SchemaChangeEventWithPreSchema) {
                        ((SchemaChangeEventWithPreSchema) evt).fillPreSchema(oldSchema);
                    }
                });

        List<SchemaChangeEvent> finalSchemaChangeEvents = new ArrayList<>();
        for (SchemaChangeEvent schemaChangeEvent : rewrittenSchemaChangeEvents) {
            if (metadataApplier.acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
                finalSchemaChangeEvents.add(schemaChangeEvent);
            } else {
                LOG.info("Ignored schema change {}.", schemaChangeEvent);
            }
        }
        return finalSchemaChangeEvents;
    }

    private static List<SchemaChangeEvent> rewriteSchemaChangeEvents(
            Schema oldSchema,
            List<SchemaChangeEvent> schemaChangeEvents,
            SchemaChangeBehavior schemaChangeBehavior) {
        switch (schemaChangeBehavior) {
            case EVOLVE:
            case TRY_EVOLVE:
                return schemaChangeEvents;
            case LENIENT:
                return schemaChangeEvents.stream()
                        .flatMap(evt -> lenientizeSchemaChangeEvent(oldSchema, evt))
                        .collect(Collectors.toList());
            case EXCEPTION:
                schemaChangeEvents.stream()
                        .filter(e -> !(e instanceof CreateTableEvent))
                        .findFirst()
                        .ifPresent(
                                e -> {
                                    throw new IllegalStateException(
                                            "An unexpected schema change event "
                                                    + e
                                                    + " presents in EXCEPTION mode. Job will fail now.");
                                });
                return schemaChangeEvents;
            case IGNORE:
                return schemaChangeEvents.stream()
                        .filter(e -> e instanceof CreateTableEvent)
                        .collect(Collectors.toList());
            default:
                throw new IllegalArgumentException(
                        "Unexpected schema change behavior: " + schemaChangeBehavior);
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeSchemaChangeEvent(
            Schema oldSchema, SchemaChangeEvent schemaChangeEvent) {
        TableId tableId = schemaChangeEvent.tableId();
        switch (schemaChangeEvent.getType()) {
            case ADD_COLUMN:
                return lenientizeAddColumnEvent((AddColumnEvent) schemaChangeEvent, tableId);
            case DROP_COLUMN:
                return lenientizeDropColumnEvent(
                        oldSchema, (DropColumnEvent) schemaChangeEvent, tableId);
            case RENAME_COLUMN:
                return lenientizeRenameColumnEvent(
                        oldSchema, (RenameColumnEvent) schemaChangeEvent, tableId);
            default:
                return Stream.of(schemaChangeEvent);
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeRenameColumnEvent(
            Schema oldSchema, RenameColumnEvent schemaChangeEvent, TableId tableId) {
        List<AddColumnEvent.ColumnWithPosition> appendColumns = new ArrayList<>();
        Map<String, DataType> convertNullableColumns = new HashMap<>();
        schemaChangeEvent
                .getNameMapping()
                .forEach(
                        (oldColName, newColName) -> {
                            Column column =
                                    oldSchema
                                            .getColumn(oldColName)
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    "Non-existed column "
                                                                            + oldColName
                                                                            + " in evolved schema."));
                            if (!column.getType().isNullable()) {
                                // It's a not-nullable column, we need to cast it to
                                // nullable first
                                convertNullableColumns.put(oldColName, column.getType().nullable());
                            }
                            appendColumns.add(
                                    new AddColumnEvent.ColumnWithPosition(
                                            column.copy(newColName)
                                                    .copy(column.getType().nullable())));
                        });

        List<SchemaChangeEvent> events = new ArrayList<>();
        events.add(new AddColumnEvent(tableId, appendColumns));
        if (!convertNullableColumns.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, convertNullableColumns));
        }
        return events.stream();
    }

    private static Stream<SchemaChangeEvent> lenientizeDropColumnEvent(
            Schema oldSchema, DropColumnEvent schemaChangeEvent, TableId tableId) {
        Map<String, DataType> convertNullableColumns =
                schemaChangeEvent.getDroppedColumnNames().stream()
                        .map(oldSchema::getColumn)
                        .flatMap(e -> e.map(Stream::of).orElse(Stream.empty()))
                        .filter(col -> !col.getType().isNullable())
                        .collect(
                                Collectors.toMap(
                                        Column::getName, column -> column.getType().nullable()));

        if (convertNullableColumns.isEmpty()) {
            return Stream.empty();
        } else {
            return Stream.of(new AlterColumnTypeEvent(tableId, convertNullableColumns));
        }
    }

    private static Stream<SchemaChangeEvent> lenientizeAddColumnEvent(
            AddColumnEvent schemaChangeEvent, TableId tableId) {
        return Stream.of(
                new AddColumnEvent(
                        tableId,
                        schemaChangeEvent.getAddedColumns().stream()
                                .map(
                                        col ->
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                col.getAddColumn().getName(),
                                                                col.getAddColumn()
                                                                        .getType()
                                                                        .nullable(),
                                                                col.getAddColumn().getComment(),
                                                                col.getAddColumn()
                                                                        .getDefaultValueExpression())))
                                .collect(Collectors.toList())));
    }
}
