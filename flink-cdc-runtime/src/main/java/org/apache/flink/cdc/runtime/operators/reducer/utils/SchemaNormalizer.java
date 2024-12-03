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
import org.apache.flink.cdc.common.types.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rewrites schema change events based on current schema change behavior and backfills pre-schema
 * info.
 */
public class SchemaNormalizer {

    public static List<SchemaChangeEvent> normalizeSchemaChangeEvents(
            Schema oldSchema,
            List<SchemaChangeEvent> schemaChangeEvents,
            SchemaChangeBehavior schemaChangeBehavior) {
        List<SchemaChangeEvent> rewrittenSchemaChangeEvents =
                rewriteSchemaChangeEvents(oldSchema, schemaChangeEvents, schemaChangeBehavior);
        rewrittenSchemaChangeEvents.forEach(
                evt -> {
                    if (evt instanceof SchemaChangeEventWithPreSchema) {
                        ((SchemaChangeEventWithPreSchema) evt).fillPreSchema(oldSchema);
                    }
                });
        return rewrittenSchemaChangeEvents;
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
                // fallthrough
            case IGNORE:
                return schemaChangeEvents.stream()
                        .filter(e -> e instanceof CreateTableEvent)
                        .collect(Collectors.toList());
            default:
                throw new IllegalArgumentException(
                        "Unexpected schema change behavior: " + schemaChangeBehavior);
        }
    }

    public static Stream<SchemaChangeEvent> lenientizeSchemaChangeEvent(
            Schema oldSchema, SchemaChangeEvent schemaChangeEvent) {
        TableId tableId = schemaChangeEvent.tableId();
        switch (schemaChangeEvent.getType()) {
            case ADD_COLUMN:
                {
                    AddColumnEvent addColumnEvent = (AddColumnEvent) schemaChangeEvent;
                    return Stream.of(
                            new AddColumnEvent(
                                    tableId,
                                    addColumnEvent.getAddedColumns().stream()
                                            .map(
                                                    col ->
                                                            new AddColumnEvent.ColumnWithPosition(
                                                                    Column.physicalColumn(
                                                                            col.getAddColumn()
                                                                                    .getName(),
                                                                            col.getAddColumn()
                                                                                    .getType()
                                                                                    .nullable(),
                                                                            col.getAddColumn()
                                                                                    .getComment(),
                                                                            col.getAddColumn()
                                                                                    .getDefaultValueExpression())))
                                            .collect(Collectors.toList())));
                }
            case DROP_COLUMN:
                {
                    DropColumnEvent dropColumnEvent = (DropColumnEvent) schemaChangeEvent;
                    Map<String, DataType> convertNullableColumns =
                            dropColumnEvent.getDroppedColumnNames().stream()
                                    .map(oldSchema::getColumn)
                                    .flatMap(e -> e.map(Stream::of).orElse(Stream.empty()))
                                    .filter(col -> !col.getType().isNullable())
                                    .collect(
                                            Collectors.toMap(
                                                    Column::getName,
                                                    column -> column.getType().nullable()));

                    if (convertNullableColumns.isEmpty()) {
                        return Stream.empty();
                    } else {
                        return Stream.of(new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                }
            case RENAME_COLUMN:
                {
                    RenameColumnEvent renameColumnEvent = (RenameColumnEvent) schemaChangeEvent;
                    List<AddColumnEvent.ColumnWithPosition> appendColumns = new ArrayList<>();
                    Map<String, DataType> convertNullableColumns = new HashMap<>();
                    renameColumnEvent
                            .getNameMapping()
                            .forEach(
                                    (key, value) -> {
                                        Column column =
                                                oldSchema
                                                        .getColumn(key)
                                                        .orElseThrow(
                                                                () ->
                                                                        new IllegalArgumentException(
                                                                                "Non-existed column "
                                                                                        + key
                                                                                        + " in evolved schema."));
                                        if (!column.getType().isNullable()) {
                                            // It's a not-nullable column, we need to cast it to
                                            // nullable first
                                            convertNullableColumns.put(
                                                    key, column.getType().nullable());
                                        }
                                        appendColumns.add(
                                                new AddColumnEvent.ColumnWithPosition(
                                                        Column.physicalColumn(
                                                                value,
                                                                column.getType().nullable(),
                                                                column.getComment(),
                                                                column
                                                                        .getDefaultValueExpression())));
                                    });

                    List<SchemaChangeEvent> events = new ArrayList<>();
                    events.add(new AddColumnEvent(tableId, appendColumns));
                    if (!convertNullableColumns.isEmpty()) {
                        events.add(new AlterColumnTypeEvent(tableId, convertNullableColumns));
                    }
                    return events.stream();
                }
            default:
                return Stream.of(schemaChangeEvent);
        }
    }
}
