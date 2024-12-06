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
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.testutils.operators.EventOperatorTestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;

/** Unit test cases for {@link SchemaMapper}. */
public class SchemaMapperTest extends ReducerTestBase {
    private static final TableId TABLE_ID = TableId.parse("foo.bar.baz");
    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.VARCHAR(128))
                    .physicalColumn("age", DataTypes.FLOAT())
                    .physicalColumn("notes", DataTypes.STRING().notNull())
                    .build();

    @Test
    void testEvolvedSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        AlterColumnTypeEvent alterColumnTypeEventWithBackfill =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("age", DataTypes.DOUBLE()),
                        Collections.singletonMap("age", DataTypes.FLOAT()));
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaMapper(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                "UTC",
                                                SchemaChangeBehavior.EVOLVE),
                                (op) ->
                                        new EventOperatorTestHarness<>(
                                                op, 20, SchemaChangeBehavior.EVOLVE),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.getInstance(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.getInstance(),
                        addColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 2, false, "Bob", 31.415926f, "Bye-bye"),
                        FlushEvent.getInstance(),
                        renameColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 3, true, "Cicada", 123.456f, "Ok"),
                        FlushEvent.getInstance(),
                        alterColumnTypeEventWithBackfill,
                        genInsert(TABLE_ID, "IBSDS", 4, false, "Derrida", 7.81876754837, "Nah"),
                        FlushEvent.getInstance(),
                        dropColumnEvent,
                        genInsert(TABLE_ID, "IBSD", 5, true, "Eve", 1.414),
                        FlushEvent.getInstance(),
                        truncateTableEvent,
                        genInsert(TABLE_ID, "IBSD", 6, false, "Ferris", 0.001),
                        FlushEvent.getInstance(),
                        dropTableEvent);
    }

    @Test
    void testTryEvolvedSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        AlterColumnTypeEvent alterColumnTypeEventWithBackfill =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("age", DataTypes.DOUBLE()),
                        Collections.singletonMap("age", DataTypes.FLOAT()));
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaMapper(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                "UTC",
                                                SchemaChangeBehavior.TRY_EVOLVE),
                                (op) ->
                                        new EventOperatorTestHarness<>(
                                                op, 20, SchemaChangeBehavior.TRY_EVOLVE),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.getInstance(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.getInstance(),
                        addColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 2, false, "Bob", 31.415926f, "Bye-bye"),
                        FlushEvent.getInstance(),
                        renameColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 3, true, "Cicada", 123.456f, "Ok"),
                        FlushEvent.getInstance(),
                        alterColumnTypeEventWithBackfill,
                        genInsert(TABLE_ID, "IBSDS", 4, false, "Derrida", 7.81876754837, "Nah"),
                        FlushEvent.getInstance(),
                        dropColumnEvent,
                        genInsert(TABLE_ID, "IBSD", 5, true, "Eve", 1.414),
                        FlushEvent.getInstance(),
                        truncateTableEvent,
                        genInsert(TABLE_ID, "IBSD", 6, false, "Ferris", 0.001),
                        FlushEvent.getInstance(),
                        dropTableEvent);
    }

    @Test
    void testTryEvolvedWithPartialSuccessSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaMapper(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                "UTC",
                                                SchemaChangeBehavior.TRY_EVOLVE),
                                (op) ->
                                        new EventOperatorTestHarness<>(
                                                op,
                                                20,
                                                Duration.ofSeconds(3),
                                                SchemaChangeBehavior.TRY_EVOLVE,
                                                ImmutableSet.of(
                                                        CREATE_TABLE,
                                                        ADD_COLUMN,
                                                        RENAME_COLUMN,
                                                        ALTER_COLUMN_TYPE,
                                                        DROP_COLUMN),
                                                ImmutableSet.of(ALTER_COLUMN_TYPE, DROP_COLUMN)),
                                (operator, harness) -> {
                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.getInstance(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.getInstance(),
                        addColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 2, false, "Bob", 31.415926f, "Bye-bye"),
                        FlushEvent.getInstance(),
                        renameColumnEvent,
                        genInsert(TABLE_ID, "IBSFS", 3, true, "Cicada", 123.456f, "Ok"),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "IBSFS", 4, false, "Derrida", null, "Nah"),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "IBSFS", 5, true, "Eve", null, null),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "IBSFS", 6, false, "Ferris", null, null),
                        FlushEvent.getInstance());
    }

    @Test
    void testLenientSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));
        AddColumnEvent addColumnEventAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));

        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));
        AddColumnEvent appendRenamedColumnAtLast =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("footnotes", DataTypes.STRING()),
                                        AddColumnEvent.ColumnPosition.LAST,
                                        null)));
        AlterColumnTypeEvent alterNonNullColumnsToNullable =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("notes", DataTypes.STRING()),
                        Collections.singletonMap("notes", DataTypes.STRING().notNull()));

        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));
        AlterColumnTypeEvent alterColumnTypeEventWithBackfill =
                new AlterColumnTypeEvent(
                        TABLE_ID,
                        Collections.singletonMap("age", DataTypes.DOUBLE()),
                        Collections.singletonMap("age", DataTypes.FLOAT()));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaMapper(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                "UTC",
                                                SchemaChangeBehavior.LENIENT),
                                (op) ->
                                        new EventOperatorTestHarness<>(
                                                op, 20, SchemaChangeBehavior.LENIENT),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.getInstance(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.getInstance(),
                        addColumnEventAtLast,
                        genInsert(TABLE_ID, "ISFSB", 2, "Bob", 31.415926f, "Bye-bye", false),
                        FlushEvent.getInstance(),
                        appendRenamedColumnAtLast,
                        alterNonNullColumnsToNullable,
                        genInsert(TABLE_ID, "ISFSBS", 3, "Cicada", 123.456f, null, true, "Ok"),
                        FlushEvent.getInstance(),
                        alterColumnTypeEventWithBackfill,
                        genInsert(
                                TABLE_ID,
                                "ISDSBS",
                                4,
                                "Derrida",
                                7.81876754837,
                                null,
                                false,
                                "Nah"),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISDSBS", 5, "Eve", 1.414, null, true, null),
                        FlushEvent.getInstance(),
                        truncateTableEvent,
                        genInsert(TABLE_ID, "ISDSBS", 6, "Ferris", 0.001, null, false, null),
                        FlushEvent.getInstance(),
                        dropTableEvent);
    }

    @Test
    void testIgnoreSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));

        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));

        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(
                        TABLE_ID, Collections.singletonMap("age", DataTypes.DOUBLE()));

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(TABLE_ID, Collections.singletonList("footnotes"));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(TABLE_ID);
        DropTableEvent dropTableEvent = new DropTableEvent(TABLE_ID);

        Assertions.assertThat(
                        runInHarness(
                                () ->
                                        new SchemaMapper(
                                                ROUTING_RULES,
                                                Duration.ofMinutes(3),
                                                "UTC",
                                                SchemaChangeBehavior.IGNORE),
                                (op) ->
                                        new EventOperatorTestHarness<>(
                                                op, 20, SchemaChangeBehavior.IGNORE),
                                (operator, harness) -> {

                                    // Create a Table
                                    operator.processElement(wrap(createTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "ISFS", 1, "Alice", 17.1828f,
                                                            "Hello")));

                                    // Add a Column
                                    operator.processElement(wrap(addColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSFS",
                                                            2,
                                                            false,
                                                            "Bob",
                                                            31.415926f,
                                                            "Bye-bye")));

                                    // Rename a Column
                                    operator.processElement(wrap(renameColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSFS", 3, true, "Cicada",
                                                            123.456f, "Ok")));

                                    // Alter a Column's Type
                                    operator.processElement(wrap(alterColumnTypeEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID,
                                                            "IBSDS",
                                                            4,
                                                            false,
                                                            "Derrida",
                                                            7.81876754837,
                                                            "Nah")));

                                    // Drop a column
                                    operator.processElement(wrap(dropColumnEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 5, true, "Eve",
                                                            1.414)));

                                    // Truncate a table
                                    operator.processElement(wrap(truncateTableEvent));
                                    operator.processElement(
                                            wrap(
                                                    genInsert(
                                                            TABLE_ID, "IBSD", 6, false, "Ferris",
                                                            0.001)));

                                    // Drop a table
                                    operator.processElement(wrap(dropTableEvent));
                                }))
                .map(StreamRecord::getValue)
                .containsExactly(
                        FlushEvent.getInstance(),
                        createTableEvent,
                        genInsert(TABLE_ID, "ISFS", 1, "Alice", 17.1828f, "Hello"),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISFS", 2, "Bob", 31.415926f, "Bye-bye"),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISFS", 3, "Cicada", 123.456f, null),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISFS", 4, "Derrida", null, null),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISFS", 5, "Eve", null, null),
                        FlushEvent.getInstance(),
                        genInsert(TABLE_ID, "ISFS", 6, "Ferris", null, null),
                        FlushEvent.getInstance());
    }

    @Test
    void testExceptionSchemaEvolution() throws Exception {
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA);
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("added_flag", DataTypes.BOOLEAN()),
                                        AddColumnEvent.ColumnPosition.AFTER,
                                        "id")));

        RenameColumnEvent renameColumnEvent =
                new RenameColumnEvent(TABLE_ID, Collections.singletonMap("notes", "footnotes"));

        Assertions.assertThatThrownBy(
                        () ->
                                ReducerTestBase.runInHarness(
                                        () ->
                                                new SchemaMapper(
                                                        ROUTING_RULES,
                                                        Duration.ofMinutes(3),
                                                        "UTC",
                                                        SchemaChangeBehavior.EXCEPTION),
                                        (op) ->
                                                new EventOperatorTestHarness<>(
                                                        op, 20, SchemaChangeBehavior.EXCEPTION),
                                        (operator, harness) -> {

                                            // Create a Table
                                            operator.processElement(wrap(createTableEvent));
                                            operator.processElement(
                                                    wrap(
                                                            genInsert(
                                                                    TABLE_ID, "ISFS", 1, "Alice",
                                                                    17.1828f, "Hello")));

                                            // Add a Column
                                            operator.processElement(wrap(addColumnEvent));
                                            operator.processElement(
                                                    wrap(
                                                            genInsert(
                                                                    TABLE_ID,
                                                                    "IBSFS",
                                                                    2,
                                                                    false,
                                                                    "Bob",
                                                                    31.415926f,
                                                                    "Bye-bye")));

                                            // Rename a Column
                                            operator.processElement(wrap(renameColumnEvent));
                                            operator.processElement(
                                                    wrap(
                                                            genInsert(
                                                                    TABLE_ID, "IBSFS", 3, true,
                                                                    "Cicada", 123.456f, "Ok")));
                                        }))
                .isExactlyInstanceOf(IllegalStateException.class)
                .cause()
                .cause()
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "An unexpected schema change event AddColumnEvent{tableId=foo.bar.baz, addedColumns=[ColumnWithPosition{column=`added_flag` BOOLEAN, position=AFTER, existedColumnName=id}]} presents in EXCEPTION mode. Job will fail now.");
    }
}
