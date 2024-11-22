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

package org.apache.flink.cdc.connectors.stimps.source;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/** Source function for "STIMPS" testing source. */
public class StimpsSourceFunction extends RichParallelSourceFunction<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(StimpsSourceFunction.class);

    private int subTaskId;

    public static final TableId TABLE_ID =
            TableId.tableId("stimps_namespace", "stimps_database", "stimps_table");

    private static final Schema INITIAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.VARCHAR(17))
                    .primaryKey("id")
                    .partitionKey("id")
                    .build();

    private static BinaryRecordData event(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }

    private static final DataType[] APPENDED_DATATYPES =
            new DataType[] {
                DataTypes.TINYINT(),
                DataTypes.BOOLEAN(),
                DataTypes.VARCHAR(10),
                DataTypes.TIMESTAMP()
            };

    private static final Object[] APPENDED_OBJECTS =
            new Object[] {
                (byte) 17,
                false,
                BinaryStringData.fromString("sigh"),
                TimestampData.fromTimestamp(java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000"))
            };

    private static final int APPENDED_COLUMNS_SIZE = APPENDED_DATATYPES.length;

    private static final String[] NAMES =
            new String[] {
                "Edgeworth", "Ferris", "Gumshoe", "Harry", "IINA", "Juliet", "Kio", "Lilith"
            };

    private static DataChangeEvent insert(BinaryRecordData after) {
        return DataChangeEvent.insertEvent(TABLE_ID, after);
    }

    private static DataChangeEvent update(BinaryRecordData before, BinaryRecordData after) {
        return DataChangeEvent.updateEvent(TABLE_ID, before, after);
    }

    private static DataChangeEvent delete(BinaryRecordData before) {
        return DataChangeEvent.deleteEvent(TABLE_ID, before);
    }

    @Override
    public void run(SourceContext<Event> context) throws InterruptedException {
        {
            // Emits shared CreateTableEvent first
            LOG.info("{}> Emitting CreateTableEvent", subTaskId);
            collect(context, new CreateTableEvent(TABLE_ID, INITIAL_SCHEMA));

            BinaryRecordData event1 =
                    event(INITIAL_SCHEMA, (long) 1000 * subTaskId + 1, "Alice #" + subTaskId);
            BinaryRecordData event2 =
                    event(INITIAL_SCHEMA, (long) 1000 * subTaskId + 1, "Bob #" + subTaskId);
            collect(context, insert(event1));
            collect(context, update(event1, event2));
            collect(context, delete(event2));
        }

        DataType appendedColumnType = APPENDED_DATATYPES[subTaskId % APPENDED_COLUMNS_SIZE];
        Object appendedObject = APPENDED_OBJECTS[subTaskId % APPENDED_COLUMNS_SIZE];
        {
            // Test adding conflicting columns
            Column appendedColumn = Column.physicalColumn("foo", appendedColumnType);
            collect(
                    context,
                    new AddColumnEvent(
                            TABLE_ID,
                            Collections.singletonList(AddColumnEvent.last(appendedColumn))));
            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT())
                            .physicalColumn("name", DataTypes.VARCHAR(17))
                            .physicalColumn("foo", appendedColumnType)
                            .primaryKey("id")
                            .partitionKey("id")
                            .build();

            BinaryRecordData event3 =
                    event(
                            schemaV2,
                            (long) 1000 * subTaskId + 2,
                            "Cecily #" + subTaskId,
                            appendedObject);
            BinaryRecordData event4 =
                    event(
                            schemaV2,
                            (long) 1000 * subTaskId + 2,
                            "Derrida #" + subTaskId,
                            appendedObject);
            collect(context, insert(event3));
            collect(context, update(event3, event4));
            collect(context, delete(event4));
        }

        {
            // Test appending irrelevant columns
            Column irrelevantColumn = Column.physicalColumn("bar_" + subTaskId, DataTypes.STRING());
            collect(
                    context,
                    new AddColumnEvent(
                            TABLE_ID,
                            Collections.singletonList(AddColumnEvent.last(irrelevantColumn))));
            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT())
                            .physicalColumn("name", DataTypes.VARCHAR(17))
                            .physicalColumn("foo", appendedColumnType)
                            .physicalColumn("bar_" + subTaskId, DataTypes.STRING())
                            .primaryKey("id")
                            .partitionKey("id")
                            .build();

            for (int i = 0; i < NAMES.length; i++) {
                BinaryRecordData before =
                        event(
                                schemaV3,
                                (long) 1000 * subTaskId + 3 + i,
                                NAMES[i] + " #" + subTaskId,
                                appendedObject,
                                "Before before");
                BinaryRecordData after =
                        event(
                                schemaV3,
                                (long) 1000 * subTaskId + 3 + i,
                                NAMES[i] + " #" + subTaskId,
                                appendedObject,
                                "After after");
                collect(context, insert(before));
                collect(context, update(before, after));
                collect(context, delete(after));
            }
        }
    }

    @Override
    public void cancel() {}

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    private void collect(SourceContext<Event> sourceContext, Event event) {
        LOG.info("{}> Emitting event {}", subTaskId, event);
        sourceContext.collect(event);
    }
}
