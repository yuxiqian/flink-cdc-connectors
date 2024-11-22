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

package org.apache.flink.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DataChangeEventWithSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** A {@link TypeSerializer} for {@link DataChangeEventWithSchema}. */
public class DataChangeEventWithSchemaSerializer
        extends TypeSerializerSingleton<DataChangeEventWithSchema> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final DataChangeEventWithSchemaSerializer INSTANCE =
            new DataChangeEventWithSchemaSerializer();

    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
    private final DataChangeEventSerializer dataChangeEventSerializer =
            DataChangeEventSerializer.INSTANCE;

    @Override
    public DataChangeEventWithSchema createInstance() {
        return new DataChangeEventWithSchema(
                new Schema.Builder().comment("Whatever").build(),
                DataChangeEvent.deleteEvent(TableId.tableId("unknown"), null));
    }

    @Override
    public void serialize(DataChangeEventWithSchema event, DataOutputView target)
            throws IOException {
        schemaSerializer.serialize(event.getSchema(), target);
        dataChangeEventSerializer.serialize(event.getDataChangeEvent(), target);
    }

    @Override
    public DataChangeEventWithSchema deserialize(DataInputView source) throws IOException {
        return new DataChangeEventWithSchema(
                schemaSerializer.deserialize(source),
                dataChangeEventSerializer.deserialize(source));
    }

    @Override
    public DataChangeEventWithSchema deserialize(
            DataChangeEventWithSchema reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public DataChangeEventWithSchema copy(DataChangeEventWithSchema from) {
        return new DataChangeEventWithSchema(
                schemaSerializer.copy(from.getSchema()),
                dataChangeEventSerializer.copy(from.getDataChangeEvent()));
    }

    @Override
    public DataChangeEventWithSchema copy(
            DataChangeEventWithSchema from, DataChangeEventWithSchema reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<DataChangeEventWithSchema> snapshotConfiguration() {
        return new DataChangeEventWithSchemaSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link DataChangeEventWithSchemaSerializer}. */
    public static final class DataChangeEventWithSchemaSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<DataChangeEventWithSchema> {

        public DataChangeEventWithSchemaSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
