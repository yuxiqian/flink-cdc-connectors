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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that forcefully updates a table's schema cache to keep schema in
 * sync, not considering what its previous schema is. This is merely used for weak structured type
 * inferencing scenario and should not be passed to a {@link MetadataApplier}.
 */
@PublicEvolving
public class EmplaceTableSchemaEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final Schema schema;

    public EmplaceTableSchemaEvent(TableId tableId, Schema schema) {
        this.tableId = tableId;
        this.schema = schema;
    }

    /** Returns the table schema. */
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmplaceTableSchemaEvent)) {
            return false;
        }
        EmplaceTableSchemaEvent that = (EmplaceTableSchemaEvent) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema);
    }

    @Override
    public String toString() {
        return "EmplaceTableSchemaEvent{" + "tableId=" + tableId + ", schema=" + schema + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.CREATE_TABLE;
    }
}
