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

package org.apache.flink.cdc.runtime.operators.schema.event;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import java.util.Objects;

/**
 * The request from {@link SchemaOperator} to {@link SchemaRegistry} to request to change schemas.
 */
public class SchemaChangeRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    /** The sender of the request. */
    private final TableId tableId;
    /** The schema changes. */
    private final SchemaChangeEvent schemaChangeEvent;
    /** The ID of subTask that initiated the request. */
    private final int subTaskId;
    /**
     * Nonce code to distinguish flush events corresponding to each schema change event from
     * different subTasks.
     */
    private final long nonce;

    public SchemaChangeRequest(
            TableId tableId, SchemaChangeEvent schemaChangeEvent, int subTaskId, long nonce) {
        this.tableId = tableId;
        this.schemaChangeEvent = schemaChangeEvent;
        this.subTaskId = subTaskId;
        this.nonce = nonce;
    }

    public TableId getTableId() {
        return tableId;
    }

    public SchemaChangeEvent getSchemaChangeEvent() {
        return schemaChangeEvent;
    }

    public int getSubTaskId() {
        return subTaskId;
    }

    public long getNonce() {
        return nonce;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaChangeRequest)) {
            return false;
        }
        SchemaChangeRequest that = (SchemaChangeRequest) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(schemaChangeEvent, that.schemaChangeEvent)
                && subTaskId == that.subTaskId
                && nonce == that.nonce;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schemaChangeEvent, subTaskId, nonce);
    }
}
