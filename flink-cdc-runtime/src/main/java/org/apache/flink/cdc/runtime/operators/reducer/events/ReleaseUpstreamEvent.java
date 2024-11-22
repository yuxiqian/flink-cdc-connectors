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

package org.apache.flink.cdc.runtime.operators.reducer.events;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.reducer.SchemaMapper;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * A {@link OperatorEvent} from schema reducer to notify {@link SchemaMapper} that it could release
 * upstream and update local schemas now.
 */
public class ReleaseUpstreamEvent implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    /** The schema changes to be broadcast. */
    private final TableId tableId;

    private final Schema schema;

    public ReleaseUpstreamEvent(TableId tableId, Schema schema) {
        this.tableId = tableId;
        this.schema = schema;
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReleaseUpstreamEvent)) {
            return false;
        }
        ReleaseUpstreamEvent that = (ReleaseUpstreamEvent) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema);
    }
}
