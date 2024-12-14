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

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * An {@link Event} from {@code SchemaOperator} to notify that schema of specified tables have
 * changed, and any downstream operators should re-request them via the SchemaEvolutionClient.
 */
public class FlushSchemaEvent implements Event, Serializable {

    /**
     * The schema changes from which table. If tableId is null, it means this {@code FlushEvent}
     * should flush all pending events, no matter which table it belongs to.
     */
    private final Set<TableId> changedTables;

    public FlushSchemaEvent(Set<TableId> changedTables) {
        this.changedTables = changedTables;
    }

    public Set<TableId> getChangedTables() {
        return changedTables;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlushSchemaEvent that = (FlushSchemaEvent) o;
        return Objects.equals(changedTables, that.changedTables);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(changedTables);
    }

    @Override
    public String toString() {
        return "FlushSchemaEvent{" + "changedTables=" + changedTables + '}';
    }
}
