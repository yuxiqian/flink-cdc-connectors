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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An {@link Event} from {@code SchemaOperator} to notify {@code DataSinkWriterOperator} that it
 * start flushing.
 */
public class FlushEvent implements Event {

    /**
     * The schema changes from which table. If tableId is null, it means this {@code FlushEvent}
     * should flush all pending events, no matter which table it belongs to.
     */
    private final @Nullable TableId tableId;

    /**
     * With the Schema Operator - Registry topology, a nonce code is required to distinguish flush
     * events corresponding to each schema change event from different subTasks.
     */
    private final long nonce;

    /** With the distributed topology, we don't need to track flush events for various tables. */
    private static final FlushEvent FLUSH_ALL_EVENT = new FlushEvent(null, -1L);

    protected FlushEvent(@Nullable TableId tableId, long nonce) {
        this.tableId = tableId;
        this.nonce = nonce;
    }

    public static FlushEvent ofAll() {
        return FLUSH_ALL_EVENT;
    }

    public static FlushEvent of(@Nonnull TableId tableId, long nonce) {
        return new FlushEvent(tableId, nonce);
    }

    public @Nullable TableId getTableId() {
        return tableId;
    }

    public long getNonce() {
        return nonce;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlushEvent)) {
            return false;
        }
        FlushEvent that = (FlushEvent) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(nonce, that.nonce);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, nonce);
    }

    @Override
    public String toString() {
        if (tableId == null) {
            return "FlushEvent{ << not table-specific >> }";
        } else {
            return "FlushEvent{" + "tableId=" + tableId + ", nonce=" + nonce + '}';
        }
    }
}
