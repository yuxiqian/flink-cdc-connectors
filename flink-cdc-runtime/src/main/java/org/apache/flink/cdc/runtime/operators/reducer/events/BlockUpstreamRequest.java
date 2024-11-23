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

import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.reducer.SchemaMapper;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * A {@link OperatorEvent} from schema reducer to notify {@link SchemaMapper} that it should emit
 * {@link FlushEvent} to downstream sink writer.
 */
public class BlockUpstreamRequest implements OperatorEvent {
    private static final long serialVersionUID = 1L;

    /** The schema changes from which table is executing it. */
    private final TableId tableId;

    private final int reduceSeqNum;

    public BlockUpstreamRequest(TableId tableId, int reduceSeqNum) {
        this.tableId = tableId;
        this.reduceSeqNum = reduceSeqNum;
    }

    public TableId getTableId() {
        return tableId;
    }

    public int getReduceSeqNum() {
        return reduceSeqNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BlockUpstreamRequest)) {
            return false;
        }
        BlockUpstreamRequest that = (BlockUpstreamRequest) o;
        return Objects.equals(tableId, that.tableId) && reduceSeqNum == that.reduceSeqNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, reduceSeqNum);
    }
}
