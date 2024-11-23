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
import org.apache.flink.cdc.runtime.operators.reducer.SchemaReducer;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.Map;
import java.util.Objects;

/** Mapper's request to {@link SchemaReducer} for reducing an incompatible schema. */
public class ReduceSchemaResponse implements CoordinationResponse {

    private final Map<TableId, Schema> latestReducedSchema;
    private final int reduceSeqNum;

    public ReduceSchemaResponse(Map<TableId, Schema> latestReducedSchema, int reduceSeqNum) {
        this.latestReducedSchema = latestReducedSchema;
        this.reduceSeqNum = reduceSeqNum;
    }

    public Map<TableId, Schema> getLatestReducedSchema() {
        return latestReducedSchema;
    }

    public int getReduceSeqNum() {
        return reduceSeqNum;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReduceSchemaResponse)) {
            return false;
        }
        ReduceSchemaResponse that = (ReduceSchemaResponse) o;
        return Objects.equals(latestReducedSchema, that.latestReducedSchema)
                && reduceSeqNum == that.reduceSeqNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestReducedSchema, reduceSeqNum);
    }

    @Override
    public String toString() {
        return "ReduceSchemaResponse{"
                + "latestReducedSchema="
                + latestReducedSchema
                + ", reduceSeqNum="
                + reduceSeqNum
                + '}';
    }
}
