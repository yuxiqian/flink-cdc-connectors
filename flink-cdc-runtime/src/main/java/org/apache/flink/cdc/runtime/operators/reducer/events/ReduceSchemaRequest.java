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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.operators.reducer.SchemaReducer;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

/** Mapper's request to {@link SchemaReducer} for reducing an incompatible schema. */
public class ReduceSchemaRequest implements CoordinationRequest {
    // Indicating which source subTask does this schema change event comes from.
    private final int sourceSubTaskId;

    // Indicating which schema mapper initiates this schema change request.
    private final int sinkSubTaskId;

    // A schema change event is uniquely bound to a sourceSubTaskId.
    private final SchemaChangeEvent schemaChangeEvent;

    public static ReduceSchemaRequest createAlignRequest(int sinkSubTaskId) {
        return new ReduceSchemaRequest(-1, sinkSubTaskId, null);
    }

    public ReduceSchemaRequest(
            int sourceSubTaskId, int sinkSubTaskId, SchemaChangeEvent schemaChangeEvent) {
        this.sourceSubTaskId = sourceSubTaskId;
        this.sinkSubTaskId = sinkSubTaskId;
        this.schemaChangeEvent = schemaChangeEvent;
    }

    // Checking if this schema request was triggered by a "blockUpstreamRequest" for mapper
    // alignment purposes, instead of an actual incompatible schema. We don't need to infer schema
    // for those requests.
    public boolean isAlignRequest() {
        return sourceSubTaskId == -1 || schemaChangeEvent == null;
    }

    public int getSourceSubTaskId() {
        Preconditions.checkState(
                !isAlignRequest(), "Unable to fetch source subTaskId for an align event.");
        return sourceSubTaskId;
    }

    public int getSinkSubTaskId() {
        return sinkSubTaskId;
    }

    public SchemaChangeEvent getSchemaChangeEvent() {
        Preconditions.checkState(
                !isAlignRequest(), "Unable to fetch source subTaskId for an align event.");
        return schemaChangeEvent;
    }

    @Override
    public String toString() {
        return isAlignRequest()
                ? "ReduceSchemaRequest{sinkSubTaskId=" + sinkSubTaskId + ", align=true}"
                : "ReduceSchemaRequest{"
                        + "sourceSubTaskId="
                        + sourceSubTaskId
                        + ", sinkSubTaskId="
                        + sinkSubTaskId
                        + ", schemaChangeEvent="
                        + schemaChangeEvent
                        + '}';
    }
}
