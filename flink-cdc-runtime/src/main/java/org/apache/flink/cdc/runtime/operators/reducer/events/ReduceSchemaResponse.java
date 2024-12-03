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
import org.apache.flink.cdc.runtime.operators.reducer.SchemaReducer;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.List;
import java.util.Objects;

/** Mapper's request to {@link SchemaReducer} for reducing an incompatible schema. */
public class ReduceSchemaResponse implements CoordinationResponse {

    private final List<SchemaChangeEvent> reducedSchemaResult;
    private final int reduceSeqNum;

    public ReduceSchemaResponse(List<SchemaChangeEvent> reducedSchemaResult, int reduceSeqNum) {
        this.reducedSchemaResult = reducedSchemaResult;
        this.reduceSeqNum = reduceSeqNum;
    }

    public List<SchemaChangeEvent> getReducedSchemaResult() {
        return reducedSchemaResult;
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
        return Objects.equals(reducedSchemaResult, that.reducedSchemaResult)
                && reduceSeqNum == that.reduceSeqNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reducedSchemaResult, reduceSeqNum);
    }

    @Override
    public String toString() {
        return "ReduceSchemaResponse{"
                + "reducedSchemaResult="
                + reducedSchemaResult
                + ", reduceSeqNum="
                + reduceSeqNum
                + '}';
    }
}
