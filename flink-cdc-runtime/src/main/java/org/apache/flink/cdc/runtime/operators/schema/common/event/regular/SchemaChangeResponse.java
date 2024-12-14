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

package org.apache.flink.cdc.runtime.operators.schema.common.event.regular;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The response for {@link SchemaChangeRequest} from {@link SchemaCoordinator} to {@link
 * SchemaOperator}.
 */
public class SchemaChangeResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    /**
     * Actually finished schema change events. This will only be effective if status is {@code
     * accepted}.
     */
    private final List<Event> appliedSchemaChangeEvents;

    private final Map<TableId, Schema> evolvedSchemas;

    private final ResponseCode responseCode;

    public static SchemaChangeResponse success(
            List<Event> schemaChangeEvents, Map<TableId, Schema> evolvedSchemas) {
        return new SchemaChangeResponse(ResponseCode.SUCCESS, schemaChangeEvents, evolvedSchemas);
    }

    public static SchemaChangeResponse busy() {
        return new SchemaChangeResponse(ResponseCode.BUSY);
    }

    public static SchemaChangeResponse waitingForFlush() {
        return new SchemaChangeResponse(ResponseCode.WAITING_FOR_FLUSH);
    }

    private SchemaChangeResponse(ResponseCode responseCode) {
        this(responseCode, Collections.emptyList(), Collections.emptyMap());
    }

    private SchemaChangeResponse(
            ResponseCode responseCode,
            List<Event> appliedSchemaChangeEvents,
            Map<TableId, Schema> evolvedSchemas) {
        this.responseCode = responseCode;
        this.appliedSchemaChangeEvents = appliedSchemaChangeEvents;
        this.evolvedSchemas = evolvedSchemas;
    }

    public boolean isSuccess() {
        return ResponseCode.SUCCESS.equals(responseCode);
    }

    public boolean isRegistryBusy() {
        return ResponseCode.BUSY.equals(responseCode);
    }

    public boolean isWaitingForFlush() {
        return ResponseCode.WAITING_FOR_FLUSH.equals(responseCode);
    }

    public Map<TableId, Schema> getEvolvedSchemas() {
        return evolvedSchemas;
    }

    public List<Event> getAppliedEvents() {
        return appliedSchemaChangeEvents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaChangeResponse)) {
            return false;
        }
        SchemaChangeResponse response = (SchemaChangeResponse) o;
        return Objects.equals(evolvedSchemas, response.evolvedSchemas)
                && Objects.equals(appliedSchemaChangeEvents, response.appliedSchemaChangeEvents)
                && responseCode == response.responseCode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(evolvedSchemas, appliedSchemaChangeEvents, responseCode);
    }

    @Override
    public String toString() {
        return "SchemaChangeResponse{"
                + "appliedSchemaChangeEvents="
                + appliedSchemaChangeEvents
                + ", evolvedSchemas="
                + evolvedSchemas
                + ", responseCode="
                + responseCode
                + '}';
    }

    /**
     * Schema Change Response status code.
     *
     * <p>- Accepted: Requested schema change request has been accepted exclusively. Any other
     * schema change requests will be blocked.
     *
     * <p>- Busy: Schema registry is currently busy processing another schema change request.
     *
     * <p>- Waiting for Flush: This schema change request has not collected enough flush success
     * events and could not be safely applied yet.
     */
    public enum ResponseCode {
        SUCCESS,
        BUSY,
        WAITING_FOR_FLUSH
    }
}
