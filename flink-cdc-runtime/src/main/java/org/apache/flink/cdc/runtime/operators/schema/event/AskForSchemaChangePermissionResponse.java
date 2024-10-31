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

import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

/**
 * The request from {@link SchemaOperator} to {@link SchemaRegistry} to request to change schemas.
 */
public class AskForSchemaChangePermissionResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;
    private final boolean accepted;
    private final long versionCode;

    private AskForSchemaChangePermissionResponse(boolean accepted, long versionCode) {
        this.accepted = accepted;
        this.versionCode = versionCode;
    }

    public static AskForSchemaChangePermissionResponse accepted(long versionCode) {
        return new AskForSchemaChangePermissionResponse(true, versionCode);
    }

    public static AskForSchemaChangePermissionResponse rejected() {
        return new AskForSchemaChangePermissionResponse(false, -1L);
    }

    public long getVersionCode() {
        return versionCode;
    }

    public boolean isAccepted() {
        return accepted;
    }
}
