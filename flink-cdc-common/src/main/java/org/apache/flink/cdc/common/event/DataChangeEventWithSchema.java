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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class {@code DataChangeEvent} represents the data change events of external systems, such as
 * INSERT, UPDATE, DELETE and so on.
 */
@PublicEvolving
public class DataChangeEventWithSchema implements ChangeEvent, Serializable {

    private static final long serialVersionUID = 1L;

    public DataChangeEventWithSchema(Schema schema, DataChangeEvent dataChangeEvent) {
        this.dataChangeEvent = dataChangeEvent;
        this.schema = schema;
    }

    /** Wrapped data change event. */
    private final DataChangeEvent dataChangeEvent;

    /** Schema description of given data change event. */
    private final Schema schema;

    @Override
    public TableId tableId() {
        return dataChangeEvent.tableId();
    }

    public Schema getSchema() {
        return schema;
    }

    public DataChangeEvent getDataChangeEvent() {
        return dataChangeEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataChangeEventWithSchema)) {
            return false;
        }
        DataChangeEventWithSchema that = (DataChangeEventWithSchema) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(dataChangeEvent, that.dataChangeEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, dataChangeEvent);
    }

    @Override
    public String toString() {
        String before, after;
        if (dataChangeEvent.before() == null) {
            before = "";
        } else {
            before =
                    SchemaUtils.createFieldGetters(schema).stream()
                            .map(fg -> fg.getFieldOrNull(dataChangeEvent.before()))
                            .map(o -> o != null ? o.toString() : "null")
                            .collect(Collectors.joining(", "));
        }

        if (dataChangeEvent.after() == null) {
            after = "";
        } else {
            after =
                    SchemaUtils.createFieldGetters(schema).stream()
                            .map(fg -> fg.getFieldOrNull(dataChangeEvent.after()))
                            .map(o -> o != null ? o.toString() : "null")
                            .collect(Collectors.joining(", "));
        }
        return String.format(
                "DataChangeEventWithSchema{schema=%s, before=[%s], after=[%s], op=%s, meta=%s}",
                schema, before, after, dataChangeEvent.op(), dataChangeEvent.meta());
    }
}
