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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** A test for the {@link SchemaInferencingUtils}. */
public class SchemaInferencingUtilsTest {

    @Test
    public void testSchemaCompatibilityChecking() {
        Schema lhs =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .physicalColumn("foo", DataTypes.STRING())
                        .physicalColumn("bar_3", DataTypes.STRING())
                        .physicalColumn("bar_2", DataTypes.STRING())
                        .build();

        Schema rhs =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(17))
                        .physicalColumn("foo", DataTypes.VARCHAR(10))
                        .physicalColumn("bar_2", DataTypes.STRING())
                        .build();

        Assertions.assertThat(SchemaInferencingUtils.isSchemaCompatible(lhs, rhs)).isTrue();
    }
}