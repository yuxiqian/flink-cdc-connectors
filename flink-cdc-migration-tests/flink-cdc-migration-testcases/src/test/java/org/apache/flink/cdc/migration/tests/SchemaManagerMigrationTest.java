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

package org.apache.flink.cdc.migration.tests;

import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;

import org.junit.Test;

/** Migration test cases for {@link SchemaManager}. */
public class SchemaManagerMigrationTest extends MigrationTestBase {

    public static String mockCaseName = "SchemaManagerMigrationMock";

    @Test
    public void testMigration() throws Exception {
        // Schema manager are not compatible between 3.0 to 3.1 update.
        for (FlinkCdcVersion version : getVersionSince(FlinkCdcVersion.v3_1_0)) {
            testMigrationFromTo(version, getSnapshotVersion(), mockCaseName);
        }
    }
}