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

package org.apache.flink.cdc.connectors.stimps.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

/**
 * A {@link DataSource} for "stimps" connector that tests single-table-in-multi-partition scenario.
 */
@Internal
public class StimpsDataSource implements DataSource {

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceFunctionProvider.of(new StimpsSourceFunction());
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        throw new UnsupportedOperationException("Stimps doesn't need a metadata accessor!");
    }
}
