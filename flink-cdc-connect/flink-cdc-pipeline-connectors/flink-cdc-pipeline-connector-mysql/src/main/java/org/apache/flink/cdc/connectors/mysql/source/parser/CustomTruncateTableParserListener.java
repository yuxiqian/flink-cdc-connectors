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

package org.apache.flink.cdc.connectors.mysql.source.parser;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.antlr.listener.TruncateTableParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/** Copied from {@link TruncateTableParserListener} in Debezium 1.9.8.Final. */
public class CustomTruncateTableParserListener extends MySqlParserBaseListener {
    private final MySqlAntlrDdlParser parser;

    private final LinkedList<SchemaChangeEvent> changes;

    private static final Logger LOG =
            LoggerFactory.getLogger(CustomTruncateTableParserListener.class);

    public CustomTruncateTableParserListener(
            MySqlAntlrDdlParser parser, LinkedList<SchemaChangeEvent> changes) {
        this.parser = parser;
        this.changes = changes;
    }

    public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = this.parser.parseQualifiedTableId(ctx.tableName().fullId());
        changes.add(new TruncateTableEvent(toCdcTableId(tableId)));
        super.enterTruncateTable(ctx);
    }

    private org.apache.flink.cdc.common.event.TableId toCdcTableId(TableId dbzTableId) {
        return org.apache.flink.cdc.common.event.TableId.tableId(
                dbzTableId.catalog(), dbzTableId.table());
    }
}
