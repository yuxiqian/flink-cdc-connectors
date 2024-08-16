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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventVisitorVoid;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeChecks;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** Supports {@link DorisDataSink} to schema evolution. */
public class DorisMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);
    private DorisOptions dorisOptions;
    private SchemaChangeManager schemaChangeManager;
    private Configuration config;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public DorisMetadataApplier(DorisOptions dorisOptions, Configuration config) {
        this.dorisOptions = dorisOptions;
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.config = config;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(ADD_COLUMN, ALTER_COLUMN_TYPE, DROP_COLUMN, RENAME_COLUMN);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        event.visit(
                new SchemaChangeEventVisitorVoid() {

                    @Override
                    public void visit(AddColumnEvent event) throws Exception {
                        applyAddColumnEvent(event);
                    }

                    @Override
                    public void visit(AlterColumnTypeEvent event) throws Exception {
                        applyAlterColumnTypeEvent(event);
                    }

                    @Override
                    public void visit(CreateTableEvent event) throws Exception {
                        applyCreateTableEvent(event);
                    }

                    @Override
                    public void visit(DropColumnEvent event) throws Exception {
                        applyDropColumnEvent(event);
                    }

                    @Override
                    public void visit(DropTableEvent event) {
                        throw new UnsupportedSchemaChangeEventException(event);
                    }

                    @Override
                    public void visit(RenameColumnEvent event) throws Exception {
                        applyRenameColumnEvent(event);
                    }

                    @Override
                    public void visit(TruncateTableEvent event) {
                        throw new UnsupportedSchemaChangeEventException(event);
                    }
                });
    }

    private void applyCreateTableEvent(CreateTableEvent event)
            throws IOException, IllegalArgumentException {
        Schema schema = event.getSchema();
        TableId tableId = event.tableId();
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTable(tableId.getTableName());
        tableSchema.setDatabase(tableId.getSchemaName());
        tableSchema.setFields(buildFields(schema));
        tableSchema.setDistributeKeys(buildDistributeKeys(schema));

        if (CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            tableSchema.setModel(DataModel.DUPLICATE);
        } else {
            tableSchema.setKeys(schema.primaryKeys());
            tableSchema.setModel(DataModel.UNIQUE);
        }

        Map<String, String> tableProperties =
                DorisDataSinkOptions.getPropertiesByPrefix(config, TABLE_CREATE_PROPERTIES_PREFIX);
        tableSchema.setProperties(tableProperties);
        schemaChangeManager.createTable(tableSchema);
    }

    private Map<String, FieldSchema> buildFields(Schema schema) {
        // Guaranteed the order of column
        Map<String, FieldSchema> fieldSchemaMap = new LinkedHashMap<>();
        List<String> columnNameList = schema.getColumnNames();
        for (String columnName : columnNameList) {
            Column column = schema.getColumn(columnName).get();
            String typeString;
            if (column.getType() instanceof LocalZonedTimestampType
                    || column.getType() instanceof TimestampType
                    || column.getType() instanceof ZonedTimestampType) {
                int precision = DataTypeChecks.getPrecision(column.getType());
                typeString =
                        String.format("%s(%s)", "DATETIMEV2", Math.min(Math.max(precision, 0), 6));
            } else {
                typeString =
                        DorisTypeMapper.toDorisType(
                                DataTypeUtils.toFlinkDataType(column.getType()));
            }
            fieldSchemaMap.put(
                    column.getName(),
                    new FieldSchema(
                            column.getName(),
                            typeString,
                            column.getDefaultValueExpression(),
                            column.getComment()));
        }
        return fieldSchemaMap;
    }

    private List<String> buildDistributeKeys(Schema schema) {
        if (!CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            return schema.primaryKeys();
        }
        if (!CollectionUtil.isNullOrEmpty(schema.getColumnNames())) {
            return Collections.singletonList(schema.getColumnNames().get(0));
        }
        return new ArrayList<>();
    }

    private String buildTypeString(DataType dataType) {
        if (dataType instanceof LocalZonedTimestampType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(dataType);
            return String.format("%s(%s)", "DATETIMEV2", Math.min(Math.max(precision, 0), 6));
        } else {
            return DorisTypeMapper.toDorisType(DataTypeUtils.toFlinkDataType(dataType));
        }
    }

    private void applyAddColumnEvent(AddColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
        for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
            Column column = col.getAddColumn();
            FieldSchema addFieldSchema =
                    new FieldSchema(
                            column.getName(),
                            buildTypeString(column.getType()),
                            column.getDefaultValueExpression(),
                            column.getComment());
            schemaChangeManager.addColumn(
                    tableId.getSchemaName(), tableId.getTableName(), addFieldSchema);
        }
    }

    private void applyDropColumnEvent(DropColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<String> droppedColumns = event.getDroppedColumnNames();
        for (String col : droppedColumns) {
            schemaChangeManager.dropColumn(tableId.getSchemaName(), tableId.getTableName(), col);
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        Map<String, String> nameMapping = event.getNameMapping();
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            schemaChangeManager.renameColumn(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    entry.getKey(),
                    entry.getValue());
        }
    }

    private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        Map<String, DataType> typeMapping = event.getTypeMapping();

        for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
            schemaChangeManager.modifyColumnDataType(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    new FieldSchema(
                            entry.getKey(),
                            buildTypeString(entry.getValue()),
                            null)); // Currently, AlterColumnTypeEvent carries no comment info. This
            // will be fixed after FLINK-35243 got merged.
        }
    }
}
