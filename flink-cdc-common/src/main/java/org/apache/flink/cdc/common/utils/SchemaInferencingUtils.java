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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.Streams;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utils for {@link Schema} to perform inferencing evolution. Some methods might be different from
 * {@link SchemaUtils}'s merging rules
 */
@PublicEvolving
public class SchemaInferencingUtils {
    public static boolean isSchemaCompatible(
            @Nullable Schema currentSchema, Schema upcomingSchema) {
        if (currentSchema == null) {
            return false;
        }
        Map<String, DataType> currentColumnTypes =
                currentSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Column::getType));
        List<Column> upcomingColumns = upcomingSchema.getColumns();

        for (Column upcomingColumn : upcomingColumns) {
            String columnName = upcomingColumn.getName();
            DataType upcomingColumnType = upcomingColumn.getType();
            DataType currentColumnType = currentColumnTypes.get(columnName);

            if (!isDataTypeCompatible(currentColumnType, upcomingColumnType)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDataTypeCompatible(
            @Nullable DataType currentType, DataType upcomingType) {
        // If two types are identical, they're compatible of course.
        if (Objects.equals(currentType, upcomingType)) {
            return true;
        }

        // Or, if an upcoming column does not exist in current schema, it can't be compatible.
        if (currentType == null) {
            return false;
        }

        // Or, check if upcomingType is presented in the type merging tree.
        return TYPE_MERGING_TREE.get(upcomingType.getClass()).contains(currentType);
    }

    /**
     * Coercing {@code upcomingRow} with {@code upcomingTypes} schema into {@code currentTypes}
     * schema. Invoking this method implicitly assumes that {@code isSchemaCompatible(currentSchema,
     * upcomingSchema)} returns true, and no further check will be performed here. If these
     * preconditions are violated, unexpected things might happen!
     */
    public static Object[] coerceRow(
            String timezone,
            Schema currentSchema,
            Schema upcomingSchema,
            List<Object> upcomingRow) {
        List<Column> currentColumns = currentSchema.getColumns();
        Map<String, DataType> upcomingColumnTypes =
                upcomingSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Column::getType));
        Map<String, Object> upcomingColumnObjects =
                Streams.zip(
                                currentSchema.getColumnNames().stream(),
                                upcomingRow.stream(),
                                Tuple2::of)
                        .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
        Object[] coercedRow = new Object[currentSchema.getColumnCount()];

        for (int i = 0; i < currentSchema.getColumnCount(); i++) {
            Column currentColumn = currentColumns.get(i);
            String columnName = currentColumn.getName();
            if (upcomingColumnTypes.containsKey(columnName)) {

                DataType upcomingType = upcomingColumnTypes.get(columnName);
                DataType currentType = currentColumn.getType();

                if (Objects.equals(upcomingType, currentType)) {
                    coercedRow[i] = upcomingColumnObjects.get(columnName);
                } else {
                    coercedRow[i] =
                            coerceObject(
                                    timezone,
                                    upcomingColumnObjects.get(columnName),
                                    upcomingColumnTypes.get(columnName),
                                    currentColumn.getType());
                }
            } else {
                coercedRow[i] = null;
            }
        }
        return coercedRow;
    }

    /**
     * Try to merge {@code upcomingSchema} into {@code currentSchema} by performing lenient schema
     * changes. Returns evolved schema and corresponding schema change event interpretations.
     */
    public static Tuple2<Schema, List<SchemaChangeEvent>> getLeastCommonSchema(
            TableId tableId, @Nullable Schema currentSchema, Schema upcomingSchema) {
        // No current schema record, we need to create it first.
        if (currentSchema == null) {
            return Tuple2.of(
                    upcomingSchema,
                    Collections.singletonList(new CreateTableEvent(tableId, upcomingSchema)));
        }

        // Current schema is compatible with upcoming ones, just return it and perform no schema
        // evolution.
        if (isSchemaCompatible(currentSchema, upcomingSchema)) {
            return Tuple2.of(currentSchema, Collections.emptyList());
        }

        Map<String, DataType> oldTypeMapping = new HashMap<>();
        Map<String, DataType> newTypeMapping = new HashMap<>();
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();

        Map<String, Column> currentColumns =
                currentSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, col -> col));
        List<Column> upcomingColumns = upcomingSchema.getColumns();

        List<Column> appendedColumns = new ArrayList<>();

        for (Column upcomingColumn : upcomingColumns) {
            String columnName = upcomingColumn.getName();
            DataType upcomingColumnType = upcomingColumn.getType();
            if (currentColumns.containsKey(columnName)) {
                Column currentColumn = currentColumns.get(columnName);
                DataType currentColumnType = currentColumn.getType();
                DataType leastCommonType =
                        getLeastCommonType(currentColumnType, upcomingColumnType);
                if (!Objects.equals(leastCommonType, currentColumnType)) {
                    oldTypeMapping.put(columnName, currentColumnType);
                    newTypeMapping.put(columnName, leastCommonType);
                }

            } else {
                addedColumns.add(new AddColumnEvent.ColumnWithPosition(upcomingColumn));
                appendedColumns.add(upcomingColumn);
            }
        }

        List<Column> commonColumns = new ArrayList<>();
        for (Column column : currentSchema.getColumns()) {
            if (newTypeMapping.containsKey(column.getName())) {
                commonColumns.add(column.copy(newTypeMapping.get(column.getName())));
            } else {
                commonColumns.add(column);
            }
        }

        commonColumns.addAll(appendedColumns);

        List<SchemaChangeEvent> events = new ArrayList<>();
        if (!newTypeMapping.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, newTypeMapping, oldTypeMapping));
        }

        if (!addedColumns.isEmpty()) {
            events.add(new AddColumnEvent(tableId, addedColumns));
        }

        return Tuple2.of(currentSchema.copy(commonColumns), events);
    }

    private static DataType getLeastCommonType(DataType currentType, DataType targetType) {
        if (Objects.equals(currentType, targetType)) {
            return currentType;
        }
        List<DataType> currentTypeTree = TYPE_MERGING_TREE.get(currentType.getClass());
        List<DataType> targetTypeTree = TYPE_MERGING_TREE.get(targetType.getClass());

        for (DataType type : currentTypeTree) {
            if (targetTypeTree.contains(type)) {
                return type;
            }
        }

        // The most universal type and our final resort: STRING.
        return DataTypes.STRING();
    }

    public static Object coerceObject(
            String timezone,
            Object originalField,
            DataType originalType,
            DataType destinationType) {
        if (originalField == null) {
            return null;
        }
        if (destinationType.is(DataTypeRoot.BIGINT)) {
            if (originalField instanceof Byte) {
                // TINYINT
                return ((Byte) originalField).longValue();
            } else if (originalField instanceof Short) {
                // SMALLINT
                return ((Short) originalField).longValue();
            } else if (originalField instanceof Integer) {
                // INT
                return ((Integer) originalField).longValue();
            } else if (originalField instanceof Long) {
                // BIGINT
                return originalField;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot fit type \"%s\" into a BIGINT column. "
                                        + "Currently only TINYINT / SMALLINT / INT / LONG can be accepted by a BIGINT column",
                                originalField.getClass()));
            }
        } else if (destinationType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) destinationType;
            BigDecimal decimalValue;
            if (originalField instanceof Byte) {
                decimalValue = BigDecimal.valueOf(((Byte) originalField).longValue(), 0);
            } else if (originalField instanceof Short) {
                decimalValue = BigDecimal.valueOf(((Short) originalField).longValue(), 0);
            } else if (originalField instanceof Integer) {
                decimalValue = BigDecimal.valueOf(((Integer) originalField).longValue(), 0);
            } else if (originalField instanceof Long) {
                decimalValue = BigDecimal.valueOf((Long) originalField, 0);
            } else if (originalField instanceof DecimalData) {
                decimalValue = ((DecimalData) originalField).toBigDecimal();
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot fit type \"%s\" into a DECIMAL column. "
                                        + "Currently only BYTE / SHORT / INT / LONG / DECIMAL can be accepted by a DECIMAL column",
                                originalField.getClass()));
            }
            return decimalValue != null
                    ? DecimalData.fromBigDecimal(
                            decimalValue, decimalType.getPrecision(), decimalType.getScale())
                    : null;
        } else if (destinationType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            if (originalField instanceof Float) {
                // FLOAT
                return ((Float) originalField).doubleValue();
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot fit type \"%s\" into a DOUBLE column. "
                                        + "Currently only FLOAT can be accepted by a DOUBLE column",
                                originalField.getClass()));
            }
        } else if (destinationType.is(DataTypeRoot.VARCHAR)) {
            if (originalField instanceof StringData) {
                return originalField;
            } else {
                return BinaryStringData.fromString(originalField.toString());
            }
        } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            // For now, TimestampData / ZonedTimestampData / LocalZonedTimestampData has no
            // difference in its internal representation, so there's no need to do any precision
            // conversion.
            return originalField;
        } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
            return originalField;
        } else if (destinationType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && originalType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return originalField;
        } else if (destinationType.is(DataTypeFamily.TIMESTAMP)
                && originalType.is(DataTypeFamily.TIMESTAMP)) {
            return castToTimestamp(originalField, timezone);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Column type \"%s\" doesn't support type coercion", destinationType));
        }
    }

    private static TimestampData castToTimestamp(Object object, String timezone) {
        if (object == null) {
            return null;
        }
        if (object instanceof LocalZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((LocalZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else if (object instanceof ZonedTimestampData) {
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(
                            ((ZonedTimestampData) object).toInstant(), ZoneId.of(timezone)));
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to implicitly coerce object `%s` as a TIMESTAMP.", object));
        }
    }

    public static final Map<Class<? extends DataType>, List<DataType>> TYPE_MERGING_TREE =
            getTypeMergingTree();

    private static Map<Class<? extends DataType>, List<DataType>> getTypeMergingTree() {
        DataType stringType = DataTypes.STRING();
        DataType doubleType = DataTypes.DOUBLE();
        DataType floatType = DataTypes.FLOAT();
        DataType decimalType =
                DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
        DataType bigIntType = DataTypes.BIGINT();
        DataType intType = DataTypes.INT();
        DataType smallIntType = DataTypes.SMALLINT();
        DataType tinyIntType = DataTypes.TINYINT();
        DataType timestampTzType = DataTypes.TIMESTAMP_TZ(ZonedTimestampType.MAX_PRECISION);
        DataType timestampLtzType = DataTypes.TIMESTAMP_LTZ(LocalZonedTimestampType.MAX_PRECISION);
        DataType timestampType = DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION);
        DataType dateType = DataTypes.DATE();

        Map<Class<? extends DataType>, List<DataType>> mergingTree = new HashMap<>();

        // Simple data types
        mergingTree.put(VarCharType.class, ImmutableList.of(stringType));
        mergingTree.put(CharType.class, ImmutableList.of(stringType));
        mergingTree.put(BooleanType.class, ImmutableList.of(stringType));
        mergingTree.put(BinaryType.class, ImmutableList.of(stringType));
        mergingTree.put(DoubleType.class, ImmutableList.of(doubleType, stringType));
        mergingTree.put(FloatType.class, ImmutableList.of(floatType, doubleType, stringType));
        mergingTree.put(
                DecimalType.class,
                ImmutableList.of(decimalType, floatType, doubleType, stringType));
        mergingTree.put(
                BigIntType.class,
                ImmutableList.of(bigIntType, decimalType, floatType, doubleType, stringType));
        mergingTree.put(
                IntType.class,
                ImmutableList.of(
                        intType, bigIntType, decimalType, floatType, doubleType, stringType));
        mergingTree.put(
                SmallIntType.class,
                ImmutableList.of(
                        smallIntType,
                        intType,
                        bigIntType,
                        decimalType,
                        floatType,
                        doubleType,
                        stringType));
        mergingTree.put(
                TinyIntType.class,
                ImmutableList.of(
                        tinyIntType,
                        smallIntType,
                        intType,
                        bigIntType,
                        decimalType,
                        floatType,
                        doubleType,
                        stringType));

        // Timestamp series
        mergingTree.put(ZonedTimestampType.class, ImmutableList.of(timestampTzType, stringType));
        mergingTree.put(
                LocalZonedTimestampType.class,
                ImmutableList.of(timestampLtzType, timestampTzType, stringType));
        mergingTree.put(
                TimestampType.class,
                ImmutableList.of(timestampType, timestampLtzType, timestampTzType, stringType));
        mergingTree.put(
                DateType.class,
                ImmutableList.of(
                        dateType, timestampType, timestampLtzType, timestampTzType, stringType));
        mergingTree.put(TimeType.class, ImmutableList.of(stringType));

        // Complex types
        mergingTree.put(RowType.class, ImmutableList.of(stringType));
        mergingTree.put(ArrayType.class, ImmutableList.of(stringType));
        mergingTree.put(MapType.class, ImmutableList.of(stringType));
        return mergingTree;
    }
}
