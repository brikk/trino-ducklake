/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import com.google.common.collect.ImmutableList
import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.parquet.Field
import io.trino.parquet.GroupField
import io.trino.parquet.ParquetTypeUtils.getArrayElementColumn
import io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn
import io.trino.parquet.PrimitiveField
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.GroupColumnIO
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import org.apache.parquet.schema.Type.Repetition.REPEATED
import java.util.Optional

/**
 * Utility class for converting between Ducklake/Trino types and Parquet Field objects.
 * Supports all types: primitives, arrays, maps, structs/rows, and arbitrary nesting.
 */
object DucklakeParquetTypeUtils {
    /**
     * Construct a Parquet Field from a Trino type and Parquet ColumnIO.
     * Recursively handles all nested types (ROW, MAP, ARRAY).
     * Returns Optional.empty() if the columnIO is null (e.g., a struct field missing from an older Parquet file).
     */
    fun constructField(trinoType: Type, columnIO: ColumnIO?): Optional<Field> {
        return constructField(trinoType, columnIO, null)
    }

    /**
     * Identity-aware variant for DuckLake table columns. Nested ROW children resolve by column_id
     * before names, preventing rename/swap/drop-readd from binding the wrong physical child.
     */
    fun constructField(
            trinoType: Type,
            columnIO: ColumnIO?,
            rootColumnId: Long,
            currentColumns: List<DucklakeColumn>,
            eraColumns: List<DucklakeColumn>,
            sourceNamesByFieldId: Map<Long, String>,
            hasNameMap: Boolean): Optional<Field> {
        val context = IdentityContext(currentColumns, eraColumns, sourceNamesByFieldId, hasNameMap)
        return constructField(trinoType, columnIO, context.identity(rootColumnId), context)
    }

    private fun constructField(
            trinoType: Type,
            columnIO: ColumnIO?,
            identity: DucklakeColumn?,
            context: IdentityContext? = null): Optional<Field> {
        if (columnIO == null) {
            return Optional.empty()
        }
        return when (trinoType) {
            is RowType -> constructRowField(trinoType, columnIO as GroupColumnIO, identity, context)
            is MapType -> constructMapField(trinoType, columnIO as GroupColumnIO, identity, context)
            is ArrayType -> constructArrayField(trinoType, columnIO, identity, context)
            else -> constructPrimitiveField(trinoType, columnIO as PrimitiveColumnIO)
        }
    }

    private fun constructRowField(
            rowType: RowType,
            columnIO: GroupColumnIO,
            identity: DucklakeColumn?,
            context: IdentityContext?): Optional<Field> {
        val children = ImmutableList.builder<Optional<Field>>()
        var hasAnyField = false
        val identityChildren = identity?.let { context?.children(it.columnId) }.orEmpty()
        for ((index, rowField) in rowType.fields.withIndex()) {
            val fieldName = rowField.name
                    .orElseThrow { IllegalArgumentException("ROW type field must have a name") }
            val childIdentity = identityChildren.getOrNull(index)
            val childColumnIO = if (childIdentity == null) columnIO.getChild(fieldName)
                    else context!!.resolveChild(columnIO, childIdentity)
            val childField = constructField(rowField.type, childColumnIO, childIdentity, context)
            hasAnyField = hasAnyField or childField.isPresent
            children.add(childField)
        }
        if (!hasAnyField) {
            return Optional.empty()
        }
        return Optional.of(groupField(rowType, columnIO, children.build()))
    }

    private fun constructMapField(
            mapType: MapType,
            columnIO: GroupColumnIO,
            identity: DucklakeColumn?,
            context: IdentityContext?): Optional<Field> {
        val keyValueColumnIO = getMapKeyValueColumn(columnIO)
        if (keyValueColumnIO.childrenCount != 2) {
            return Optional.empty()
        }
        val children = identity?.let { context?.children(it.columnId) }.orEmpty()
        val keyIdentity = children.singleOrNull { it.columnName.equals("key", ignoreCase = true) }
        val valueIdentity = children.singleOrNull { it.columnName.equals("value", ignoreCase = true) }
        val keyField = constructField(mapType.keyType, keyValueColumnIO.getChild(0), keyIdentity, context)
        val valueField = constructField(mapType.valueType, keyValueColumnIO.getChild(1), valueIdentity, context)
        return Optional.of(groupField(mapType, columnIO, ImmutableList.of(keyField, valueField)))
    }

    private fun constructArrayField(
            arrayType: ArrayType,
            columnIO: ColumnIO,
            identity: DucklakeColumn?,
            context: IdentityContext?): Optional<Field> {
        val required = columnIO.type.repetition != OPTIONAL
        val repetitionLevel = columnIO.repetitionLevel
        val definitionLevel = columnIO.definitionLevel
        // Legacy 2-level LIST: repeated primitive with no intermediate group.
        if (columnIO is PrimitiveColumnIO) {
            if (columnIO.type.repetition != REPEATED || repetitionLevel == 0 || definitionLevel == 0) {
                throw TrinoException(NOT_SUPPORTED, "Unsupported schema for Parquet column (" + columnIO.columnDescriptor + ")")
            }
            val elementField: Optional<Field> = Optional.of<Field>(PrimitiveField(
                    arrayType.elementType, true, columnIO.columnDescriptor, columnIO.id))
            return Optional.of(GroupField(
                    arrayType, repetitionLevel - 1, definitionLevel - 1, true, ImmutableList.of(elementField)))
        }
        val groupColumnIO = columnIO as GroupColumnIO
        if (groupColumnIO.childrenCount != 1) {
            return Optional.empty()
        }
        val elementColumnIO = getArrayElementColumn(groupColumnIO.getChild(0))
        val elementIdentity = identity?.let { context?.children(it.columnId)?.singleOrNull() }
        val elementField = constructField(arrayType.elementType, elementColumnIO, elementIdentity, context)
        return Optional.of(groupField(arrayType, groupColumnIO, ImmutableList.of(elementField)))
    }

    private fun constructPrimitiveField(trinoType: Type, columnIO: PrimitiveColumnIO): Optional<Field> {
        val required = columnIO.type.repetition != OPTIONAL
        return Optional.of(PrimitiveField(
                trinoType,
                required,
                columnIO.columnDescriptor,
                columnIO.id))
    }

    private fun groupField(type: Type, columnIO: GroupColumnIO, children: List<Optional<Field>>): GroupField =
        GroupField(
                type,
                columnIO.repetitionLevel,
                columnIO.definitionLevel,
                columnIO.type.repetition != OPTIONAL,
                children)

    private class IdentityContext(
            currentColumns: List<DucklakeColumn>,
            eraColumns: List<DucklakeColumn>,
            private val sourceNamesByFieldId: Map<Long, String>,
            private val hasNameMap: Boolean) {
        private val currentById = currentColumns.associateBy { it.columnId }
        private val eraById = eraColumns.associateBy { it.columnId }
        private val childrenByParent = currentColumns.filter { it.parentColumn != null }
                .groupBy { it.parentColumn!! }
                .mapValues { (_, children) -> children.sortedBy { it.columnOrder } }

        fun identity(columnId: Long): DucklakeColumn? = currentById[columnId]

        fun children(columnId: Long): List<DucklakeColumn> = childrenByParent[columnId].orEmpty()

        fun resolveChild(group: GroupColumnIO, child: DucklakeColumn): ColumnIO? {
            sourceNamesByFieldId[child.columnId]?.let { sourceName ->
                return group.getChild(sourceName)
            }
            if (!hasNameMap) {
                directChildWithFieldId(group, child.columnId)?.let { return it }
                val era = eraById[child.columnId]
                if (era != null) {
                    group.getChild(era.columnName)?.let { return it }
                }
                if (eraColumnsAbsent() || era != null) {
                    return group.getChild(child.columnName)
                }
                return null
            }
            // Catalog 0.7.2 returns only top-level name-map rows. Preserve the old nested-name
            // fallback until it exposes nested target ids; callers track that remaining gap.
            return group.getChild(child.columnName)
        }

        private fun eraColumnsAbsent(): Boolean = eraById.isEmpty()

        private fun directChildWithFieldId(group: GroupColumnIO, fieldId: Long): ColumnIO? {
            for (index in 0 until group.childrenCount) {
                val child = group.getChild(index)
                if (child.type.id?.intValue()?.toLong() == fieldId) {
                    return child
                }
            }
            return null
        }
    }
}
