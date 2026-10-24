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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeException
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.block.MapBlockBuilder
import io.trino.spi.block.RowBlockBuilder
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeUtils.readNativeValue
import io.trino.spi.type.TypeUtils.writeNativeValue
import java.lang.reflect.InvocationTargetException

/** Per-footer projection. Only the physical resolver may register an absent stable field. */
internal class DucklakeFileDefaults(
    private val catalog: DucklakeCatalog,
    private val table: DucklakeTableHandle,
    columns: List<DucklakeColumn>,
    private val typeConverter: DucklakeTypeConverter) {
    private val targetColumns = columns.associateBy { it.columnId }
    private val children = columns.filter { it.parentColumn != null }.groupBy { it.parentColumn!! }
        .mapValues { (_, nodes) -> nodes.sortedBy { it.columnOrder } }
    private val tree = columns
    private val defaults = mutableMapOf<Long, Any?>()

    fun validate(columnId: Long, type: Type) {
        val target = targetColumns[columnId] ?: throw TrinoException(NOT_SUPPORTED, "Missing final-1.1 target field ID $columnId")
        if (type != typeConverter.toTrinoType(target, tree)) throw TrinoException(NOT_SUPPORTED,
            "File projection type differs from fresh target metadata for field $columnId")
    }

    fun missing(column: DucklakeColumn, type: Type) {
        validate(column.columnId, type)
        defaults[column.columnId] = DucklakeDecodedValueConverter.convert(decoded(column.columnId), type)
    }

    fun missingBlock(column: DucklakeColumnHandle): Block {
        val target = targetColumns.getValue(column.columnId)
        missing(target, column.columnType)
        return writeNativeValue(column.columnType, defaults[column.columnId])
    }

    // Optional public API boundary, like the opt-in setter: no fallback to text, SQL or guessed NULL.
    private fun decoded(columnId: Long): Any? {
        require(catalog is JdbcDucklakeCatalog) { "Decoded file defaults require JdbcDucklakeCatalog" }
        val method = defaultMethod()
        return try { method.invoke(catalog, table.tableId, columnId, table.snapshotId) }
        catch (e: InvocationTargetException) { throw projectionFailure(e) }
    }

    private fun defaultMethod(): java.lang.reflect.Method = try {
        JdbcDucklakeCatalog::class.java.getMethod("getMissingFileColumnDefaultDecoded",
            Long::class.javaPrimitiveType, Long::class.javaPrimitiveType, Long::class.javaPrimitiveType)
    } catch (e: NoSuchMethodException) {
        throw TrinoException(NOT_SUPPORTED, "Final-1.1 file evolution requires the decoded catalog default API", e)
    }

    private fun projectionFailure(failure: InvocationTargetException): Throwable {
        val cause = failure.targetException
        return if (cause is DucklakeException) TrinoException(NOT_SUPPORTED, cause.message ?: "Catalog refused file default projection", cause)
        else cause
    }

    fun needsRewrite(columnId: Long): Boolean = columnId in defaults || children[columnId].orEmpty().any { needsRewrite(it.columnId) }

    fun rewrite(column: DucklakeColumnHandle, block: Block): Block {
        val type = column.columnType
        val builder = type.createBlockBuilder(null, block.positionCount)
        for (position in 0 until block.positionCount) {
            writeNativeValue(type, builder, value(column.columnId, type, block, position))
        }
        return builder.build()
    }

    private fun value(id: Long, type: Type, block: Block, position: Int): Any? {
        if (id in defaults) return defaults[id]
        // A missing child never makes a stored NULL parent non-NULL.
        if (block.isNull(position)) return null
        if (!needsRewrite(id)) return readNativeValue(type, block, position)
        val nodes = children[id].orEmpty()
        return when (type) {
            is RowType -> {
                val row = type.getObject(block, position)
                val builder = type.createBlockBuilder(null, 1) as RowBlockBuilder
                builder.buildEntry<RuntimeException> { fields -> type.fields.forEachIndexed { index, field ->
                    writeNativeValue(field.type, fields[index], value(nodes[index].columnId, field.type, row.getRawFieldBlock(index), row.rawIndex))
                } }
                type.getObject(builder.build(), 0)
            }
            is ArrayType -> {
                val elements = type.getObject(block, position)
                val builder = type.elementType.createBlockBuilder(null, elements.positionCount)
                for (index in 0 until elements.positionCount) writeNativeValue(type.elementType, builder,
                    value(nodes.single().columnId, type.elementType, elements, index))
                builder.build()
            }
            is MapType -> {
                val map = type.getObject(block, position)
                val key = nodes.single { it.columnName.equals("key", ignoreCase = true) }
                val item = nodes.single { it.columnName.equals("value", ignoreCase = true) }
                val builder = type.createBlockBuilder(null, 1) as MapBlockBuilder
                builder.buildEntry<RuntimeException> { keys, values ->
                    for (index in 0 until map.size) {
                        writeNativeValue(type.keyType, keys, value(key.columnId, type.keyType, map.rawKeyBlock, map.rawOffset + index))
                        writeNativeValue(type.valueType, values, value(item.columnId, type.valueType, map.rawValueBlock, map.rawOffset + index))
                    }
                }
                type.getObject(builder.build(), 0)
            }
            else -> readNativeValue(type, block, position)
        }
    }
}
