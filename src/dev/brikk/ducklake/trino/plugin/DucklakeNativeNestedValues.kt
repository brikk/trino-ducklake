/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import io.trino.spi.block.MapBlockBuilder
import io.trino.spi.block.RowBlockBuilder
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.TypeUtils.writeNativeValue

/** Native JDBC payloads are values, not DuckDB text. Never parse their toString() output. */
internal object DucklakeNativeNestedValues {
    fun row(value: Any, type: RowType, mapping: InlinedTextFieldMapping.Struct?): Any {
        val source = when (value) {
            is java.sql.Struct -> {
                val names = requireNotNull(mapping?.sourceNames) { "Native STRUCT requires authoritative source-era field order" }
                val attributes = value.attributes
                require(names.size == attributes.size) { "Native STRUCT width differs from source-era metadata" }
                require(names.toSet().size == names.size) { "Duplicate source STRUCT field names" }
                names.zip(attributes.toList()).toMap()
            }
            is Map<*, *> -> value
            else -> error("Not a native STRUCT")
        }
        val fields = type.fields
        val targetNames = fields.map { it.name.orElseThrow().lowercase() }
        val keys = source.keys.map { require(it is String) { "Native STRUCT field name is not text" }; it.lowercase() }
        require(keys.toSet().size == keys.size) { "Ambiguous native STRUCT field names" }
        val expected = mapping?.sourceNames?.map { it.lowercase() } ?: if (mapping == null) targetNames else null
        if (expected != null) require(keys.toSet() == expected.toSet()) { "Native STRUCT fields differ from source metadata" }
        val indexByName = mapping?.indexByEraName ?: targetNames.withIndex().associate { it.value to it.index }
        val values = arrayOfNulls<Any?>(fields.size)
        source.entries.forEach { (name, item) ->
            indexByName[(name as String).lowercase()]?.let { index ->
                values[index] = DucklakeInlinedValueConverter.convertJdbcValue(item, fields[index].type, mapping?.children?.get(index))
            }
        }
        if (mapping != null) fields.indices.filter { mapping.eraNames[it] == null }.forEach { values[it] = mapping.defaults[it] }
        val builder = type.createBlockBuilder(null, 1) as RowBlockBuilder
        builder.buildEntry<RuntimeException> { targets -> fields.indices.forEach { writeNativeValue(fields[it].type, targets[it], values[it]) } }
        return type.getObject(builder.build(), 0)
    }

    fun map(value: Map<*, *>, type: MapType, mapping: InlinedTextFieldMapping.Container?): Any {
        if (type.keyType is RowType || type.keyType is ArrayType || type.keyType is MapType) {
            throw UnsupportedOperationException("Inlined map with nested KEY type ${type.keyType.displayName} is not supported")
        }
        val builder = type.createBlockBuilder(null, 1) as MapBlockBuilder
        builder.buildEntry<RuntimeException> { keys, values -> value.forEach { (key, item) ->
            require(key != null) { "Native MAP key is NULL" }
            writeNativeValue(type.keyType, keys, DucklakeInlinedValueConverter.convertJdbcValue(key, type.keyType))
            writeNativeValue(type.valueType, values, DucklakeInlinedValueConverter.convertJdbcValue(item, type.valueType, mapping?.child))
        } }
        return type.getObject(builder.build(), 0)
    }
}
