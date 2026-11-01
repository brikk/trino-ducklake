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

import dev.brikk.ducklake.catalog.DucklakeTypeSignature
import dev.brikk.ducklake.catalog.DucklakeQuotedList
import dev.brikk.ducklake.catalog.InlinedTypeNode
import io.trino.spi.type.ArrayType
import io.trino.spi.type.RowType
import io.trino.spi.type.StandardTypes
import io.trino.spi.type.Type
import io.trino.spi.type.TypeManager
import io.trino.spi.type.TypeParameter
import java.util.Locale
import java.util.Optional

private val SIMPLE_CATALOG_FIELD = Regex("[A-Za-z_][A-Za-z0-9_]*")

/** Producing the public catalog grammar needs the same quoted-name encoding as consuming it. */
internal fun catalogTypeFieldName(name: String): String =
    if (SIMPLE_CATALOG_FIELD.matches(name)) name else DucklakeQuotedList.encodeValue(name)

/** Use the candidate's public bounded grammar, never a connector-side approximation of it. */
internal fun catalogCompositeType(signature: String, typeManager: TypeManager, scalar: (String) -> Type): Type? {
    val keyword = signature.trimStart().takeWhile { it.isLetterOrDigit() || it == '_' }.lowercase(Locale.ROOT)
    if (keyword !in setOf("struct", "list", "map")) return null
    fun convert(node: InlinedTypeNode): Type = when (node.type) {
        "list" -> ArrayType(convert(node.children.single()))
        "struct" -> RowType.from(node.children.map { child -> RowType.Field(Optional.of(child.name), convert(child)) })
        "map" -> typeManager.getParameterizedType(StandardTypes.MAP, listOf(
            TypeParameter.typeParameter(convert(node.children[0]).typeDescriptor),
            TypeParameter.typeParameter(convert(node.children[1]).typeDescriptor)))
        else -> scalar(node.type)
    }
    return convert(DucklakeTypeSignature.parse(signature))
}
