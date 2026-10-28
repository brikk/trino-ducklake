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

import io.airlift.slice.Slice
import io.trino.spi.Page
import io.trino.spi.type.StandardTypes
import io.trino.spi.type.Type
import io.trino.spi.type.TypeUtils.readNativeValue
import io.trino.spi.type.VarcharType

/**
 * DuckLake maps its `json` type to Trino's native `JsonType` (`io.trino.type.JsonType`, which
 * lives in `trino-main` — NOT importable on the connector's `trino-spi`-only compile classpath).
 * This helper lets the plugin recognise and manipulate JSON columns without a compile-time reference:
 *
 *   - [isJson] identifies the type by its type-signature base string (`"json"`), which is stable
 *       and requires neither a class import nor a `TypeManager` handle.
 *   - 483 JSON uses UTF-8 Slice blocks; 484 uses native JSON blocks. The SPI's `getSlice` /
 *       `writeSlice` text boundary works with both. Never hand a 484 JSON block to a VARCHAR writer.
 *   - [toParquetWriteType] swaps a top-level JSON column for VARCHAR when building the *physical*
 *       parquet write schema: `io.trino.parquet.writer.ParquetSchemaConverter` / `ParquetWriters`
 *       reject `JsonType`, and DuckLake/DuckDB itself stores JSON in parquet as a plain UTF-8
 *       string column — so writing it physically as VARCHAR is lossless and cross-engine-faithful.
 *       The DuckLake catalog type stays `json`, and the read path reconstructs it as `JsonType`.
 *       JSON nested inside ARRAY/MAP/ROW is out of scope for v1 (the parquet library rejects it
 *       loudly, which is the desired fail-loud behavior).
 */
object DucklakeJsonSupport {
    fun isJson(type: Type): Boolean = type.baseName == StandardTypes.JSON

    fun nativeValue(type: Type, text: Slice): Any {
        if (type.javaType == Slice::class.java) return text
        val builder = type.createBlockBuilder(null, 1)
        type.writeSlice(builder, text)
        return requireNotNull(readNativeValue(type, builder.build(), 0))
    }

    /** Convert only JSON channels requiring a different physical block (484), preserving text and NULLs. */
    fun toParquetPage(page: Page, types: List<Type>): Page {
        val jsonChannels = types.indices.filter { isJson(types[it]) && types[it].javaType != Slice::class.java }
        if (jsonChannels.isEmpty()) return page
        val blocks = Array(page.channelCount) { page.getBlock(it) }
        for (channel in jsonChannels) {
            val type = types[channel]
            val block = blocks[channel]
            val builder = VarcharType.VARCHAR.createBlockBuilder(null, page.positionCount)
            for (position in 0 until page.positionCount) {
                if (block.isNull(position)) builder.appendNull()
                else VarcharType.VARCHAR.writeSlice(builder, type.getSlice(block, position))
            }
            blocks[channel] = builder.build()
        }
        return Page(page.positionCount, *blocks)
    }

    /**
     * The physical Trino type to hand the parquet writer for [type]: a top-level JSON column
     * becomes unbounded VARCHAR, everything else is unchanged. Used only for the parquet write
     * schema + value writers; the logical catalog type is untouched.
     */
    fun toParquetWriteType(type: Type): Type =
            if (isJson(type)) VarcharType.VARCHAR else type
}
