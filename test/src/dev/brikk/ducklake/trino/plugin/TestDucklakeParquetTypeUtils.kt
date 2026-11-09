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

import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.parquet.GroupField
import io.trino.parquet.PrimitiveField
import io.trino.spi.ErrorCodeSupplier
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Unit tests for [DucklakeParquetTypeUtils.constructField], focused on the parquet LIST
 * backward-compatibility encodings that `add_files` may encounter in files written by other
 * engines (Spark, legacy Hive).
 */
class TestDucklakeParquetTypeUtils {
    private val arrayOfInt = ArrayType(IntegerType.INTEGER)

    @Test
    fun testTwoLevelRepeatedPrimitiveListBuildsGroupField() {
        // Legacy 2-level list: `repeated int32 element` directly under the message, no LIST group.
        val columnIO = columnIO("message m { repeated int32 my_list; }")

        val field = DucklakeParquetTypeUtils.constructField(arrayOfInt, columnIO.getChild(0))

        assertThat(field).isPresent
        val groupField = field.get() as GroupField
        assertThat(groupField.type).isEqualTo(arrayOfInt)
        // Levels are adjusted down one and the repeated primitive becomes the single element.
        assertThat(groupField.children).hasSize(1)
        assertThat(groupField.children[0]).isPresent
        assertThat(groupField.children[0].get()).isInstanceOf(PrimitiveField::class.java)
    }

    @Test
    fun testThreeLevelListStillBuildsGroupField() {
        val columnIO = columnIO(
                "message m { optional group my_list (LIST) { repeated group list { optional int32 element; } } }")

        val field = DucklakeParquetTypeUtils.constructField(arrayOfInt, columnIO.getChild(0))

        assertThat(field).isPresent
        assertThat(field.get()).isInstanceOf(GroupField::class.java)
        assertThat((field.get() as GroupField).type).isEqualTo(arrayOfInt)
    }

    @Test
    fun testVariantGroupReadAsVarcharIsAnExplicitUnsupportedRead() {
        val columnIO = columnIO("message m { optional group v { required binary metadata; required binary value; } }")
        val variant = DucklakeColumn(1, 1, null, 1, 1, "v", "VARIANT", true, null, null)

        assertErrorCode(NOT_SUPPORTED) {
            DucklakeParquetTypeUtils.constructField(VARCHAR, columnIO.getChild(0), 1, listOf(variant), listOf(variant), emptyMap(), false)
        }
    }

    @Test
    fun testGroupWhereCatalogHasScalarIsAnInternalError() {
        val columnIO = columnIO("message m { optional group v { required int32 x; } }")
        val scalar = DucklakeColumn(1, 1, null, 1, 1, "v", "VARCHAR", true, null, null)

        assertErrorCode(GENERIC_INTERNAL_ERROR) {
            DucklakeParquetTypeUtils.constructField(VARCHAR, columnIO.getChild(0), 1, listOf(scalar), listOf(scalar), emptyMap(), false)
        }
    }

    @Test
    fun testNonRepeatedPrimitiveWhereCatalogHasArrayIsAnInternalError() {
        val columnIO = columnIO("message m { required int32 my_list; }")

        assertErrorCode(GENERIC_INTERNAL_ERROR) { DucklakeParquetTypeUtils.constructField(arrayOfInt, columnIO.getChild(0)) }
    }

    private fun assertErrorCode(expected: ErrorCodeSupplier, action: () -> Unit) {
        assertThatThrownBy(action)
                .isInstanceOfSatisfying(TrinoException::class.java) {
                    assertThat(it.errorCode).isEqualTo(expected.toErrorCode())
                }
    }

    private fun columnIO(schemaText: String): MessageColumnIO {
        val schema: MessageType = MessageTypeParser.parseMessageType(schemaText)
        return ColumnIOFactory().getColumnIO(schema)
    }
}
