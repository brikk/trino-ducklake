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

import io.trino.spi.type.ArrayType
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.SqlVarbinary
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.TypeUtils.writeNativeValue
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.sql.Struct

class TestDucklakeNativeNestedValues {
    @Test
    fun nativeNamedStructPreservesPunctuationNullAndLiteralNull() {
        val type = RowType.from(listOf(RowType.field("a,b", INTEGER), RowType.field("x:y", VARCHAR), RowType.field("MiXeD", VARCHAR)))
        assertThat(materialize(type, linkedMapOf("a,b" to 7, "x:y" to "NULL", "MiXeD" to null)))
            .isEqualTo(listOf(7, "NULL", null))
        assertThatThrownBy { materialize(type, mapOf("unknown" to 7)) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("source metadata")
    }

    @Test
    fun textNamesAndVarcharEscapesAreDecodedWithoutConsumingBlobHexEscapes() {
        val type = RowType.from(listOf(RowType.field("back\\slash", VARCHAR), RowType.field("q'uote", VARCHAR),
            RowType.field("b", io.trino.spi.type.VarbinaryType.VARBINARY)))
        val raw = """{'back\\slash':'NULL','q\'uote':'C:\\docs','b':'\x00\x5C'}"""
        val row = materialize(type, raw) as List<*>
        assertThat(row[0]).isEqualTo("NULL")
        assertThat(row[1]).isEqualTo("C:\\docs")
        assertThat((row[2] as SqlVarbinary).bytes).containsExactly(0, 0x5c)
    }

    @Test
    fun jdbcStructUsesEraOrderNotTargetOrderAndRetainsRenameDropAndDefaultIdentity() {
        val type = RowType.from(listOf(RowType.field("renamed", INTEGER), RowType.field("text", VARCHAR), RowType.field("added", INTEGER)))
        val mapping = InlinedTextFieldMapping.Struct(listOf("old", "text", null), List(3) { null },
            listOf(null, null, 42L), listOf("text", "dropped", "old"))
        assertThat(materialize(type, struct("NULL", "discarded", 7), mapping)).isEqualTo(listOf(7, "NULL", 42))
        assertThat(materialize(type, struct(null, "discarded", null), mapping)).isEqualTo(listOf(null, null, 42))
    }

    @Test
    fun missingEraOrderAndWrongNativeWidthRefuseInsteadOfGuessing() {
        val type = RowType.from(listOf(RowType.field("renamed", INTEGER)))
        assertThatThrownBy { materialize(type, struct(7)) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("source-era field order")
        val mapping = InlinedTextFieldMapping.Struct(listOf("old"), listOf(null), listOf(null), listOf("old"))
        assertThatThrownBy { materialize(type, struct(7, 8), mapping) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("width")
    }

    @Test
    fun nativeListAndMapContainersPropagateTheirStructIdentityMapping() {
        val row = RowType.from(listOf(RowType.field("renamed", INTEGER)))
        val map = MapType(VARCHAR, row, TypeOperators())
        val type = ArrayType(map)
        val fields = InlinedTextFieldMapping.Struct(listOf("old"), listOf(null), listOf(null), listOf("old"))
        val mapping = InlinedTextFieldMapping.Container(InlinedTextFieldMapping.Container(fields))
        val raw = listOf(mapOf("k" to struct(7)), null, emptyMap<String, Any?>())
        assertThat(materialize(type, raw, mapping)).isEqualTo(listOf(mapOf("k" to listOf(7)), null, emptyMap<String, Any?>()))
    }

    @Test
    fun nativeMapDoesNotBroadenCompositeKeyOrNullKeyAdmission() {
        val row = RowType.from(listOf(RowType.field("k", INTEGER)))
        assertThatThrownBy { materialize(MapType(row, INTEGER, TypeOperators()), emptyMap<String, Any?>()) }
            .isInstanceOf(UnsupportedOperationException::class.java).hasMessageContaining("nested KEY")
        assertThatThrownBy { materialize(MapType(VARCHAR, INTEGER, TypeOperators()), mapOf(null to 1)) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("key is NULL")
    }

    private fun materialize(type: Type, value: Any, mapping: InlinedTextFieldMapping? = null): Any? =
        type.getObjectValue(writeNativeValue(type, DucklakeInlinedValueConverter.convertJdbcValue(value, type, mapping)), 0)

    private fun struct(vararg values: Any?): Struct = object : Struct {
        override fun getSQLTypeName(): String = "STRUCT"
        override fun getAttributes(): Array<out Any?> = values
        override fun getAttributes(map: MutableMap<String, Class<*>>?): Array<out Any?> = values
    }
}
