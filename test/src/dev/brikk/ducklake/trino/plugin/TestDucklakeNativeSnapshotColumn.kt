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

import io.trino.parquet.ParquetTypeUtils.getColumnIO
import io.trino.spi.TrinoException
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import org.apache.parquet.schema.MessageTypeParser
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeNativeSnapshotColumn {
    @Test
    fun reservedFieldIdNotANameCoincidenceIdentifiesTheColumn() {
        val schema = MessageTypeParser.parseMessageType("message t { optional int64 renamed = 2147483539; optional int64 _ducklake_internal_snapshot_id = 3; }")
        assertThat(DucklakeNativeSnapshotColumn.resolve(getColumnIO(schema, schema))!!.name).isEqualTo("renamed")
        val withoutId = MessageTypeParser.parseMessageType("message t { optional int64 _ducklake_internal_snapshot_id; }")
        assertThat(DucklakeNativeSnapshotColumn.resolve(getColumnIO(withoutId, withoutId))).isNull()
    }

    @Test
    fun ambiguousOrNonBigintFieldsFailInsteadOfReturningFileConstants() {
        for (fields in listOf("optional int32 bad = 2147483539;", "repeated int64 bad = 2147483539;",
            "optional int64 a = 2147483539; optional int64 b = 2147483539;")) {
            val schema = MessageTypeParser.parseMessageType("message t { $fields }")
            assertThatThrownBy { DucklakeNativeSnapshotColumn.resolve(getColumnIO(schema, schema)) }
                .isInstanceOf(TrinoException::class.java)
        }
    }

    @Test
    fun presentNullOrNegativeSnapshotsFailLoudly() {
        for (value in listOf(null, -1L)) {
            assertThatThrownBy { DucklakeNativeSnapshotColumn.validate(writeNativeValue(BIGINT, value)) }
                .isInstanceOf(TrinoException::class.java).hasMessageContaining("NULL or negative")
        }
        assertThat(BIGINT.getLong(DucklakeNativeSnapshotColumn.validate(writeNativeValue(BIGINT, 7L)), 0)).isEqualTo(7L)
        assertThat(BIGINT.getLong(DucklakeNativeSnapshotColumn.fallback(9L), 0)).isEqualTo(9L)
    }
}
