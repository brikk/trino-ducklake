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

import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airlift.json.ObjectMapperProvider
import io.trino.spi.TrinoException
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.Type
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import io.trino.type.TypeDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeRowIdentity {
    private val mapper = ObjectMapperProvider().apply {
        setJsonDeserializers(mapOf<Class<*>, JsonDeserializer<*>>(Type::class.java to TypeDeserializer(TESTING_TYPE_MANAGER)))
    }.get()

    @Test
    fun splitWireFormatDistinguishesAbsentNullAndZeroStarts() {
        for (start in listOf(null, 0L, 123L)) {
            val split = DucklakeSplit("file:///data.parquet", emptyList(), start, 10, 100, "parquet", TupleDomain.all())
                    .copy(dataFileId = 42L)
            val decoded = mapper.readValue(mapper.writeValueAsBytes(split), DucklakeSplit::class.java)
            assertThat(decoded).isEqualTo(split)
            assertThat(decoded.rowIdStart).isEqualTo(start)
            assertThat(decoded.dataFileId).isEqualTo(42L)
        }
        val split = DucklakeSplit("file:///data.parquet", emptyList(), 123L, 10, 100, "parquet", TupleDomain.all())
        val json = mapper.valueToTree<ObjectNode>(split)
        json.remove("rowIdStart")
        assertThat(mapper.treeToValue(json, DucklakeSplit::class.java).rowIdStart).isNull()
    }

    @Test
    fun mutationHandleAndRowTypeSurviveWorkerSerialization() {
        val file = DucklakeMergeTableHandle.DataFileRange(4, null, 10, emptyList(), "file:///data.parquet")
        assertThat(mapper.readValue(mapper.writeValueAsBytes(file), DucklakeMergeTableHandle.DataFileRange::class.java))
                .isEqualTo(file)
        val column = DucklakeColumnHandle.rowIdColumnHandle()
        assertThat(mapper.readValue(mapper.writeValueAsBytes(column), DucklakeColumnHandle::class.java)).isEqualTo(column)
        assertThat(column.columnType).isEqualTo(DucklakeMergeRowId.TYPE)
        val routing = DucklakeUpdatePartitioningHandle(listOf(DucklakeUpdatePartitioningHandle.FilePositions(4, 10)))
        assertThat(mapper.readValue(mapper.writeValueAsBytes(routing), DucklakeUpdatePartitioningHandle::class.java)).isEqualTo(routing)
    }

    @Test
    fun missingOrTruncatedIdentityFailsInsteadOfInventingIds() {
        assertThatThrownBy { DucklakeRowIdentity.rowId(null, null, 7) }
                .isInstanceOf(TrinoException::class.java).hasMessageContaining("neither is available")
        assertThatThrownBy { DucklakeRowIdentity.rowId(longArrayOf(41), 100L, 1) }
                .isInstanceOf(TrinoException::class.java).hasMessageContaining("shorter than the data file")
        assertThat(DucklakeRowIdentity.rowId(longArrayOf(41, 99), null, 1)).isEqualTo(99L)
        assertThat(DucklakeRowIdentity.rowId(longArrayOf(41, 99), 100L, 1)).isEqualTo(99L)
        assertThat(DucklakeRowIdentity.rowId(null, 0L, 7)).isEqualTo(7L)
    }
}
