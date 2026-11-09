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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestDucklakeLongSet {
    @Test
    fun fullLongDomainUnionDifferenceAndCopyDoNotNarrowPositions() {
        val values = listOf(Long.MIN_VALUE, -1L, 0L, Int.MAX_VALUE.toLong(), 0xffffffffL, 1L shl 32, Long.MAX_VALUE)
        val set = DucklakeLongSet.copyOf(values)
        assertThat(set).containsExactlyInAnyOrderElementsOf(values)
        values.forEach { assertThat(set.containsLong(it)).isTrue() }
        val copy = set.copy()
        copy.addAll(set)
        copy.remove(Long.MIN_VALUE)
        assertThat(set).hasSize(values.size)
        assertThat(set.subtract(copy)).containsExactly(Long.MIN_VALUE)
        assertThat(copy.filterLong { it >= 0 }).containsExactlyInAnyOrderElementsOf(values.filter { it >= 0 })
    }

    @Test
    fun retainedMemoryTracksCompressedContainersNotBoxedCardinality() {
        val set = DucklakeLongSet()
        val emptySize = set.getRetainedSizeInBytes()
        set.addRange(0, 1_000_000)
        assertThat(set).hasSize(1_000_000)
        assertThat(set.getRetainedSizeInBytes()).isGreaterThan(emptySize).isLessThan(1_000_000)
        assertThat(set.containsLong(999_999)).isTrue()
        assertThat(set.containsLong(1_000_000)).isFalse()
    }
}
