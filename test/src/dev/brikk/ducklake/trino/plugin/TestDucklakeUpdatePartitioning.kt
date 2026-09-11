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

import io.trino.spi.Page
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

import io.trino.spi.type.BigintType.BIGINT

class TestDucklakeUpdatePartitioning {
    @Test
    fun rowIdsFromOneDataFileAlwaysShareABucket()
    {
        val handle = DucklakeUpdatePartitioningHandle(listOf(
                DucklakeUpdatePartitioningHandle.FilePositions(11L, 10L),
                DucklakeUpdatePartitioningHandle.FilePositions(22L, 5L)))
        val buckets = DucklakeNodePartitioningProvider.UpdateBucketFunction(handle, 97)
        val block = DucklakeMergeRowId.TYPE.createBlockBuilder(null, 5)
        for ((file, position) in listOf(11L to 0L, 11L to 9L, 22L to 0L, 22L to 4L)) {
            block.buildEntry<RuntimeException> { fields ->
                BIGINT.writeLong(fields[0], file)
                BIGINT.writeLong(fields[1], position)
            }
        }
        block.appendNull() // NOT MATCHED INSERT: no source file
        val page = Page(block.build())

        assertThat(buckets.getBucket(page, 0)).isEqualTo(buckets.getBucket(page, 1))
        assertThat(buckets.getBucket(page, 2)).isEqualTo(buckets.getBucket(page, 3))
        assertThat(buckets.getBucket(page, 0)).isNotEqualTo(buckets.getBucket(page, 2))
        assertThat(buckets.getBucket(page, 4)).isZero()
    }

    @Test
    fun rowIdOutsideEveryFileFailsLoud()
    {
        val handle = DucklakeUpdatePartitioningHandle(listOf(
                DucklakeUpdatePartitioningHandle.FilePositions(11L, 10L)))
        val buckets = DucklakeNodePartitioningProvider.UpdateBucketFunction(handle, 8)
        val block = BIGINT.createBlockBuilder(null, 1)
        BIGINT.writeLong(block, 10L) // half-open end: not owned

        assertThatThrownBy { buckets.getBucket(Page(DucklakeMergeRowId.block(11L, block.build())), 0) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("position outside data file")
    }

    @Test
    fun duplicateFileIdsAreRejected()
    {
        assertThatThrownBy {
            DucklakeUpdatePartitioningHandle(listOf(
                    DucklakeUpdatePartitioningHandle.FilePositions(11L, 10L),
                    DucklakeUpdatePartitioningHandle.FilePositions(11L, 5L)))
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Duplicate DuckLake data file IDs")
    }
}
