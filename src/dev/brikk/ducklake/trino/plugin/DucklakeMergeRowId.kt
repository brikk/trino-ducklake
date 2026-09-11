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

import io.trino.spi.block.Block
import io.trino.spi.block.RowBlock
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.RowType
import io.trino.spi.type.TypeUtils.writeNativeValue

/** Worker-exchanged mutation locator, matching upstream's file identity + physical position. */
object DucklakeMergeRowId {
    val TYPE: RowType = RowType.from(listOf(RowType.field("data_file_id", BIGINT), RowType.field("position", BIGINT)))

    data class Locator(val dataFileId: Long, val position: Long)

    fun block(dataFileId: Long, positions: Block): Block = RowBlock.fromFieldBlocks(
            positions.positionCount, arrayOf(
                    RunLengthEncodedBlock.create(writeNativeValue(BIGINT, dataFileId), positions.positionCount), positions))

    fun read(block: Block, position: Int): Locator {
        require(!block.isNull(position)) { "DuckLake mutation locator is null" }
        val row = TYPE.getObject(block, position)
        val index = row.rawIndex
        require(!row.getRawFieldBlock(0).isNull(index) && !row.getRawFieldBlock(1).isNull(index)) {
            "DuckLake mutation locator has a null file ID or position"
        }
        val result = Locator(BIGINT.getLong(row.getRawFieldBlock(0), index), BIGINT.getLong(row.getRawFieldBlock(1), index))
        require(result.dataFileId >= 0 && result.position >= 0) { "Negative DuckLake mutation locator: $result" }
        return result
    }
}
