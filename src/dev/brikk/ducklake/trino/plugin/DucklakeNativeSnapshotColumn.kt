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

import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.TypeUtils.writeNativeValue
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.apache.parquet.schema.Type.Repetition.REPEATED

/** Native snapshot_id is a per-row insert snapshot, identified by its reserved Parquet field ID. */
internal object DucklakeNativeSnapshotColumn {
    fun resolve(message: MessageColumnIO): ColumnIO? {
        val matches = (0 until message.childrenCount).map { message.getChild(it) }
            .filter { it.type.id?.intValue() == DucklakeDeleteFileReader.SNAPSHOT_ID_PARQUET_FIELD_ID }
        if (matches.size > 1) throw TrinoException(GENERIC_INTERNAL_ERROR, "Ambiguous native snapshot_id field ID in Parquet footer")
        val column = matches.singleOrNull() ?: return null
        requireSnapshotType(column)
        return column
    }

    private fun requireSnapshotType(column: ColumnIO) {
        val type = column.type
        if (!type.isPrimitive || type.asPrimitiveType().primitiveTypeName != INT64 || type.repetition == REPEATED)
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Native snapshot_id field must be a scalar INT64")
        if (!isSnapshotAnnotation(type.logicalTypeAnnotation))
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Native snapshot_id field has an incompatible logical type")
    }

    private fun isSnapshotAnnotation(annotation: LogicalTypeAnnotation?): Boolean = when (annotation) {
        null -> true
        is IntLogicalTypeAnnotation -> annotation.bitWidth == 64 && annotation.isSigned
        else -> false
    }

    fun fallback(beginSnapshot: Long): Block {
        if (beginSnapshot < 0) throw TrinoException(GENERIC_INTERNAL_ERROR, "Negative file begin snapshot for native snapshot_id")
        return writeNativeValue(BIGINT, beginSnapshot)
    }

    fun validate(block: Block): Block {
        for (position in 0 until block.positionCount) {
            if (block.isNull(position) || BIGINT.getLong(block, position) < 0)
                throw TrinoException(GENERIC_INTERNAL_ERROR, "Native snapshot_id field contains a NULL or negative snapshot")
        }
        return block
    }
}
