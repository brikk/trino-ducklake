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

import dev.brikk.ducklake.corpus.ReplayEngineSkip
import io.trino.client.ErrorInfo
import io.trino.client.FailureInfo
import io.trino.spi.StandardErrorCode
import io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.StandardErrorCode.TYPE_MISMATCH
import io.trino.spi.TrinoException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestTrinoReplayEngineErrors {
    @Test
    fun onlyConnectorNotSupportedSkips() {
        val connector = failure(NOT_SUPPORTED, "capability rejected", listOf(CONNECTOR_FRAME))
        assertThat(classify(connector)).isInstanceOf(ReplayEngineSkip::class.java)

        for (stack in listOf(emptyList(), listOf(ENGINE_FRAME), listOf(ENGINE_FRAME, CONNECTOR_FRAME))) {
            val engine = failure(NOT_SUPPORTED, "Unsupported Trino column type", stack)
            assertThat(classify(engine)).isSameAs(engine)
        }
    }

    @Test
    fun unknownFunctionSkipsButWrongArgumentsFail() {
        assertThat(classify(failure(FUNCTION_NOT_FOUND, "line 1:8: Function 'missing' not registered")))
            .isInstanceOf(ReplayEngineSkip::class.java)
        val wrongTypes = failure(FUNCTION_NOT_FOUND, "Unexpected parameters (bigint) for function lower. Expected: lower(varchar(x))")
        assertThat(classify(wrongTypes)).isSameAs(wrongTypes)
    }

    @Test
    fun typeMismatchSkipsBoundedVarcharLiteralOperandsOnly() {
        for (message in listOf("Cannot apply operator: bigint = varchar(3)", "Cannot apply operator: varchar(3) = bigint")) {
            assertThat(classify(failure(TYPE_MISMATCH, message))).isInstanceOf(ReplayEngineSkip::class.java)
        }
        for (message in listOf(
            "Cannot apply operator: bigint = varchar", "Cannot apply operator: bigint = boolean",
            "Cannot cast bigint to varchar(3)", "Cannot apply operator: bigint = boolean\nCannot apply operator: bigint = varchar(3)")) {
            val error = failure(TYPE_MISMATCH, message)
            assertThat(classify(error)).isSameAs(error)
        }
    }

    @Test
    fun missingRelationSkipsOnlyWhenItWasNeverALakeTableOrTrinoView() {
        val missing = failure(TABLE_NOT_FOUND, "line 1:15: Table 'replay.main.t' does not exist")
        for (history in listOf(emptyList(), listOf("duckdb"), listOf("duckdb", "other/1"))) {
            assertThat(classify(missing, history)).isInstanceOf(ReplayEngineSkip::class.java)
        }
        // A null entry is a table, including a dropped table from an earlier snapshot.
        for (history in listOf(listOf(null), listOf("duckdb", null), listOf("trino"), listOf("TRINO/1"))) {
            assertThat(classify(missing, history)).isSameAs(missing)
        }
        assertThat(classify(missing, null)).isSameAs(missing)
    }

    @Test
    fun missingRelationMustBelongToTheConnectedCatalogAndHaveARecognizedName() {
        for (message in listOf("Table 'other.main.t' does not exist", "unrecognized missing table message")) {
            val error = failure(TABLE_NOT_FOUND, message)
            assertThat(TrinoReplayEngine.classifyEngineError(error, "replay") { _, _ -> error("must not query the lake") })
                .isSameAs(error)
        }
        val quoted = failure(TABLE_NOT_FOUND, "Table 'replay.\"a.b\".\"t\"\"q\"' does not exist")
        val names = mutableListOf<Pair<String, String>>()
        assertThat(TrinoReplayEngine.classifyEngineError(quoted, "replay") { schema, table ->
            names += schema to table
            emptyList()
        }).isInstanceOf(ReplayEngineSkip::class.java)
        assertThat(names).containsExactly("a.b" to "t\"q")
    }

    @Test
    fun everyOtherCodeFailsEvenWithAnOldSkipMessageAndConnectorStack() {
        val classifiedCodes = setOf(NOT_SUPPORTED, FUNCTION_NOT_FOUND, TYPE_MISMATCH, TABLE_NOT_FOUND)
        for (code in StandardErrorCode.entries.filterNot { it in classifiedCodes }) {
            val error = failure(code, "not supported; Function 'missing' not registered; Table 'replay.main.t' does not exist",
                listOf(CONNECTOR_FRAME))
            assertThat(classify(error)).`as`(code.name).isSameAs(error)
        }
    }

    @Test
    fun absentServerErrorInfoAndUnstructuredExceptionsFail() {
        val noCode = FailureInfo("io.trino.spi.TrinoException", "not supported", null,
            emptyList(), listOf(CONNECTOR_FRAME), null, null).toException()
        for (error in listOf(noCode, RuntimeException("does not exist"), TrinoException(NOT_SUPPORTED, "unsupported"))) {
            assertThat(classify(error)).isSameAs(error)
        }
        val wrappedSkip = RuntimeException("query failed", failure(FUNCTION_NOT_FOUND, "Function 'missing' not registered"))
        assertThat(classify(wrappedSkip)).isInstanceOf(ReplayEngineSkip::class.java)
        val wrappedFailure = RuntimeException("query failed", failure(GENERIC_INTERNAL_ERROR, "not supported"))
        assertThat(classify(wrappedFailure)).isSameAs(wrappedFailure)
    }

    @Test
    fun relationLookupFailureIsNotMistakenForAnAbsentTable() {
        val error = failure(TABLE_NOT_FOUND, "Table 'replay.main.t' does not exist")
        val lookupFailure = IllegalStateException("metadata unavailable")
        assertThatThrownBy {
            TrinoReplayEngine.classifyEngineError(error, "replay") { _, _ -> throw lookupFailure }
        }.isSameAs(lookupFailure)
    }

    private fun classify(error: RuntimeException, history: List<String?>? = emptyList()): RuntimeException =
        TrinoReplayEngine.classifyEngineError(error, "replay") { _, _ -> history }

    private fun failure(code: StandardErrorCode, message: String, stack: List<String> = listOf(ENGINE_FRAME)): RuntimeException {
        val error = code.toErrorCode()
        return FailureInfo("io.trino.spi.TrinoException", message, null, emptyList(), stack,
            ErrorInfo(error.code, error.name, error.type.name), null).toException()
    }

    private companion object {
        const val CONNECTOR_FRAME = "dev.brikk.ducklake.trino.plugin.DucklakeTypeConverter.toTrinoType(DucklakeTypeConverter.kt:1)"
        const val ENGINE_FRAME = "io.trino.parquet.reader.ColumnReaderFactory.create(ColumnReaderFactory.java:1)"
    }
}
