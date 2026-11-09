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
import io.trino.client.ErrorLocation
import io.trino.client.FailureInfo
import io.trino.spi.StandardErrorCode
import io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND
import io.trino.spi.StandardErrorCode.TYPE_MISMATCH
import io.trino.spi.TrinoException
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
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
        for ((sql, types) in listOf("SELECT 123 = '123'" to "integer = varchar(3)",
            "SELECT '123' = 123" to "varchar(3) = integer")) {
            val column = sql.indexOf('=') + 1
            val error = failure(TYPE_MISMATCH, "line 1:$column: Cannot apply operator: $types", location = ErrorLocation(1, column))
            assertThat(classify(error, sql = sql)).isInstanceOf(ReplayEngineSkip::class.java)
        }
        for (message in listOf(
            "Cannot apply operator: bigint = varchar", "Cannot apply operator: bigint = boolean",
            "Cannot cast bigint to varchar(3)", "Cannot apply operator: bigint = boolean\nCannot apply operator: bigint = varchar(3)")) {
            val error = failure(TYPE_MISMATCH, message)
            assertThat(classify(error)).isSameAs(error)
        }
    }

    @Test
    fun boundedExpressionsCharAndUnrelatedLiteralsDoNotSkip() {
        val cases = listOf(
            "SELECT 1 = CAST('123' AS varchar(3))" to "integer = varchar(3)",
            "SELECT 1 = CAST('123' AS char(3))" to "integer = char(3)",
            "SELECT 'unused', 1 = v FROM (VALUES '123') t(v)" to "integer = varchar(3)",
            "SELECT 1 = substr('123', 1, 3)" to "integer = varchar(3)",
            "SELECT 1 = v FROM (SELECT '123' AS v)" to "integer = varchar(3)",
            "SELECT '123', 1 = 2" to "integer = varchar(3)",
            "SELECT 1 = '123'" to "integer = char(3)",
            "SELECT '123' = 1" to "integer = varchar(3)",
            "SELECT 1 = '123'" to "integer = varchar(4)")
        for ((sql, types) in cases) {
            val column = sql.indexOf('=') + 1
            val error = failure(TYPE_MISMATCH, "line 1:$column: Cannot apply operator: $types", location = ErrorLocation(1, column))
            assertThat(classify(error, sql = sql)).`as`(sql).isSameAs(error)
        }
        val sql = "SELECT 1 = '123', 1 = CAST('123' AS varchar(3))"
        val column = sql.lastIndexOf('=') + 1
        val error = failure(TYPE_MISMATCH, "line 1:$column: Cannot apply operator: integer = varchar(3)",
            location = ErrorLocation(1, column))
        assertThat(classify(error, sql = sql)).isSameAs(error)
    }

    @Test
    fun unprovableLiteralLocationsFailClosed() {
        val message = "line 1:10: Cannot apply operator: integer = varchar(3)"
        val missingLocation = failure(TYPE_MISMATCH, message)
        assertThat(classify(missingLocation, sql = "SELECT 1 = '123'")).isSameAs(missingLocation)
        val located = failure(TYPE_MISMATCH, message, location = ErrorLocation(1, 10))
        for (sql in listOf("not valid SQL", "SELECT 1 /* '123' */ = v", "SELECT 1 = v -- '123'", "SELECT 1 = \"'123'\"")) {
            assertThat(classify(located, sql = sql)).isSameAs(located)
        }
    }

    @Test
    fun realAnalyzerLocationsIdentifyOnlyTheFailingLiteralOperand() {
        DistributedQueryRunner.builder(testSessionBuilder().build()).build().use { runner ->
            for (sql in listOf("SELECT 1 = '1'", "SELECT 'it''s' = 1", "SELECT\n 1 = ('😀')", "SELECT 1 + '1'",
                "SELECT 1 > '1'", "SELECT 1 >= '1'", "SELECT 1 <> '1'")) {
                val error = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException::class.java) { runner.execute(sql) }
                assertThat(classify(error, sql = sql)).`as`(sql).isInstanceOf(ReplayEngineSkip::class.java)
            }
            for (sql in listOf("SELECT 'unused', 1 = CAST('1' AS varchar(1))", "SELECT 1 = CAST('1' AS char(1))")) {
                val error = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException::class.java) { runner.execute(sql) }
                assertThat(classify(error, sql = sql)).`as`(sql).isSameAs(error)
            }
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
            assertThat(TrinoReplayEngine.classifyEngineError(error, "replay", "SELECT 1") { _, _ -> error("must not query the lake") })
                .isSameAs(error)
        }
        val quoted = failure(TABLE_NOT_FOUND, "Table 'replay.\"a.b\".\"t\"\"q\"' does not exist")
        val names = mutableListOf<Pair<String, String>>()
        assertThat(TrinoReplayEngine.classifyEngineError(quoted, "replay", "SELECT 1") { schema, table ->
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
            TrinoReplayEngine.classifyEngineError(error, "replay", "SELECT 1") { _, _ -> throw lookupFailure }
        }.isSameAs(lookupFailure)
    }

    private fun classify(error: RuntimeException, history: List<String?>? = emptyList(), sql: String = "SELECT 1"): RuntimeException =
        TrinoReplayEngine.classifyEngineError(error, "replay", sql) { _, _ -> history }

    private fun failure(code: StandardErrorCode, message: String, stack: List<String> = listOf(ENGINE_FRAME),
        location: ErrorLocation? = null): RuntimeException {
        val error = code.toErrorCode()
        return FailureInfo("io.trino.spi.TrinoException", message, null, emptyList(), stack,
            ErrorInfo(error.code, error.name, error.type.name), location).toException()
    }

    private companion object {
        const val CONNECTOR_FRAME = "dev.brikk.ducklake.trino.plugin.DucklakeTypeConverter.toTrinoType(DucklakeTypeConverter.kt:1)"
        const val ENGINE_FRAME = "io.trino.parquet.reader.ColumnReaderFactory.create(ColumnReaderFactory.java:1)"
    }
}
