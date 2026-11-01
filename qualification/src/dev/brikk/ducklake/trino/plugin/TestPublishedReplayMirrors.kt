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

import dev.brikk.ducklake.corpus.CorpusRunner
import dev.brikk.ducklake.corpus.ReplayReadEngine
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/** H15/N19 through the actual Trino adapter. Oracle totals cannot satisfy these coverage gates. */
class TestPublishedReplayMirrors {
    @TempDir lateinit var directory: Path

    private val setup = """
        statement ok
        INSTALL ducklake

        statement ok
        LOAD ducklake

        statement ok
        ATTACH 'ducklake:{TEST_DIR}/published.db' AS lake (DATA_PATH '{TEST_DIR}/data', DUCKLAKE_VERSION '1.0', AUTOMATIC_MIGRATION FALSE)

        statement ok
        CREATE TABLE lake.t(id INTEGER,b BOOLEAN,txt VARCHAR,d DOUBLE,f FLOAT,xs DOUBLE[],s STRUCT(f FLOAT))

        statement ok
        INSERT INTO lake.t VALUES (0,TRUE,'9',1e23,'1.17549435e-38'::FLOAT,[1e23,NULL],{'f':'1.17549435e-38'::FLOAT}),(1,FALSE,'true','5e-324'::DOUBLE,'1e-45'::FLOAT,NULL,NULL),(2,NULL,'false','-0.0'::DOUBLE,-4033279.75::FLOAT,[],{'f':NULL})

    """.trimIndent() + "\n\n"

    private fun query(sql: String, types: String, expected: String, sort: String = "nosort", label: String = ""): String =
        "query $types $sort${if (label.isEmpty()) "" else " $label"}\n$sql\n----\n$expected\n\n"

    @Test
    fun booleanHashesFloatsNestedTextAndLabelsHavePositiveMeasuredTrinoCoverage() {
        val fixture = directory.resolve("published-values.test")
        val text = setup +
            query("SELECT b FROM lake.t ORDER BY id", "I", "3 values hashing to 184e6f979a6b9d76049c9b0ff19e1068") +
            query("SELECT b,txt FROM lake.t ORDER BY id", "IT", "6 values hashing to 5efb26c1b2452e75ae993e36f7931249") +
            query("SELECT b,txt FROM lake.t ORDER BY id", "IT", "6 values hashing to 3caa583fb6e97db58d6e459e0be3ba32", "rowsort", "typed") +
            query("SELECT b,txt FROM lake.t ORDER BY id DESC", "IT", "6 values hashing to 3caa583fb6e97db58d6e459e0be3ba32", "rowsort", "typed") +
            query("SELECT b,txt FROM lake.t ORDER BY id", "IT", "6 values hashing to 6d544ba3f4e91363e4c718e42dd827a8", "valuesort") +
            query("SELECT d,f FROM lake.t ORDER BY id", "RR", "1e+23\n1.1754944e-38\n5e-324\n1e-45\n-0.0\n-4033279.75") +
            query("SELECT xs,s FROM lake.t WHERE id=0", "TT", "[1e+23, NULL]\n{'f': 1.1754944e-38}") +
            query("SELECT txt FROM lake.t WHERE id=1", "T", "1 values hashing to 74d9a83219cabaab06a69fd318873f33")
        Files.writeString(fixture, text)
        TrinoReplayEngine().use { engine ->
            val calls = mutableListOf<String>()
            val observed = observing(engine, calls)
            val report = CorpusRunner(directory, emptyMap(), engine = observed, metadataRewriter = engine.metadataRewriter).run(listOf(fixture))
            assertThat(report.failures).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.crashes).withFailMessage { report.summary() }.isEmpty()
            report.requireMirrorPasses(8)
            assertThat(report.totalMirrorFailures).isZero()
            assertThat(report.totalMirrorSkips).withFailMessage { report.summary() }.isZero()
            assertThat(calls).hasSize(8)
            assertThat(engine.executeQuery("SELECT b,txt FROM lake.t WHERE id=0")).containsExactly(listOf("true", "9"))
            println(report.summary())
        }
    }

    @Test
    fun wrongHashFailsBeforeMirroringAndTheLaterValidRecordStillMirrors() {
        val fixture = directory.resolve("published-wrong-hash.test")
        Files.writeString(fixture, setup +
            query("SELECT b FROM lake.t WHERE id=0 /* wrong-java-hash */", "I", "1 values hashing to 74d9a83219cabaab06a69fd318873f33") +
            query("SELECT b FROM lake.t WHERE id=0 /* correct-native-hash */", "I", "1 values hashing to b026324c6904b2a9cb4b88d6d61c81d1"))
        TrinoReplayEngine().use { engine ->
            val calls = mutableListOf<String>()
            val report = CorpusRunner(directory, emptyMap(), engine = observing(engine, calls), metadataRewriter = engine.metadataRewriter)
                .run(listOf(fixture))
            assertThat(report.crashes).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.failures).hasSize(1)
            assertThat(report.failures.single().second.reason).contains("result hash")
            assertThat(report.totalOracleQueryPasses).isEqualTo(1)
            report.requireMirrorPasses(1)
            assertThat(report.totalMirrorAttempts).isEqualTo(1)
            assertThat(calls).singleElement().asString().contains("correct-native-hash").doesNotContain("wrong-java-hash")
        }
    }

    private fun observing(engine: TrinoReplayEngine, calls: MutableList<String>): ReplayReadEngine = object : ReplayReadEngine by engine {
        override fun executeQuery(sql: String): List<List<String?>> {
            calls.add(sql)
            return engine.executeQuery(sql)
        }
    }
}
