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
import dev.brikk.ducklake.corpus.OracleAttachment
import dev.brikk.ducklake.corpus.ReplayReadEngine
import dev.brikk.ducklake.corpus.ReplaySetupFormat
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

class TestTrinoReplayBackends {
    @Test
    fun postgresMirrorBindsFixtureMetadataToActualSchemaWithoutSkippingTheFile() {
        val root = Path.of(System.getProperty("ducklake.corpus.root", "ducklake/test/sql"))
        val fixture = root.resolve("view/view_null_column_aliases.test")
        val before = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))
        TrinoReplayEngine().use { engine ->
            var tableRead = false
            val connectFailures = mutableListOf<String>()
            val observer = object : ReplayReadEngine by engine {
                override fun connect(attachment: OracleAttachment) {
                    try { engine.connect(attachment) }
                    catch (failure: RuntimeException) {
                        connectFailures.add(failure.stackTraceToString())
                        throw failure
                    }
                }
                override fun executeQuery(sql: String): List<List<String?>> {
                    if (!tableRead) {
                        assertThat(engine.executeQuery("SELECT a FROM dl.t")).containsExactly(listOf("42"))
                        tableRead = true
                    }
                    return engine.executeQuery(sql)
                }
            }
            val report = CorpusRunner(root, engine = observer, metadataRewriter = engine.metadataRewriter,
                setupFormat = ReplaySetupFormat.DUCKLAKE_1_0,
                mirrorFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                executionFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                oracleSqlBinder = engine.oracleSqlBinder).run(listOf(fixture))
            assertThat(report.failures).withFailMessage { report.summary() + connectFailures.joinToString("\n") }.isEmpty()
            assertThat(report.crashes).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.skippedFiles).isEmpty()
            assertThat(report.totalOracleQueryPasses).isEqualTo(5)
            assertThat(tableRead).isTrue()
            // Three native-view reads remain unsupported; two oracle metadata queries are not lake relations.
            // All five native goldens execute, and the independent real Trino table read above must succeed.
            assertThat(report.totalMirrorAttempts).isEqualTo(3)
            assertThat(report.totalMirrorFailures).isZero()
            assertThat(report.totalMirrorSkips).withFailMessage { report.summary() }.isEqualTo(5)
        }
        assertThat(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))).isEqualTo(before)
    }
}
