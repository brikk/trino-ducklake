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
import dev.brikk.ducklake.corpus.ReplayOracleSqlBinder
import dev.brikk.ducklake.corpus.ReplayReadEngine
import dev.brikk.ducklake.corpus.ReplaySetupFormat
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

class TestTrinoFormatScopes {
    @Test
    fun explicit11FixtureCannotRunItsFeatureSqlOrConnectA11ReaderInThe10Lane() {
        val root = Path.of(System.getProperty("ducklake.corpus.root", "ducklake/test/sql"))
        val fixture = root.resolve("catalog/nested_schema/core.test")
        val before = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))
        TrinoReplayEngine().use { engine ->
            var connections = 0
            val executedSql = mutableListOf<String>()
            val observer = object : ReplayReadEngine by engine {
                override fun connect(attachment: OracleAttachment) {
                    connections++
                    engine.connect(attachment)
                }
            }
            val binder = ReplayOracleSqlBinder { sql, context ->
                executedSql.add(sql)
                engine.oracleSqlBinder.bind(sql, context)
            }
            val report = CorpusRunner(root, engine = observer, metadataRewriter = engine.metadataRewriter,
                setupFormat = ReplaySetupFormat.DUCKLAKE_1_0,
                mirrorFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                executionFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0), oracleSqlBinder = binder).run(listOf(fixture))
            assertThat(report.failures).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.ranFiles).isEmpty()
            assertThat(report.haltedFiles).hasSize(1)
            assertThat(report.files.single().fileSkipReason).contains("format-out-of-scope", "stored DuckLake 1.1")
            assertThat(connections).isZero()
            assertThat(report.totalMirrorPasses).isZero()
            assertThat(executedSql.any { it.trimStart().startsWith("ATTACH", ignoreCase = true) }).isTrue()
            assertThat(executedSql.any { it.contains("CREATE SCHEMA", ignoreCase = true) }).isFalse()
        }
        assertThat(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))).isEqualTo(before)
    }
}
