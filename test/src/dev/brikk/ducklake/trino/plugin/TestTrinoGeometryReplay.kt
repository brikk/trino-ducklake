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

import dev.brikk.ducklake.corpus.CorpusRunner
import dev.brikk.ducklake.corpus.ReplayReadEngine
import dev.brikk.ducklake.corpus.ReplaySetupFormat
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class TestTrinoGeometryReplay {
    @Test
    fun nestedGeometryPassesGoldenAndActualTrinoMirrorComparison() {
        val root = Path.of(System.getProperty("ducklake.corpus.root", "ducklake/test/sql"))
        TrinoReplayEngine().use { engine ->
            for ((kind, table) in mapOf("list" to "t1", "map" to "t3", "struct" to "t2")) {
                // The upstream fixtures `USE ducklake`, then read the table by its bare name.
                val fixture = root.resolve("geo/ducklake_geometry_nested_$kind.test")
                val bareQuery = "select * from $table"
                assertThat(Files.readString(fixture)).contains("$bareQuery;")
                var mirrored = false
                val executed = mutableListOf<String>()
                val observer = object : ReplayReadEngine by engine {
                    override fun executeQuery(sql: String): List<List<String?>> {
                        val result = engine.executeQuery(sql)
                        executed.add(sql)
                        if (sql.trim().removeSuffix(";").equals(bareQuery, ignoreCase = true)) mirrored = true
                        return result
                    }
                }
                engine.beginFile()
                val runner = CorpusRunner(root, emptyMap(), engine = observer, metadataRewriter = engine.metadataRewriter,
                    setupFormat = ReplaySetupFormat.DUCKLAKE_1_0,
                    mirrorFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                    executionFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                    oracleSqlBinder = engine.oracleSqlBinder)
                val report = runner.run(listOf(fixture))
                assertThat(report.failures).withFailMessage { report.summary() }.isEmpty()
                assertThat(report.crashes).withFailMessage { report.summary() }.isEmpty()
                // A green oracle or an engine-skip is insufficient: the affected query must have
                // returned actual Trino rows, followed by the shared driver's comparison above.
                assertThat(mirrored).withFailMessage {
                    "Trino must execute the nested $kind geometry query; executed=$executed; ${report.summary()}"
                }.isTrue()
            }
        }
    }
}
