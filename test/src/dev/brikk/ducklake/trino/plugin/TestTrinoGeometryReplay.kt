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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class TestTrinoGeometryReplay {
    @TempDir
    lateinit var directory: Path

    @Test
    fun nestedGeometryPassesGoldenAndActualTrinoMirrorComparison() {
        val root = Path.of(System.getProperty("ducklake.corpus.root", "ducklake/test/sql"))
        TrinoReplayEngine().use { engine ->
            for ((kind, table) in mapOf("list" to "t1", "map" to "t3", "struct" to "t2")) {
                // The original fixtures USE ducklake, then query bare table names. The adapter's
                // general bare-name gate cannot distinguish those from oracle-local temp tables.
                // Explicitly qualify this known lake read in a test-local copy, preserving all
                // upstream setup and goldens, so this regression proves actual Trino comparison.
                val source = Files.readString(root.resolve("geo/ducklake_geometry_nested_$kind.test"))
                val originalQuery = "select * from $table;"
                val qualifiedQuery = "select * from ducklake.$table;"
                assertThat(source).contains(originalQuery)
                val fixture = directory.resolve("$kind.test")
                Files.writeString(fixture, source.replace(originalQuery, qualifiedQuery))
                var mirrored = false
                val executed = mutableListOf<String>()
                val observer = object : ReplayReadEngine by engine {
                    override fun executeQuery(sql: String): List<List<String?>> {
                        val result = engine.executeQuery(sql)
                        executed.add(sql)
                        if (sql.trim().removeSuffix(";").equals(qualifiedQuery.removeSuffix(";"), ignoreCase = true)) mirrored = true
                        return result
                    }
                }
                val runner = CorpusRunner(directory, emptyMap(), engine = observer, metadataRewriter = engine.metadataRewriter)
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
