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
import dev.brikk.ducklake.corpus.ReplaySetupFormat
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

class TestTrinoNativeSnapshotReplay {
    @Test
    fun originalNativeSnapshotFixtureHasRealTrinoCoverageWithoutSqlOrGoldenChanges() {
        val root = Path.of(System.getProperty("ducklake.corpus.root", "ducklake/test/sql"))
        val fixture = root.resolve("virtualcolumns/ducklake_snapshot_id.test")
        val before = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))
        TrinoReplayEngine().use { engine ->
            val report = CorpusRunner(root, engine = engine, metadataRewriter = engine.metadataRewriter,
                setupFormat = ReplaySetupFormat.DUCKLAKE_1_0,
                mirrorFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                executionFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0)).run(listOf(fixture))
            assertThat(report.failures).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.crashes).withFailMessage { report.summary() }.isEmpty()
            assertThat(report.skippedFiles).isEmpty()
            report.requireMirrorPasses(2)
            assertThat(report.totalMirrorFailures).isZero()
            assertThat(report.totalOracleQueryPasses).isEqualTo(3)
            println(report.summary())
        }
        assertThat(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(fixture))).isEqualTo(before)
    }
}
