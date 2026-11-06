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

import dev.brikk.ducklake.corpus.CorpusReport
import dev.brikk.ducklake.corpus.CorpusRunner
import dev.brikk.ducklake.corpus.FileResult
import dev.brikk.ducklake.corpus.ReplaySetupFormat
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Corpus replay with the Trino engine mirroring lake reads (live-vs-live
 * against the DuckDB oracle) on the PostgreSQL backend axis.
 *
 * Every corpus file's DuckLake ATTACH is rewritten onto an isolated PG
 * database ([TrinoReplayEngine.metadataRewriter]); the oracle executes the
 * file verbatim against it; each golden-validated, `accepts`-gated query is
 * re-executed through Trino and compared row-for-row against the oracle's
 * live result. A failure here means Trino read the same catalog state
 * differently than DuckDB — exactly the class of bug this harness exists for.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestTrinoCorpusReplay {

    private val corpusRoot: Path =
        Path.of(
            System.getProperty("ducklake.corpus.root")
                ?: "../ducklake-corpus-replay/ducklake/test/sql",
        )

    /**
     * Per-file skips: upstream's own PostgreSQL-axis skip set (their PG CI
     * config — includes the PG-locking files that HANG a replay) plus
     * Trino-mirror-specific entries.
     */
    private val skipList: Map<String, String> =
        dev.brikk.ducklake.corpus.PostgresAxisSkips.SKIPS + mapOf(
            "types/floats.test" to
                "DuckDB orders NaN above every number in comparisons, so `WHERE f > 1` counts the NaN " +
                "row; Trino follows IEEE semantics where NaN > 1 is false and returns one row fewer",
        )

    @BeforeAll
    fun setup() {
        val ducklakeOk =
            runCatching {
                DriverManager.getConnection("jdbc:duckdb:").use { c ->
                    c.createStatement().use {
                        it.execute("INSTALL ducklake")
                        it.execute("LOAD ducklake")
                        it.execute("INSTALL postgres")
                        it.execute("LOAD postgres")
                    }
                }
            }.isSuccess
        assumeTrue(ducklakeOk, "ducklake/postgres extensions not installable (network?)")
        assumeTrue(Files.isDirectory(corpusRoot), "corpus submodule not initialized at $corpusRoot")
    }

    @Test
    fun `corpus lake reads mirror through trino`() {
        val discovery = CorpusRunner(corpusRoot, skipList)
        val dirsProp = System.getProperty("ducklake.corpus.dirs", "general,catalog,time_travel,view")
        val files =
            if (dirsProp == "all") {
                discovery.discover()
            } else {
                dirsProp.split(',').flatMap { discovery.discover(it.trim()) }
            }
        assertThat(files).isNotEmpty()

        // Fresh engine (runner + PG server) per chunk: each Trino catalog holds
        // a connection pool, so hundreds of per-file catalogs in one runner
        // exhaust PostgreSQL's max_connections. Chunking bounds the blast
        // radius and keeps memory flat on full-corpus runs.
        val results = replay(files)
        val report = CorpusReport(results)
        println(report.summary(maxFailures = 40))
        val reportDirectory = Files.createDirectories(Path.of("build", "reports", "corpus-replay"))
        Files.writeString(reportDirectory.resolve("trino-summary.txt"), report.summary(maxFailures = 40))

        assertThat(report.failures)
            .withFailMessage { "trino corpus-mirror failures:\n" + report.summary(maxFailures = 40) }
            .isEmpty()
        assertThat(report.crashes)
            .withFailMessage { "harness crashes:\n" + report.summary(maxFailures = 40) }
            .isEmpty()
        assertThat(report.totalPassed).isGreaterThan(50)
        report.requireMirrorPasses(1)
    }

    private fun replay(files: List<Path>): List<FileResult> =
        files.chunked(CHUNK_SIZE).flatMap { chunk ->
            TrinoReplayEngine().use { engine ->
                CorpusRunner(corpusRoot, skipList, engine = engine, metadataRewriter = engine.metadataRewriter,
                    setupFormat = ReplaySetupFormat.DUCKLAKE_1_0,
                    mirrorFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                    executionFormatScope = setOf(ReplaySetupFormat.DUCKLAKE_1_0),
                    oracleSqlBinder = engine.oracleSqlBinder).run(chunk).files
            }
        }

    companion object {
        private const val CHUNK_SIZE = 30
    }
}
