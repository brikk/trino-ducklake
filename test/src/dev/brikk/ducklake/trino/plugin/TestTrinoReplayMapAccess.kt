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

import dev.brikk.ducklake.corpus.GoldenComparator
import dev.brikk.ducklake.corpus.OracleAttachment
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

class TestTrinoReplayMapAccess {
    @TempDir
    lateinit var directory: Path

    @Test
    fun mapAndRowAccessesMatchDuckdbIncludingMissingKeysAndKeyCase() = withLake { connection, engine ->
        connection.createStatement().use { statement ->
            statement.execute("CREATE TABLE lake.t (id INTEGER, m MAP(VARCHAR, INTEGER), r STRUCT(i INTEGER), " +
                    "mm MAP(VARCHAR, MAP(VARCHAR, INTEGER)), mr MAP(VARCHAR, STRUCT(Name INTEGER)), " +
                    "rm STRUCT(Name MAP(VARCHAR, INTEGER)))")
            statement.execute("INSERT INTO lake.t VALUES " +
                    "(1, MAP {'i': 1, 'I': 2, 'a''b': 3}, {'i': 10}, MAP {'outer_key': MAP {'inner_key': 4}}, " +
                    "MAP {'Key': {'Name': 5}}, {'Name': MAP {'Key': 6}}), " +
                    "(2, MAP {'j': 3}, {'i': 20}, NULL, NULL, NULL), " +
                    "(3, NULL, NULL, NULL, NULL, NULL)")
            statement.execute("CALL ducklake_flush_inlined_data('lake')")
        }
        for (sql in listOf(
                "SELECT id FROM lake.t WHERE m.i = 1",
                "SELECT id FROM lake.t WHERE m.i = 100",
                "SELECT m.i, m.I, m.\"a'b\", 'm.I' FROM lake.t ORDER BY id",
                "SELECT r.i FROM lake.t ORDER BY id",
                "SELECT a.m.i, b.r.i FROM lake.t a JOIN lake.t b ON a.id = b.id ORDER BY a.id",
                "SELECT mm.outer_key.inner_key, mr.Key.Name, rm.Name.Key FROM lake.t ORDER BY id",
                "WITH q AS (SELECT id, mm.outer_key AS m FROM lake.t) SELECT q.m.inner_key FROM q ORDER BY id")) {
            assertMirror(connection, engine, sql)
        }
        assertThat(engine.executeQuery("SELECT m.I FROM lake.t WHERE id = 1")).isEqualTo(listOf(listOf("2")))
    }

    @Test
    fun identicalSqlRefreshesTypesAfterDdlAndLakeReconnect() = withLake { connection, engine ->
        val sql = "SELECT s.i FROM lake.t"
        connection.createStatement().use { statement ->
            statement.execute("CREATE TABLE lake.t (s MAP(VARCHAR, INTEGER))")
            statement.execute("INSERT INTO lake.t VALUES (MAP {'i': 7})")
        }
        assertMirror(connection, engine, sql)
        assertThat(engine.executeQuery(sql)).isEqualTo(listOf(listOf("7")))
        connection.createStatement().use { statement ->
            statement.execute("DROP TABLE lake.t")
            statement.execute("CREATE TABLE lake.t (s STRUCT(i INTEGER))")
            statement.execute("INSERT INTO lake.t VALUES ({'i': 8})")
        }
        assertMirror(connection, engine, sql)
        assertThat(engine.executeQuery(sql)).isEqualTo(listOf(listOf("8")))
        connection.createStatement().use { it.execute("DETACH lake") }
        attach(connection, engine, "second")
        connection.createStatement().use { statement ->
            statement.execute("CREATE TABLE lake.t (s MAP(VARCHAR, INTEGER))")
            statement.execute("INSERT INTO lake.t VALUES (MAP {'i': 9})")
        }
        assertMirror(connection, engine, sql)
        assertThat(engine.executeQuery(sql)).isEqualTo(listOf(listOf("9")))
    }

    private fun withLake(action: (Connection, TrinoReplayEngine) -> Unit) {
        TrinoReplayEngine().use { engine ->
            DriverManager.getConnection("jdbc:duckdb:").use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("INSTALL ducklake")
                    statement.execute("LOAD ducklake")
                    statement.execute("INSTALL postgres")
                    statement.execute("LOAD postgres")
                }
                attach(connection, engine, "first")
                action(connection, engine)
            }
        }
    }

    private fun attach(connection: Connection, engine: TrinoReplayEngine, name: String) {
        val uri = engine.metadataRewriter(name)
        val dataPath = directory.resolve(name).toString()
        connection.createStatement().use { statement ->
            statement.execute("ATTACH 'ducklake:$uri' AS lake (DATA_PATH '$dataPath')")
            statement.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        }
        engine.connect(OracleAttachment(uri, dataPath, "lake"))
    }

    private fun assertMirror(connection: Connection, engine: TrinoReplayEngine, sql: String) {
        val expected = connection.createStatement().use { statement ->
            statement.executeQuery(sql).use { GoldenComparator.readRows(it) }
        }
        assertThat(engine.accepts(sql)).`as`("query must be mirrored, not skipped: %s", sql).isTrue()
        assertThat(engine.executeQuery(sql)).`as`(sql).isEqualTo(expected)
    }
}
