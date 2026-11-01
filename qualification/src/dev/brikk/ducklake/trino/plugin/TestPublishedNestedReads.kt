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

import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import io.trino.testing.connector.TestingConnectorSession.SESSION
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.function.Supplier

/** N03 actual metadata and two-worker SQL, with separately expected names/values and route witnesses. */
@Execution(ExecutionMode.SAME_THREAD)
class TestPublishedNestedReads {
    @Test fun localLegacyInline(): Unit = read("1.0", false, true)
    @Test fun localLegacyParquet(): Unit = read("1.0", false, false)
    @Test fun postgresLegacyInline(): Unit = read("1.0", true, true)
    @Test fun postgresLegacyParquet(): Unit = read("1.0", true, false)
    @Test fun localFinalInline(): Unit = read("1.1", false, true)
    @Test fun localFinalParquet(): Unit = read("1.1", false, false)
    @Test fun postgresFinalInline(): Unit = read("1.1", true, true)
    @Test fun postgresFinalParquet(): Unit = read("1.1", true, false)

    private val names = listOf("MiXeD", "a,b", "x:y", "a<b>", "q\"uote", "back\\slash", " spaced ", "雪", "NULL")

    private fun read(spec: String, postgres: Boolean, inline: Boolean): Unit = Fixture(spec, postgres).use { fixture ->
        fixture.native("""
            CALL lake.set_option('data_inlining_row_limit',${if (inline) 100 else 0});
            CREATE TABLE lake.t(id INTEGER,
              s STRUCT("MiXeD" INTEGER,"a,b" VARCHAR,"x:y" VARCHAR,"a<b>" DECIMAL(10,2),
                "q""uote" VARCHAR,"back\slash" VARCHAR," spaced " VARCHAR,"雪" VARCHAR,"NULL" VARCHAR),
              xs STRUCT("List,Value" INTEGER,"List:Text" VARCHAR)[],
              m MAP(VARCHAR,STRUCT("Map:Value" DECIMAL(10,2),"Map,Text" VARCHAR)));
            INSERT INTO lake.t VALUES
              (1, struct_pack("MiXeD":=7,"a,b":='comma',"x:y":='colon',"a<b>":=12.34,
                  "q""uote":='quote',"back\slash":='slash'," spaced ":='space',"雪":='雪-value',"NULL":='NULL'),
                [struct_pack("List,Value":=11,"List:Text":='NULL'),NULL],
                map(['k','nil'],[struct_pack("Map:Value":=56.78,"Map,Text":='map'),NULL])),
              (2,NULL,NULL,NULL),
              (3,struct_pack("MiXeD":=NULL,"a,b":=NULL,"x:y":='',"a<b>":=NULL,
                  "q""uote":=NULL,"back\slash":=NULL," spaced ":=NULL,"雪":=NULL,"NULL":='NULL'),[],map());
        """.trimIndent())
        val before = fixture.metadata(inline, names)
        fixture.runner().use { runner ->
            assertTypes(runner, names)
            val sql = """SELECT id,s IS NULL,s."MiXeD",s."a,b",s."x:y",s."a<b>",s."q""uote",s."back\slash",
                s." spaced ",s."雪",s."NULL",cardinality(xs),(element_at(xs,1))."List,Value",
                (element_at(xs,1))."List:Text",cardinality(m),(element_at(m,'k'))."Map:Value",
                (element_at(m,'k'))."Map,Text" FROM t ORDER BY id""".trimIndent()
            assertThat(runner.execute(sql).materializedRows.map { it.fields }).containsExactly(
                listOf<Any?>(1,false,7,"comma","colon",BigDecimal("12.34"),"quote","slash","space","雪-value","NULL",
                    2L,11,"NULL",2L,BigDecimal("56.78"),"map"),
                listOf<Any?>(2,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null),
                listOf<Any?>(3,false,null,null,"",null,null,null,null,null,"NULL",0L,null,null,0L,null,null))

        }
        // The single-process local DuckDB JDBC fixture cannot attach the same metadata file
        // under another catalog name while the reader pool holds it. DDL runs between readers;
        // this verifies retained identity/history, not concurrent DDL or cache invalidation.
        fixture.native("""ALTER TABLE lake.t RENAME s."a,b" TO "Renamed:Field";""")
        val renamedNames = listOf("MiXeD", "Renamed:Field", "x:y", "a<b>", "q\"uote", "back\\slash", " spaced ", "雪", "NULL")
        assertThat(fixture.metadata(inline, renamedNames)).isEqualTo(before)
        fixture.runner().use { runner ->
            assertTypes(runner, renamedNames)
            assertThat(runner.execute("""SELECT id,s."Renamed:Field" FROM t ORDER BY id""").materializedRows.map { it.fields })
                .containsExactly(listOf<Any?>(1,"comma"), listOf<Any?>(2,null), listOf<Any?>(3,null))
            assertThatThrownBy { runner.execute("INSERT INTO t(id) VALUES(100)") }.hasMessageContaining("read-only")
        }
    }

    private fun assertTypes(runner: DistributedQueryRunner, expected: List<String>) {
        // TestingTrinoClient reconstructs result.types from protocol SQL text, canonicalizing
        // simple unquoted names. DESCRIBE carries metadata as VARCHAR and preserves exact names.
        val descriptions = runner.execute("DESCRIBE t").materializedRows.associate { it.getField(0) as String to it.getField(1) as String }
        expected.forEach { name -> assertThat(descriptions.getValue("s")).contains("\"${name.replace("\"", "\"\"")}\"") }
        assertThat(descriptions.getValue("xs")).contains("\"List,Value\"", "\"List:Text\"")
        assertThat(descriptions.getValue("m")).contains("\"Map:Value\"", "\"Map,Text\"")
    }

    private class Fixture(private val spec: String, postgres: Boolean) : AutoCloseable {
        private val root = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir", "/tmp/opencode")), "trino-published-n03-")
        private val pg = if (postgres) TestingDucklakePostgreSqlCatalogServer() else null
        private val db = root.resolve("metadata.db")
        private val data = Files.createDirectories(root.resolve("data"))
        private val config = DucklakeConfig().setCatalogDatabaseUrl(pg?.getJdbcUrl() ?: "jdbc:duckdb:$db")
            .setCatalogDatabaseUser(pg?.getUser()).setCatalogDatabasePassword(pg?.getPassword())
            .setDataPath(data.toString()).setEnableFinal11Reads(true)

        fun native(sql: String) = DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake; LOAD ducklake")
                statement.executeQuery("PRAGMA version").use { rows ->
                    check(rows.next() && rows.getString(2) == "d54cbbe3df")
                }
                if (pg != null) statement.execute("INSTALL postgres; LOAD postgres")
                val target = pg?.getDuckDbAttachUri() ?: "ducklake:$db"
                statement.execute("ATTACH '$target' AS lake(DATA_PATH '$data',DUCKLAKE_VERSION '$spec'," +
                    "AUTOMATIC_MIGRATION FALSE,METADATA_CATALOG 'meta')")
                statement.execute(sql)
                statement.execute("DETACH lake")
            }
        }

        fun metadata(inline: Boolean, expected: List<String>): List<Triple<Long, Long?, Long>> =
            JdbcDucklakeCatalog(config.toCatalogConfig().apply { enableFinal11Reads = true }).use { catalog ->
                catalog.readSession(Supplier {
                    val snapshot = catalog.currentSnapshotId
                    val table = catalog.getTable("main", "t", snapshot)!!
                    val projected = catalog.getTableColumns(table.tableId, snapshot)
                    val tree = catalog.getAllColumnsWithParentage(table.tableId, snapshot)
                    val struct = projected.single { it.columnName == "s" }
                    val converter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)
                    val parsed = converter.toTrinoType(struct.columnType) as RowType
                    assertThat(parsed.fields.map { it.name.orElseThrow() }).containsExactlyElementsOf(expected)
                    assertThat(parsed).isEqualTo(converter.toTrinoType(tree.single { it.columnId == struct.columnId }, tree))
                    assertThat(tree.filter { it.parentColumn == struct.columnId }.sortedBy { it.columnOrder }.map { it.columnName })
                        .containsExactlyElementsOf(expected)
                    val metadata = DucklakeMetadata(catalog, converter).getTableMetadata(SESSION,
                        DucklakeTableHandle("main", "t", table.tableId, snapshot))
                    val byName = metadata.columns.associate { it.name to it.type }
                    assertThat((byName.getValue("s") as RowType).fields.map { it.name.orElseThrow() })
                        .containsExactlyElementsOf(expected)
                    assertThat(((byName.getValue("xs") as ArrayType).elementType as RowType).fields.map { it.name.orElseThrow() })
                        .containsExactly("List,Value", "List:Text")
                    assertThat(((byName.getValue("m") as MapType).valueType as RowType).fields.map { it.name.orElseThrow() })
                        .containsExactly("Map:Value", "Map,Text")
                    val files = catalog.getDataFiles(table.tableId, snapshot)
                    val inlineInfos = catalog.getInlinedDataInfos(table.tableId, snapshot).filter { it.hasLiveRows }
                    if (inline) { assertThat(files).isEmpty(); assertThat(inlineInfos).isNotEmpty() }
                    else { assertThat(files).isNotEmpty(); assertThat(inlineInfos).isEmpty() }
                    tree.sortedBy { it.columnId }.map { Triple(it.columnId, it.parentColumn, it.columnOrder) }
                })
            }

        fun runner(): DistributedQueryRunner {
            val runner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("ducklake").setSchema("main").build())
                .setWorkerCount(2).addCoordinatorProperty("node-scheduler.include-coordinator", "false").build()
            try {
                runner.installPlugin(DucklakePlugin())
                val properties = mutableMapOf("ducklake.catalog.database-url" to config.getCatalogDatabaseUrl(),
                    "ducklake.data-path" to data.toString(), "fs.hadoop.enabled" to "true",
                    "ducklake.experimental.enable-final-1.1-reads" to "true", "ducklake.read.page-size" to "1")
                config.getCatalogDatabaseUser()?.let { properties["ducklake.catalog.database-user"] = it }
                config.getCatalogDatabasePassword()?.let { properties["ducklake.catalog.database-password"] = it }
                runner.createCatalog("ducklake", "ducklake", properties)
                return runner
            } catch (failure: Throwable) { runner.close(); throw failure }
        }

        override fun close() {
            try { pg?.close() }
            finally { Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } } }
        }
    }
}
