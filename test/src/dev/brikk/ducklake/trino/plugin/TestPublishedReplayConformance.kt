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

import dev.brikk.ducklake.corpus.GoldenComparator
import dev.brikk.ducklake.slt.SltQuery
import dev.brikk.ducklake.slt.SortMode
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.sql.DriverManager

/** Public released replay APIs on the consumer's actual test JVM; no copied implementation. */
class TestPublishedReplayConformance {
    @Test
    fun requestedTestVmMatchesTheActualRuntime() {
        assertThat(Runtime.version().feature()).isEqualTo(System.getProperty("ducklake.test.java-version", "25").toInt())
    }

    @Test
    fun floatAndDoubleTextMatchesPinnedNativeBitsAndRendering() {
        val doubles = listOf(1e23, Math.nextDown(1e23), Math.nextUp(1e23), Double.MIN_VALUE,
            -Double.MIN_VALUE, -0.0, java.lang.Double.MIN_NORMAL, Double.MAX_VALUE,
            Double.fromBits(0xfff8000000000001UL.toLong()), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
        val floats = listOf(java.lang.Float.MIN_NORMAL, Float.MIN_VALUE, -Float.MIN_VALUE, -0.0f,
            Float.fromBits(0xca762bff.toInt()), Float.MAX_VALUE, Float.fromBits(0xffc00001.toInt()))
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("PRAGMA version").use { rows ->
                    assertThat(rows.next()).isTrue()
                    assertThat(rows.getString(2)).isEqualTo("d54cbbe3df")
                }
            }
            connection.prepareStatement("SELECT ?::DOUBLE,?::FLOAT,CAST(?::DOUBLE AS VARCHAR),CAST(?::FLOAT AS VARCHAR)").use { query ->
                doubles.forEachIndexed { index, double ->
                    val float = floats[index % floats.size]
                    query.setDouble(1, double); query.setFloat(2, float)
                    query.setDouble(3, double); query.setFloat(4, float)
                    query.executeQuery().use { rows ->
                        assertThat(rows.next()).isTrue()
                        if (!double.isNaN()) assertThat(rows.getDouble(1).toRawBits()).isEqualTo(double.toRawBits())
                        if (!float.isNaN()) assertThat(rows.getFloat(2).toRawBits()).isEqualTo(float.toRawBits())
                        assertThat(GoldenComparator.renderCell(double)).isEqualTo(rows.getString(3))
                        assertThat(GoldenComparator.renderCell(float)).isEqualTo(rows.getString(4))
                    }
                }
            }
        }
        assertThat(GoldenComparator.renderCell(1e23)).isEqualTo("1e+23")
        assertThat(GoldenComparator.renderCell(Double.MIN_VALUE)).isEqualTo("5e-324")
        assertThat(GoldenComparator.renderCell(java.lang.Float.MIN_NORMAL)).isEqualTo("1.1754944e-38")
        assertThat(GoldenComparator.renderCell(Float.fromBits(0xca762bff.toInt()))).isEqualTo("-4033279.75")
    }

    @Test
    fun typedBooleanHashesDoNotChangeRawCellsOrVarcharLookalikes() {
        val query = SltQuery(1, "SELECT TRUE", "I", SortMode.NOSORT, null, null, emptyList())
        val original = listOf(listOf<String?>("true"))
        assertThat(GoldenComparator.resultHash(query, original, listOf("BOOLEAN")))
            .isEqualTo("1 values hashing to b026324c6904b2a9cb4b88d6d61c81d1")
        assertThat(GoldenComparator.resultHash(query, original, listOf("VARCHAR")))
            .isEqualTo("1 values hashing to 74d9a83219cabaab06a69fd318873f33")
        assertThat(GoldenComparator.resultHash(query, original))
            .isEqualTo("1 values hashing to 74d9a83219cabaab06a69fd318873f33")
        assertThat(original).containsExactly(listOf("true"))
        assertThat(GoldenComparator.renderCell(true)).isEqualTo("true")
        assertThatThrownBy { GoldenComparator.resultHash(query, listOf(listOf("garbage")), listOf("BOOLEAN")) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun booleanMetadataIsAppliedBeforeAllSortModes() {
        val rows = listOf(listOf("true", "9"), listOf("false", "true"), listOf(null, "false"))
        val digests = mapOf(SortMode.NOSORT to "5efb26c1b2452e75ae993e36f7931249",
            SortMode.ROWSORT to "3caa583fb6e97db58d6e459e0be3ba32", SortMode.VALUESORT to "6d544ba3f4e91363e4c718e42dd827a8")
        digests.forEach { (sort, digest) ->
            val query = SltQuery(1, "SELECT b,t", "IT", sort, null, null, emptyList())
            assertThat(GoldenComparator.resultHash(query, rows, listOf("BOOLEAN", "VARCHAR")))
                .isEqualTo("6 values hashing to $digest")
        }
    }
}
