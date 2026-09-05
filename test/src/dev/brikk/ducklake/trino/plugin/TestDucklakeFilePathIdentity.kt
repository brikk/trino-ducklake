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

import io.airlift.configuration.ConfigurationFactory
import io.trino.filesystem.manager.FileSystemConfig
import io.trino.spi.TrinoException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeFilePathIdentity {
    private val native = DucklakeFilePathIdentity(FileSystemConfig()
            .setLocalEnabled(true).setS3Enabled(true))

    @Test
    fun nativeLocalAliasesShareTheRootRelativeNamespace() {
        val expected = native.key("local:///warehouse/ducklake-x.parquet")
        for (path in listOf("file:/warehouse/ducklake-x.parquet", "file:///warehouse/ducklake-x.parquet",
                "local:/warehouse/ducklake-x.parquet")) {
            assertThat(native.key(path)).isEqualTo(expected)
        }
        assertThat(native.key("file:///a%20b")).isNotEqualTo(native.key("file:///a b"))
        assertThat(native.key("file:///a%2Fb")).isNotEqualTo(native.key("file:///a/b"))
    }

    @Test
    fun nativeS3AliasesPreserveExactBucketAndKey() {
        val expected = native.key("s3://bucket/a//b/../ducklake-x%20.parquet")
        assertThat(native.key("s3a://bucket/a//b/../ducklake-x%20.parquet")).isEqualTo(expected)
        assertThat(native.key("s3n://bucket/a//b/../ducklake-x%20.parquet")).isEqualTo(expected)
        val keys = listOf("a/b", "a//b", "a/./b", "a/x/../b", "a%2Fb", "a%2fb", "a b", "a%20b", "a\\b")
        assertThat(keys.map { native.key("s3://bucket/$it") }.toSet()).hasSize(keys.size)
        assertThat(native.key("s3://BUCKET/a/b")).isNotEqualTo(native.key("s3://bucket/a/b"))
        assertThat(native.key("s3://other/a/b")).isNotEqualTo(native.key("s3://bucket/a/b"))
    }

    @Test
    fun directoryPrefixesRespectNamespaceAndSlashBoundaries() {
        for (directory in listOf("s3://bucket/ducklake-x.lance", "s3://bucket/ducklake-x.lance/")) {
            val prefix = native.key(directory).directoryPrefix()
            assertThat(native.key("s3a://bucket/ducklake-x.lance/data/part").isUnder(prefix)).isTrue()
            assertThat(native.key("s3://bucket/ducklake-x.lance2/data/part").isUnder(prefix)).isFalse()
            assertThat(native.key("s3://other/ducklake-x.lance/data/part").isUnder(prefix)).isFalse()
        }
        assertThat(native.key("local:///x").isUnder(native.key("file:///").directoryPrefix())).isTrue()
    }

    @Test
    fun canonicalFallbackPathsWorkWithinOneNamespace() {
        val fallback = DucklakeFilePathIdentity(FileSystemConfig())
        fallback.validateNamespaces(listOf(fallback.key("/warehouse/"), fallback.key("/warehouse/ducklake-x.parquet")))
        fallback.validateNamespaces(listOf(fallback.key("hdfs://namenode:8020/warehouse/"),
                fallback.key("hdfs://namenode:8020/warehouse/ducklake-x.parquet")))
        assertThat(fallback.key("file:/warehouse/x")).isEqualTo(fallback.key("file:///warehouse/x"))
    }

    @Test
    fun azureContainerIsPartOfTheOpaqueNamespace() {
        val root = native.key("abfss://container@account.dfs.core.windows.net/warehouse/")
        val file = native.key("abfss://container@account.dfs.core.windows.net/warehouse/x")
        native.validateNamespaces(listOf(root, file))
        assertThat(file.isUnder(root.directoryPrefix())).isTrue()
        val other = native.key("abfss://other@account.dfs.core.windows.net/warehouse/x")
        assertThat(other).isNotEqualTo(file)
        assertThatThrownBy { native.validateNamespaces(listOf(root, other)) }
                .hasMessageContaining("mixed Hadoop/unqualified")
    }

    @Test
    fun unresolvedNamespaceMixturesFailClosed() {
        for (paths in listOf(
                listOf("/warehouse/x", "file:///warehouse/x"),
                listOf("/root/warehouse/x", "local:///warehouse/x"),
                listOf("hdfs://one/warehouse/x", "hdfs://two/warehouse/x"))) {
            assertThatThrownBy { native.validateNamespaces(paths.map(native::key)) }
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("mixed Hadoop/unqualified")
                    .hasMessageContaining("no files were deleted")
        }
    }

    @Test
    fun noncanonicalAndInvalidLocationsFailClosed() {
        for (path in listOf("//warehouse/x", "/warehouse/../x", "file:///warehouse/./x",
                "file:////warehouse/x", "local:///warehouse//x", "file://localhost/warehouse/x",
                "file:///warehouse\\x", "s3://bucket:443/x", "s3://user@bucket/x")) {
            assertThatThrownBy { native.key(path) }
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("cannot safely compare")
        }
    }

    @Test
    fun unexpectedListingNamespaceFailsClosed() {
        native.validateListing(native.key("file:///warehouse"), native.key("local:///warehouse/x"))
        assertThatThrownBy { native.validateListing(native.key("s3://bucket/"), native.key("s3://other/x")) }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("listing that changes")
    }

    @Test
    fun currentAndLegacyEnablePropertiesUseTheSameRouting() {
        for (prefix in listOf("fs.", "fs.native-")) {
            ConfigurationFactory(mapOf("${prefix}local.enabled" to "true", "${prefix}s3.enabled" to "true")).use { factory ->
                val identity = DucklakeFilePathIdentity(factory.build(FileSystemConfig::class.java))
                assertThat(identity.key("file:///x")).isEqualTo(identity.key("local:///x"))
                assertThat(identity.key("s3://bucket/x")).isEqualTo(identity.key("s3a://bucket/x"))
            }
        }
    }
}
