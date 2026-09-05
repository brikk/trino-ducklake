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

import com.google.inject.Inject
import io.trino.filesystem.Location
import io.trino.filesystem.manager.FileSystemConfig
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException

/** Comparison keys only: never rewrite a catalog path or a filesystem deletion target. */
class DucklakeFilePathIdentity @Inject constructor(private val config: FileSystemConfig) {
    data class Key(val namespace: String, val path: String) {
        fun directoryPrefix(): Key = copy(path = if (path.isEmpty() || path.endsWith('/')) path else "$path/")

        fun isUnder(prefix: Key): Boolean = namespace == prefix.namespace && path.startsWith(prefix.path)
    }

    fun key(path: String): Key {
        val location = try {
            Location.of(path)
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(NOT_SUPPORTED, "DuckLake maintenance cannot safely compare an invalid location", e)
        }
        val scheme = location.scheme().orElse("")
        validateAuthority(location, scheme)
        val local = scheme == "file" || scheme == "local"
        if (config.isS3Enabled && scheme in setOf("s3", "s3a", "s3n")) {
            if (location.host().isEmpty || location.port().isPresent) {
                unsupported("S3 locations without a bucket or with a port")
            }
            // Object keys are literal: dots, repeated slashes, percent text, and case all matter.
            return Key("s3:${location.host().orElseThrow()}", location.path())
        }
        val relativePath = if (scheme.isEmpty()) path.removePrefix("/") else location.path()
        validateHierarchicalPath(relativePath)
        if (local && config.isLocalEnabled) {
            // Both schemes are routed through the same configured native-local root.
            return Key("local", relativePath)
        }
        val container = location.userInfo().map { "$it@" }.orElse("")
        val host = location.host().orElse("")
        val port = if (location.port().isPresent) ":${location.port().asInt}" else ""
        return Key("opaque:$scheme://$container$host$port", relativePath)
    }

    private fun validateAuthority(location: Location, scheme: String) {
        // Azure uses URI user-info for the container, not a credential.
        val azure = scheme in setOf("abfs", "abfss", "wasb", "wasbs")
        if (location.userInfo().isPresent && !azure) {
            unsupported("locations containing user information")
        }
        val local = scheme == "file" || scheme == "local"
        if (local && (location.host().isPresent || location.port().isPresent)) {
            unsupported("local locations containing an authority or port")
        }
    }

    private fun validateHierarchicalPath(path: String) {
        val noncanonical = path.startsWith('/') || path.contains("//") || path.contains('\\')
        val dotSegments = path.split('/').any { it == "." || it == ".." }
        if (noncanonical || dotSegments) {
            unsupported("noncanonical local/Hadoop paths; use one path spelling without dot segments or repeated separators")
        }
    }

    fun validateNamespaces(keys: Collection<Key>) {
        val namespaces = keys.mapTo(mutableSetOf()) { it.namespace }
        if (namespaces.size > 1 && namespaces.any { it.startsWith("opaque:") }) {
            unsupported("mixed Hadoop/unqualified or unsupported filesystem namespaces; " +
                    "use consistent path spellings because the active Hadoop filesystem identity is unavailable")
        }
    }

    fun validateListing(root: Key, entry: Key) {
        if (root.namespace != entry.namespace) {
            unsupported("a filesystem listing that changes the scan root's namespace")
        }
    }

    private fun unsupported(reason: String): Nothing =
            throw TrinoException(NOT_SUPPORTED, "DuckLake maintenance cannot safely compare $reason; no files were deleted")
}
