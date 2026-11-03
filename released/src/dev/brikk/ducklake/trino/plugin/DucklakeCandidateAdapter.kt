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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException

/** Normal 1.0 lane; upgrading the library does not qualify final-1.1 planning. */
internal fun candidateReadAdapter(config: DucklakeConfig, openCatalog: () -> JdbcDucklakeCatalog): DucklakeCatalog {
    if (config.getCatalogFormatVersion() != "1.0") throw TrinoException(NOT_SUPPORTED,
        "This connector build accepts DuckLake format 1.0; format 1.1 requires the separately qualified read-only build")
    return openCatalog()
}
