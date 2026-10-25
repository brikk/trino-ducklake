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

/** No candidate symbols in the released 0.12 lane. Admission remains fail closed. */
internal fun candidateReadAdapter(catalog: JdbcDucklakeCatalog, config: DucklakeConfig): DucklakeCatalog {
    check(!config.isEnableFinal11Reads()) { "Final-1.1 planning requires the explicitly selected catalog candidate build" }
    return catalog
}
