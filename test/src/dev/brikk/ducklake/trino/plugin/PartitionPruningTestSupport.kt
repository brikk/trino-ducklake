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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeTable

/** A table of the shared test catalog's `test_schema` at [snapshotId]. */
internal fun testSchemaTable(catalog: DucklakeCatalog, snapshotId: Long, tableName: String): DucklakeTable {
    val schema = catalog.listSchemas(snapshotId).first { it.schemaName == "test_schema" }
    return catalog.listTables(schema.schemaId, snapshotId).first { it.tableName == tableName }
}
