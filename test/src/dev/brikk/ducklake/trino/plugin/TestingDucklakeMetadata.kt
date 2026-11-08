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
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.json.JsonCodec

internal fun testingDucklakeMetadata(
    catalog: DucklakeCatalog,
    typeConverter: DucklakeTypeConverter,
    snapshotResolver: DucklakeSnapshotResolver = DucklakeSnapshotResolver(catalog, DucklakeConfig()),
): DucklakeMetadata = DucklakeMetadata(
    catalog,
    typeConverter,
    snapshotResolver,
    JsonCodec.jsonCodec(DucklakeWriteFragment::class.java),
    JsonCodec.jsonCodec(DucklakeDeleteFragment::class.java),
    DucklakePathResolver(catalog, DucklakeConfig()),
    DucklakeTemporalPartitionEncoding.CALENDAR,
)
