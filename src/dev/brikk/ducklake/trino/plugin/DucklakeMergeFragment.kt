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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.airlift.json.JsonCodec
import io.airlift.slice.Slice
import io.airlift.slice.Slices

@JvmRecord
data class DucklakeMergeFragment @JsonCreator constructor(
    @param:JsonProperty("kind") val kind: Kind,
    @param:JsonProperty("payload") val payload: String)
{
    enum class Kind { INSERT, DELETE }

    fun toSlice(): Slice = Slices.utf8Slice(codec.toJson(this))

    companion object {
        private val codec = JsonCodec.jsonCodec(DucklakeMergeFragment::class.java)

        fun fromSlice(fragment: Slice): DucklakeMergeFragment = codec.fromJson(fragment.bytes)
    }
}
