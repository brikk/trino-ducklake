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

import io.trino.spi.type.Type
import io.trino.spi.type.TypeManager

/** Released 0.12 has no public signature parser; retain its existing consumer path unchanged. */
@Suppress("UnusedParameter", "FunctionOnlyReturningConstant") // Versioned API hook; 0.12 has no shared parser.
internal fun catalogCompositeType(signature: String, typeManager: TypeManager, scalar: (String) -> Type): Type? = null
