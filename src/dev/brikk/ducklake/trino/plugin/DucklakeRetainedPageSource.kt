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

import io.trino.spi.connector.ConnectorPageSource

/** Includes retained transform state that Trino's generic transform wrapper does not own. */
internal class DucklakeRetainedPageSource(private val delegate: ConnectorPageSource, private val retainedBytes: Long) :
        ConnectorPageSource by delegate {
    private var closed = false
    override fun getNextSourcePage() = delegate.nextSourcePage
    override fun getCompletedPositions() = delegate.completedPositions
    override fun getMetrics() = delegate.metrics
    override fun isBlocked() = delegate.isBlocked()
    override fun getMemoryUsage(): Long = if (closed) 0 else retainedBytes + delegate.memoryUsage
    override fun close() {
        try { delegate.close() } finally { closed = true }
    }
}
