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

import org.roaringbitmap.longlong.Roaring64NavigableMap
import java.io.DataInput

/** Roaring32 containers per high word; the Set view is only for catalog/wire compatibility. */
class DucklakeLongSet private constructor(private val bitmap: Roaring64NavigableMap) : AbstractMutableSet<Long>() {
    constructor() : this(Roaring64NavigableMap(false, false))

    override val size: Int get() = bitmap.longCardinality.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    override fun isEmpty(): Boolean = bitmap.isEmpty
    override fun contains(element: Long): Boolean = bitmap.contains(element)
    fun containsLong(value: Long): Boolean = bitmap.contains(value)
    fun addLong(value: Long) = bitmap.addLong(value)
    override fun add(element: Long): Boolean {
        val present = bitmap.contains(element)
        bitmap.addLong(element)
        return !present
    }
    override fun remove(element: Long): Boolean {
        val present = bitmap.contains(element)
        bitmap.removeLong(element)
        return present
    }
    override fun clear() = bitmap.clear()
    override fun iterator(): MutableIterator<Long> {
        if (bitmap.longCardinality == 0L) return java.util.Collections.emptyIterator()
        val iterator = bitmap.longIterator
        return object : MutableIterator<Long> {
            override fun hasNext(): Boolean = iterator.hasNext()
            override fun next(): Long = iterator.next()
            override fun remove(): Unit = throw UnsupportedOperationException("Use remove(value)")
        }
    }
    override fun addAll(elements: Collection<Long>): Boolean {
        val before = bitmap.longCardinality
        if (elements is DucklakeLongSet) bitmap.or(elements.bitmap)
        else elements.forEach { bitmap.addLong(it) }
        return before != bitmap.longCardinality
    }
    fun copy(): DucklakeLongSet = DucklakeLongSet(bitmap.clone())
    fun subtract(other: DucklakeLongSet): DucklakeLongSet = copy().also { it.bitmap.andNot(other.bitmap) }
    fun addRange(start: Long, end: Long) = bitmap.addRange(start, end)
    @PublishedApi
    internal fun longIterator() = bitmap.longIterator
    inline fun forEachLong(action: (Long) -> Unit) {
        val iterator = longIterator()
        while (iterator.hasNext()) action(iterator.next())
    }
    inline fun filterLong(predicate: (Long) -> Boolean): DucklakeLongSet = DucklakeLongSet().also { result ->
        forEachLong { if (predicate(it)) result.addLong(it) }
    }
    // Include wrapper/map/container headers conservatively, not just serialized bitmap bytes.
    fun getRetainedSizeInBytes(): Long = 192L + 2L * bitmap.longSizeInBytes

    companion object {
        fun copyOf(values: Collection<Long>): DucklakeLongSet =
            if (values is DucklakeLongSet) values.copy() else DucklakeLongSet().apply { addAll(values) }

        internal fun readPortable(input: DataInput): DucklakeLongSet = DucklakeLongSet().apply { bitmap.deserializePortable(input) }
    }
}
