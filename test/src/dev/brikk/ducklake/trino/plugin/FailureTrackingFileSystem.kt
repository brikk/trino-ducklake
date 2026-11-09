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

import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoInput
import io.trino.filesystem.TrinoInputFile
import io.trino.filesystem.TrinoInputStream
import io.trino.filesystem.TrinoOutputFile
import io.trino.filesystem.memory.MemoryFileSystem
import org.assertj.core.api.Assertions.assertThat
import java.io.OutputStream

internal class FailureTrackingFileSystem(private val delegate: TrinoFileSystem = MemoryFileSystem()) : TrinoFileSystem by delegate {
    var readFailure: Throwable? = null
    var inputCloseFailure: Throwable? = null
    var writeFailure: Throwable? = null
    var outputCloseFailure: Throwable? = null
    val openedInputs = mutableListOf<Int>()
    val closedInputs = mutableListOf<Int>()
    val openedOutputs = mutableListOf<Location>()
    val closedOutputs = mutableListOf<Location>()
    val deleted = mutableListOf<Location>()

    override fun newInputFile(location: Location): TrinoInputFile {
        val input = delegate.newInputFile(location)
        return object : TrinoInputFile by input {
            override fun newInput(): TrinoInput {
                val source = input.newInput()
                val id = openedInputs.size.also { openedInputs.add(it) }
                return object : TrinoInput by source {
                    override fun readFully(position: Long, buffer: ByteArray, offset: Int, length: Int) {
                        readFailure?.let { throw it }
                        source.readFully(position, buffer, offset, length)
                    }
                    override fun readTail(buffer: ByteArray, offset: Int, length: Int): Int {
                        readFailure?.let { throw it }
                        return source.readTail(buffer, offset, length)
                    }
                    override fun close() {
                        closedInputs.add(id)
                        source.close()
                        inputCloseFailure?.let { throw it }
                    }
                }
            }

            override fun newStream(): TrinoInputStream {
                val stream = input.newStream()
                val id = openedInputs.size.also { openedInputs.add(it) }
                return object : TrinoInputStream() {
                    override fun getPosition(): Long = stream.position
                    override fun seek(position: Long) = stream.seek(position)
                    override fun read(): Int = stream.read()
                    override fun read(buffer: ByteArray, offset: Int, length: Int): Int = stream.read(buffer, offset, length)
                    override fun close() {
                        closedInputs.add(id)
                        stream.close()
                    }
                }
            }
        }
    }

    override fun newOutputFile(location: Location): TrinoOutputFile {
        val output = delegate.newOutputFile(location)
        return trackingOutputFile(output) { stream ->
            openedOutputs.add(location)
            object : OutputStream() {
                override fun write(value: Int) {
                    writeFailure?.let { throw it }
                    stream.write(value)
                }
                override fun write(buffer: ByteArray, offset: Int, length: Int) {
                    writeFailure?.let { throw it }
                    stream.write(buffer, offset, length)
                }
                override fun flush() = stream.flush()
                override fun close() {
                    closedOutputs.add(location)
                    stream.close()
                    outputCloseFailure?.let { throw it }
                }
            }
        }
    }

    override fun deleteFile(location: Location) {
        deleted.add(location)
        delegate.deleteFile(location)
    }

    fun assertInputsClosed() {
        assertThat(openedInputs).isNotEmpty()
        assertThat(closedInputs).containsAll(openedInputs)
    }
}
