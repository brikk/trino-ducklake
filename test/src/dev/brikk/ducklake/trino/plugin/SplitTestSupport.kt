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

import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.testing.connector.TestingConnectorSession.SESSION

internal fun allSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle,
    constraint: Constraint = Constraint.alwaysTrue()): List<DucklakeSplit> =
    allConnectorSplits(splitManager, tableHandle, constraint).map { it as DucklakeSplit }

internal fun allConnectorSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle,
    constraint: Constraint = Constraint.alwaysTrue()): List<ConnectorSplit> =
    splitManager.getSplits(null, SESSION, tableHandle, mutableSetOf(), constraint).use { source ->
        buildList {
            while (!source.isFinished) addAll(source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get())
        }
    }
