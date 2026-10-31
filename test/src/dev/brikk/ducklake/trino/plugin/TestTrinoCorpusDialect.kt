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

import dev.brikk.house.sql.verify.TrinoVerifier
import dev.brikk.house.sql.verify.VerifyError
import dev.brikk.house.sql.verify.VerifyResult
import io.trino.spi.Plugin
import io.trino.sql.parser.SqlParser
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestTrinoCorpusDialect {
    @Test
    fun nativeParserMatchesTheSelectedEngine() {
        val serverVersion = requireNotNull(Plugin::class.java.`package`.implementationVersion)
        assertThat(SqlParser::class.java.`package`.implementationVersion).isEqualTo(serverVersion)

        // JSON_SERIALIZE becomes reserved in 484. Prove the verifier uses the target's actual
        // grammar rather than accidentally validating both lanes with the newer parser.
        val result = TrinoVerifier().verify("SELECT 1 AS json_serialize")
        if (serverVersion == "483") assertThat(result).isEqualTo(VerifyResult.Accepted)
        else assertThat(result).isInstanceOf(VerifyResult.Rejected::class.java)
    }

    @Test
    fun simpleReadIsTranspiledAndNativeVerified() {
        val result = TrinoCorpusDialect.gate("SELECT 1 AS value")
        assertThat(result).isInstanceOf(TrinoCorpusDialect.Run::class.java)
        val sql = (result as TrinoCorpusDialect.Run).trinoSql
        assertThat(TrinoVerifier().verify(sql)).isEqualTo(VerifyResult.Accepted)
    }

    @Test
    fun verifierRejectionsCarryTheNativeParserDiagnostic() {
        val result = TrinoVerifier().verify("SELECT FROM")
        assertThat(result).isInstanceOf(VerifyResult.Rejected::class.java)
        assertThat((result as VerifyResult.Rejected).error.message).isNotBlank()
        val rejection = VerifyResult.Rejected(VerifyError("native parser diagnostic\nmore detail"))
        assertThat(TrinoCorpusDialect.nativeGrammarGate("SELECT FROM", rejection))
            .isEqualTo(TrinoCorpusDialect.Skip("transpiled SQL not parseable by Trino grammar: native parser diagnostic"))
    }

    @Test
    fun missingGrammarVerdictsFailInsteadOfPassingOrSkipping() {
        assertThatThrownBy {
            TrinoCorpusDialect.nativeGrammarGate("SELECT 1", VerifyResult.Unavailable("missing parser"))
        }.isInstanceOf(IllegalStateException::class.java).hasMessageContaining("missing parser")
        assertThatThrownBy {
            TrinoCorpusDialect.nativeGrammarGate("SELECT 1", VerifyResult.Unverified("no verdict"))
        }.isInstanceOf(IllegalStateException::class.java).hasMessageContaining("no verdict")
    }

    @Test
    fun existingSemanticAndStatementGuardsRemainInPlace() {
        assertThat(TrinoCorpusDialect.gate("DELETE FROM lake.t")).isInstanceOf(TrinoCorpusDialect.Skip::class.java)
        assertThat(TrinoCorpusDialect.gate("SELECT * FROM information_schema.tables"))
            .isInstanceOf(TrinoCorpusDialect.Skip::class.java)
    }
}
