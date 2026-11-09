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

import dev.brikk.house.sql.shape.Finding
import dev.brikk.house.sql.shape.FindingKind
import dev.brikk.house.sql.shape.Severity
import dev.brikk.house.sql.shape.SqlFragment
import dev.brikk.house.sql.shape.certify
import dev.brikk.house.sql.verify.TrinoVerifier
import dev.brikk.house.sql.verify.VerifyResult

/**
 * Transpile-first corpus dialect gate for the DuckLake corpus-replay adapter
 * (TEST-ONLY — the production connector never sees raw SQL; Trino parses it).
 *
 * Exact-output replay rejects uncertified translations and even warning-level semantic hazards.
 * Unicode-only case-folding findings are accepted for the ASCII corpus. The translated SQL must
 * also parse under the selected Trino grammar; certification alone does not check that grammar.
 * Trailing ORDER BY ALL can be dropped because mirrors compare sorted rows, but not with LIMIT.
 * Literal version time travel is translated; non-literal forms have no faithful rewrite here.
 * [TrinoReplayEngine] owns alias binding and schema-aware MAP access after this gate.
 */
object TrinoCorpusDialect {

    private const val DUCKDB = "duckdb"
    private const val TRINO = "trino"

    private val verifier = TrinoVerifier()

    sealed interface Gate

    /** Runnable: [trinoSql] is the transpiled Trino query (still alias-qualified). */
    data class Run(val trinoSql: String) : Gate

    /** Engine-skip with a specific reason (never a failure). */
    data class Skip(val reason: String) : Gate

    @Suppress("ReturnCount")
    fun gate(duckdbSql: String): Gate {
        val (pre, preSkip) = preprocess(duckdbSql)
        if (preSkip != null) return preSkip
        checkNotNull(pre)

        // SqlFragment construction can throw on a hard parse failure before certification.
        val report = try {
            SqlFragment(pre, DUCKDB).certify(TRINO, desugarPipes = true)
        } catch (e: Exception) {
            return Skip("certify duckdb->trino threw: ${firstLine(e)}")
        }
        if (!report.okAccepting { it.isUnicodeScoped }) {
            val refusals = report.findings.filter { it.severity == Severity.REFUSAL && !it.isUnicodeScoped }
            return Skip("not trino-certified: " + refusals.joinToString("; ") { "${it.kind}: ${it.detail}" })
        }
        // A bare DATE column plus INTERVAL may only warn without schema information, but exact
        // replay cannot tolerate that potential type-promotion difference.
        report.findings.firstOrNull { it.kind == FindingKind.SEMANTIC_HAZARD && !it.isUnicodeScoped }?.let {
            return Skip("semantic hazard (${it.subject}): ${it.detail}")
        }

        val sql = rewriteInlineTimeTravel(report.result.sql)
        if (AT_PAREN.containsMatchIn(sql)) {
            return Skip("non-literal inline time travel has no Trino form")
        }

        // Extra signal: the transpiled SQL must re-parse under Trino's real grammar. A failure is
        // a brikk-sql emission bug — skip it (and report upstream) rather than fail at execute time.
        return nativeGrammarGate(sql, verifier.verify(sql))
    }

    internal fun nativeGrammarGate(sql: String, verify: VerifyResult): Gate = when (verify) {
        VerifyResult.Accepted -> Run(sql)
        is VerifyResult.Rejected ->
            Skip("transpiled SQL not parseable by Trino grammar: ${firstLineOf(verify.error.message)}")
        is VerifyResult.Unavailable -> error("Trino verifier unavailable: ${verify.reason}")
        is VerifyResult.Unverified -> error("Trino verifier returned no grammar verdict: ${verify.reason}")
    }

    /**
     * Up-front preprocessing on the ORIGINAL DuckDB SQL: reject non-reads, handle `ORDER BY ALL`
     * (drop when trailing — the mirror sorts rows; skip when it governs a LIMIT), and apply the
     * explicit residual skips. Returns `(pre-body, null)` when ready to parse+transpile, or
     * `(null, Skip)` to short-circuit.
     */
    @Suppress("ReturnCount")
    private fun preprocess(duckdbSql: String): Pair<String?, Skip?> {
        val trimmed = duckdbSql.trim().removeSuffix(";").trim()
        val body = stripLeadingCommentsAndWhitespace(trimmed)
        if (!startsWithSelectOrParenSelect(body) && !startsWithWithCte(body)) {
            return null to Skip("not a read (SELECT/WITH) statement")
        }
        val orderByAll = ORDER_BY_ALL.find(body)
        val pre = when {
            orderByAll == null -> body
            LIMIT_OR_OFFSET.containsMatchIn(body.substring(orderByAll.range.last + 1)) ->
                return null to Skip("ORDER BY ALL governs a LIMIT/OFFSET (top-N ordering can't be dropped)")
            else -> body.removeRange(orderByAll.range).trim()
        }
        explicitResidualSkip(pre)?.let { return null to it }
        return pre to null
    }

    /**
     * Explicit residuals `certify()` can't self-detect — checked on the ORIGINAL DuckDB SQL
     * (pre-transpile). Each is a genuine DuckDB-vs-Trino divergence with a TRUE, specific reason:
     *  - `information_schema` — valid syntax, certify says ok=true; but the catalog CONTENT differs
     *    DuckDB-vs-Trino, which the transpiler can't know.
     *  - DuckDB virtual columns (`rowid`/`filename`/`file_row_number`/`file_index`) — valid
     *    identifiers, certify ok=true; no Trino column, so they'd resolve differently.
     */
    private fun explicitResidualSkip(pre: String): Skip? = when {
        INFORMATION_SCHEMA.containsMatchIn(pre) ->
            Skip("information_schema: catalog content differs DuckDB-vs-Trino")
        DUCKDB_VIRTUAL_COLUMN.find(pre) != null ->
            Skip("DuckDB virtual column '${DUCKDB_VIRTUAL_COLUMN.find(pre)!!.value}' has no Trino equivalent")
        else -> null
    }

    /**
     * A finding whose divergence requires non-ASCII input — its [Finding.areas] include `unicode`
     * (today: `lower()`/`upper()` Turkish-İ / ß case folding). Safe to accept for our ASCII-only
     * corpus. `typeof()` (areas `[string]`) and DATE+INTERVAL (areas `[datetime]`) are NOT unicode-
     * scoped and stay blocking.
     */
    private val Finding.isUnicodeScoped: Boolean get() = UNICODE_AREA in areas

    /**
     * `t AT (VERSION => 3)` → Trino `t FOR VERSION AS OF 3` (DuckLake version == snapshot id, so
     * semantics are identical). Literal form only; idempotent.
     */
    fun rewriteInlineTimeTravel(sql: String): String =
        AT_VERSION_LITERAL.replace(sql) { m -> "FOR VERSION AS OF ${m.groupValues[1]}" }

    private fun stripLeadingCommentsAndWhitespace(sql: String): String {
        var s = sql
        while (true) {
            val t = s.trimStart()
            s = when {
                t.startsWith("--") -> t.substringAfter('\n', missingDelimiterValue = "")
                t.startsWith("/*") -> t.substringAfter("*/", missingDelimiterValue = "")
                else -> return t
            }
        }
    }

    private fun startsWithSelectOrParenSelect(body: String): Boolean {
        val b = body.trimStart('(', ' ', '\t', '\n', '\r')
        return b.regionMatches(0, "SELECT", 0, "SELECT".length, ignoreCase = true)
    }

    private fun startsWithWithCte(body: String): Boolean =
        body.regionMatches(0, "WITH", 0, "WITH".length, ignoreCase = true) &&
            !body.getOrElse(4) { ' ' }.isJavaIdentifierPart()

    private fun firstLine(e: Exception): String = firstLineOf(e.message ?: e.toString())

    private fun firstLineOf(s: String): String = s.lineSequence().firstOrNull().orEmpty()

    private val AT_VERSION_LITERAL =
        Regex("\\bAT\\s*\\(\\s*VERSION\\s*=>\\s*(\\d+)\\s*\\)", RegexOption.IGNORE_CASE)
    private val AT_PAREN = Regex("\\bAT\\s*\\(", RegexOption.IGNORE_CASE)
    private val ORDER_BY_ALL =
        Regex("\\bORDER\\s+BY\\s+ALL\\b(\\s+(ASC|DESC))?", RegexOption.IGNORE_CASE)
    private val LIMIT_OR_OFFSET = Regex("\\b(LIMIT|OFFSET)\\b", RegexOption.IGNORE_CASE)
    private val INFORMATION_SCHEMA = Regex("\\binformation_schema\\b", RegexOption.IGNORE_CASE)
    private val DUCKDB_VIRTUAL_COLUMN =
        Regex("\\b(rowid|filename|file_row_number|file_index)\\b", RegexOption.IGNORE_CASE)

    /** `Finding.areas` tag marking a divergence that only manifests on non-ASCII input. */
    private const val UNICODE_AREA = "unicode"
}
