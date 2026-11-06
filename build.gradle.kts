import java.security.MessageDigest
import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    id("buildlogic.kotlin.library")
    id("buildlogic.kotlin.brikk")
    java
    alias(libs.plugins.detekt)
}

group = "dev.brikk.ducklake"
// Trino plugins must match the server SPI exactly. Keep 483 as the default and build
// the other supported target with -PtrinoVersion=484, including its matching BOM.
val trinoVersion = providers.gradleProperty("trinoVersion").getOrElse(libs.versions.trino.get())
require(trinoVersion in setOf("483", "484")) { "Supported Trino targets: 483, 484 (got $trinoVersion)" }
val catalogTrialVersion = providers.gradleProperty("catalogTrialVersion").orNull
require(catalogTrialVersion == null || catalogTrialVersion == "0.17.0-20261106.162112-2") {
    "Only published snapshot 0.17.0-20261106.162112-2 is accepted for this trial; normal builds use 0.16.0"
}
val catalogVersion = catalogTrialVersion ?: libs.versions.ducklake.catalog.get()
val r07CatalogCandidate = providers.gradleProperty("r07CatalogCandidate").orNull
require(r07CatalogCandidate == null || r07CatalogCandidate in setOf("", catalogVersion)) {
    "Use -Pr07CatalogCandidate (or =$catalogVersion) for experimental planning on the released catalog; local bundles and snapshots are not accepted"
}
kotlin.sourceSets.main { kotlin.srcDir("trino-$trinoVersion/src") }
// <trino-version>-<our-release>: never label a 484 build as compatible with 483.
version = "$trinoVersion-0.5.3" + (if (r07CatalogCandidate != null) "-r07" else "") +
    (if (catalogTrialVersion != null) "-catalog-trial" else "")

// Idiomatic-Kotlin quality gate (detekt 2.0 runs on the JDK 25 daemon; jvmTarget is read
// from the Kotlin compilerOptions). Custom src/test layout, so point detekt at it explicitly.
detekt {
    buildUponDefaultConfig = true
    config.setFrom(rootProject.file("config/detekt/detekt.yml"))
    baseline = file("detekt-baseline.xml")
    source.setFrom("src", "test", "trino-$trinoVersion/src", if (providers.gradleProperty("r07CatalogCandidate").isPresent) "candidate/src" else "released")
}

// The brikk-sql (dev.brikk.house) TEST-ONLY tooling uses Maven Central releases; optional snapshots
// resolve from the Central Portal repo wired by the buildlogic.kotlin.brikk convention plugin.
repositories {
    // dev.brikk.ducklake catalog + corpus-replay: released versions resolve from mavenCentral;
    // -SNAPSHOTs from ~/.m2 (local two-repo dev loop) then the Central Portal snapshots repo —
    // both wired, in that order, in buildlogic.kotlin.brikk.
    mavenCentral()
}

// Both planning modes consume the same released artifacts; only final-1.1 planning is opt-in.
val r07Modules = setOf("ducklake-catalog", "slt-format", "ducklake-test-corpus-replay")
if (catalogTrialVersion != null) {
    repositories.exclusiveContent {
        forRepository {
            repositories.maven {
                name = "publishedCatalogTrial"
                url = uri("https://central.sonatype.com/repository/maven-snapshots/")
            }
        }
        filter { r07Modules.forEach { includeModule("dev.brikk.ducklake", it) } }
    }
    kotlin.sourceSets.test { kotlin.srcDir("trial/test") }
    detekt { source.from("trial/test") }
}
kotlin.sourceSets.main { kotlin.srcDir(if (r07CatalogCandidate != null) "candidate/src" else "released/src") }
dependencies {
    constraints {
        implementation("dev.brikk.ducklake:ducklake-catalog:$catalogVersion") {
            version { strictly(catalogVersion) }
        }
        testImplementation("dev.brikk.ducklake:ducklake-test-corpus-replay:$catalogVersion") {
            version { strictly(catalogVersion) }
        }
        testImplementation("dev.brikk.ducklake:slt-format:$catalogVersion") {
            version { strictly(catalogVersion) }
        }
    }
}

val verifyCatalogRelease by tasks.registering {
    group = "verification"
    description = "Attests the published catalog, replay, SLT and fixture jars in either planning mode."
    val receipt = layout.projectDirectory.file(if (catalogTrialVersion != null)
        "gradle/catalog-trial.sha256" else "gradle/catalog-release.sha256")
    inputs.file(receipt)
    inputs.property("catalogVersion", catalogVersion)
    doLast {
        val hashes = receipt.asFile.readLines().filter { it.isNotBlank() && !it.startsWith("#") }.associate { line ->
            val (hash, path) = line.split(Regex("\\s+"), limit = 2)
            path.substringAfterLast('/') to hash
        }
        val artifacts = configurations.testRuntimeClasspath.get().resolvedConfiguration.resolvedArtifacts
            .filter { it.moduleVersion.id.group == "dev.brikk.ducklake" && it.name in r07Modules }
        check(artifacts.size == 4) { "Expected catalog, catalog fixtures, SLT and replay jars: $artifacts" }
        artifacts.forEach { artifact ->
            val jar = artifact.file
            check(artifact.moduleVersion.id.version == catalogVersion) { "Unexpected catalog build: $jar" }
            val actual = MessageDigest.getInstance("SHA-256").digest(jar.readBytes())
                .joinToString("") { "%02x".format(it) }
            check(actual == hashes[jar.name]) { "Catalog release SHA256 mismatch: $jar ($actual)" }
            println("CATALOG SHA256 $actual  ${jar.absolutePath}")
        }
    }
}
val verifyR07Candidate by tasks.registering {
    group = "verification"
    description = "Checks experimental planning selection and attests its released catalog artifacts."
    dependsOn(verifyCatalogRelease)
    doFirst { check(r07CatalogCandidate != null) { "Set -Pr07CatalogCandidate to select experimental planning" } }
}
if (r07CatalogCandidate != null) {
    kotlin.sourceSets.test { kotlin.srcDir("qualification/src") }
    tasks.test { dependsOn(verifyR07Candidate) }
} else {
    kotlin.sourceSets.test { kotlin.srcDir("released/test") }
}

// Use Trino BOM for all managed dependency versions
dependencies {
    // BOM import — controls versions for io.trino, io.airlift, jackson, opentelemetry, etc.
    implementation(platform("io.trino:trino-root:$trinoVersion"))
    testImplementation(platform("io.trino:trino-root:$trinoVersion"))
    implementation(enforcedPlatform(libs.kotlin.bom)) {
        exclude(group = "org.junit")
        exclude(group = "org.junit.jupiter")
    }

    // Core implementation dependencies (versions from BOM)
    implementation("com.google.guava:guava")
    implementation("com.google.inject:guice") {
        artifact {
            classifier = "classes"
        }
    }
    implementation(libs.ducklake.catalog)
    implementation(libs.hikari)
    implementation("io.airlift:bootstrap")
    implementation("io.airlift:configuration")
    implementation("io.airlift:json")
    implementation("io.airlift:log")
    implementation("io.airlift:units")
    implementation("io.trino:trino-filesystem")
    implementation("io.trino:trino-filesystem-manager")
    implementation("io.trino:trino-hive")
    implementation("io.trino:trino-memory-context")
    implementation("io.trino:trino-parquet")
    implementation("io.trino:trino-plugin-toolkit")
    implementation("jakarta.validation:jakarta.validation-api")
    implementation("joda-time:joda-time")
    implementation("org.apache.parquet:parquet-column")
    implementation("org.apache.parquet:parquet-format-structures") {
        exclude(group = "javax.annotation", module = "javax.annotation-api")
    }
    implementation("org.weakref:jmxutils")
    implementation(libs.roaring.bitmap)

    // SPI dependencies — provided by Trino at runtime (compileOnly = Maven provided)
    compileOnly("com.fasterxml.jackson.core:jackson-annotations")
    compileOnly("io.airlift:slice")
    compileOnly("io.opentelemetry:opentelemetry-api")
    compileOnly("io.opentelemetry:opentelemetry-api-incubator")
    compileOnly("io.opentelemetry:opentelemetry-context")
    compileOnly("io.trino:trino-spi")

    // Runtime-only dependencies
    runtimeOnly("io.airlift:log-manager")
    runtimeOnly("org.postgresql:postgresql")
    // MySQL 8+ catalog backend: the driver on the deployed connector's classpath lets a user
    // point ducklake.catalog.database-url at jdbc:mysql://... HikariCP resolves it from the URL.
    runtimeOnly(libs.mysql.jdbc)

    // Test dependencies
    testImplementation("io.airlift:testing")
    testImplementation("io.trino:trino-hdfs")
    testImplementation("io.trino:trino-main")
    testImplementation("io.trino:trino-testing")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.mysql)

    // External DuckDB JDBC driver — TEST-ONLY: cross-engine tests drive a standalone DuckDB
    // process (jdbc:duckdb:) that writes/reads parquet DuckLake tables through the shared catalog,
    // to prove Trino and DuckDB agree. The production connector no longer embeds DuckDB.
    testImplementation(libs.duckdb.jdbc)
    testRuntimeOnly(libs.quack.jdbc)

    // brikk-sql: DuckDB→Trino SQL transpiler powering the corpus-replay dialect gate
    // (transpile-first — run what transpiles + is mappable, engine-skip the rest). TEST-ONLY:
    // the production connector never sees raw SQL (Trino parses it). The -jvm artifacts are the
    // JVM target of the Kotlin-Multiplatform library; brikk-sql-verify-trino's TrinoVerifier re-parses
    // transpiled SQL under Trino's real grammar as an extra gate signal.
    testImplementation(libs.brikk.sql)
    testImplementation(libs.brikk.sql.metadata)
    testImplementation(libs.brikk.sql.verify)
    testImplementation("io.trino:trino-parser") {
        version { strictly(trinoVersion) }
        because("The native verifier must use the selected server's grammar, not its default Trino 484 parser")
    }

    // Shared Testcontainer fixture (TestingDucklakePostgreSqlCatalogServer) lives in
    // ducklake-catalog's java-test-fixtures source set, published as the -test-fixtures
    // variant of dev.brikk.ducklake:ducklake-catalog.
    testImplementation(testFixtures(libs.ducklake.catalog))

    // Upstream DuckLake sqllogictest corpus replay (oracle + driver + engine seam);
    // TrinoReplayEngine implements its ReplayReadEngine, TestTrinoCorpusReplay runs it.
    testImplementation(libs.ducklake.test.corpus.replay)

    // compileOnly deps also needed at test time
    testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
    testImplementation("io.airlift:slice")
    testImplementation("io.trino:trino-spi")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf(
        "--enable-preview",
    ))
}

tasks.test {
    dependsOn(verifyCatalogRelease)
    // The handoff's JVM-sensitive renderer checks assert the actual forked runtime, not log text.
    systemProperty("ducklake.test.java-version", providers.gradleProperty("testJavaVersion").getOrElse("25"))
    // Compile against the repository's JDK 25 toolchain, but allow CI to execute the resulting
    // Java 25 bytecode on a newer VM for compatibility/stability checks.
    providers.gradleProperty("testJavaVersion").orNull?.let { version ->
        javaLauncher.set(javaToolchains.launcherFor {
            languageVersion.set(JavaLanguageVersion.of(version))
        })
    }

    useJUnitPlatform {
        // Optional JUnit tag exclusion, e.g. `-PexcludeTags=some-tag,another-tag` (comma-separated).
        // A generic escape hatch for skipping heavy/environment-specific integration tests on the
        // PR gate while keeping them runnable locally. No tags are excluded by default or in CI.
        (project.findProperty("excludeTags") as String?)
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.takeIf { it.isNotEmpty() }
            ?.let { excludeTags(*it.toTypedArray()) }
    }

    maxHeapSize = "3g"
    minHeapSize = "3g"

    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
    systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
    // Podman Docker API compatibility rejects status=restarting filter in ReportLeakedContainers
    systemProperty("ReportLeakedContainers.disabled", "true")

    // Forward the catalog-backend selector from the Gradle CLI to the test JVM. Gradle
    // doesn't propagate `-D` system properties to forked test workers by default.
    // Without this, `-Dducklake.test.catalog-backend=DUCKDB_QUACK` silently runs PG.
    System.getProperty("ducklake.test.catalog-backend")?.let {
        systemProperty("ducklake.test.catalog-backend", it)
        // Treat the property as a task input so Gradle re-runs the test task when the
        // selector value changes instead of returning a cached UP-TO-DATE result.
        inputs.property("ducklake.test.catalog-backend", it)
    }

    // Upstream DuckLake sqllogictest corpus (TestTrinoCorpusReplay): corpus location
    // in the sibling module's pinned submodule + optional dir selection
    // (-Dducklake.corpus.dirs=all for the full corpus).
    systemProperty(
        "ducklake.corpus.root",
        rootProject.projectDir.resolve("ducklake/test/sql").absolutePath,
    )
    System.getProperty("ducklake.corpus.dirs")?.let {
        systemProperty("ducklake.corpus.dirs", it)
        inputs.property("ducklake.corpus.dirs", it)
    }

    jvmArgs(
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:-OmitStackTraceInFastThrow",
        "-XX:G1HeapRegionSize=32M",
        "-XX:+UnlockDiagnosticVMOptions",
        "--add-modules=jdk.incubator.vector",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        // The standalone DuckDB JDBC driver (cross-engine tests) reaches into private
        // DirectByteBuffer internals for off-heap interop.
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}

// Assemble the Trino plugin into build/trino-plugin/trino-ducklake-<version>/ as a flat set of
// jars (this module's jar + every runtime dep). That directory is what a Trino server loads from
// its plugin/ dir, and it's the payload for the release archives below.
// A library can reintroduce SPI transitively (484's JSON library does); the server provides it.
configurations.runtimeClasspath {
    exclude(group = "io.trino", module = "trino-spi")
}
val pluginAssemble by tasks.registering(Sync::class) {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("trino-plugin/trino-ducklake-$version"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}

// Release distribution archives. Both expand to a single top-level directory
// `trino-ducklake-<version>/` full of jars — the standard Trino plugin layout: drop that
// directory into the coordinator/worker `plugin/` dir. .zip and .tar.gz are both produced so
// downloaders can pick their platform's norm; the release workflow attaches both to the GitHub
// Release. SHA-256 sidecars are generated for integrity verification.
val pluginDistZip by tasks.registering(Zip::class) {
    dependsOn(pluginAssemble)
    archiveBaseName.set("trino-ducklake")
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    from(layout.buildDirectory.dir("trino-plugin/trino-ducklake-$version")) {
        into("trino-ducklake-$version")
    }
}

val pluginDistTar by tasks.registering(Tar::class) {
    dependsOn(pluginAssemble)
    archiveBaseName.set("trino-ducklake")
    archiveVersion.set(project.version.toString())
    compression = Compression.GZIP
    archiveExtension.set("tar.gz")
    destinationDirectory.set(layout.buildDirectory.dir("distributions"))
    from(layout.buildDirectory.dir("trino-plugin/trino-ducklake-$version")) {
        into("trino-ducklake-$version")
    }
}

val pluginDist by tasks.registering {
    group = "distribution"
    description = "Builds the Trino plugin release archives (.zip + .tar.gz) with SHA-256 sidecars."
    dependsOn(pluginDistZip, pluginDistTar)
    val zipFile = pluginDistZip.flatMap { it.archiveFile }
    val tarFile = pluginDistTar.flatMap { it.archiveFile }
    // Track each target's checksum files, not the shared output directory. Otherwise switching
    // targets can mark this task up-to-date after the archive tasks run, leaving sidecars absent.
    inputs.files(zipFile, tarFile)
    outputs.files(listOf(zipFile, tarFile).map { archive ->
        archive.map { File(it.asFile.parentFile, "${it.asFile.name}.sha256") }
    })
    doLast {
        listOf(zipFile.get().asFile, tarFile.get().asFile).forEach { f ->
            val sha = MessageDigest.getInstance("SHA-256")
                .digest(f.readBytes())
                .joinToString("") { b -> "%02x".format(b) }
            File(f.parentFile, "${f.name}.sha256").writeText("$sha  ${f.name}\n")
        }
    }
}

tasks.build {
    dependsOn(pluginAssemble)
}

tasks.register("printCompileDependencyTree") {
    doLast {
        fun walk(dep: ResolvedDependency, indent: String = "") {
            dep.moduleArtifacts.forEach { artifact ->
                println("$indent${dep.moduleGroup}:${dep.moduleName}:${dep.moduleVersion}")
                println("$indent  -> ${artifact.file.absolutePath}")
            }
            dep.children
                .sortedBy { "${it.moduleGroup}:${it.moduleName}:${it.moduleVersion}" }
                .forEach { walk(it, "$indent  ") }
        }
        configurations
            .getByName("compileClasspath")
            .resolvedConfiguration
            .firstLevelModuleDependencies
            .sortedBy { "${it.moduleGroup}:${it.moduleName}:${it.moduleVersion}" }
            .forEach { walk(it) }
    }
}
