plugins {
    id("buildlogic.kotlin.common")
}

// RELEASES (e.g. 0.1.0) resolve anonymously from Maven Central (mavenCentral(), declared per-module),
// so no repository is needed here for them. SNAPSHOTS (e.g. 0.1.0-SNAPSHOT) are NOT on Central proper;
// they live in the anonymous Central Portal snapshots repo wired below. We keep it configured so
// flipping a version in libs.versions.toml to a -SNAPSHOT "just works" — no other change needed.
// The repo is scoped both ways so it stays inert for the release path: snapshotsOnly() (never
// consulted for a release version) and includeGroup(...) (never consulted for any other dependency).
// Anonymous — unlike GitHub Packages' Maven registry, which 401s even for public packages.
// brikk-sql must never enter a production artifact — declare it testImplementation only.
repositories {
    // Local two-repo dev loop: `./gradlew publishToMavenLocal` in ../ducklake-catalog, then build
    // here against it. Declared FIRST so a locally published -SNAPSHOT wins over the (possibly
    // older) timestamped one in the Central Portal snapshots repo — Gradle takes the first
    // repository that has the module. Scoped to dev.brikk.ducklake snapshots only, so it can
    // never shadow a release or any other dependency. If ~/.m2 has nothing, it is simply skipped.
    mavenLocal {
        mavenContent { snapshotsOnly() }
        content { includeGroup("dev.brikk.ducklake") }
    }
    maven {
        name = "centralPortalSnapshots"
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
        content {
            includeGroup("dev.brikk.house")
            includeGroup("dev.brikk.ducklake")
        }
    }
}
