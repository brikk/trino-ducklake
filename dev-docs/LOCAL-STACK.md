# Local Compose workbench

The workbench is [`compose/docker-compose.yml`](../compose/docker-compose.yml), not
a production install or a verified self-contained fresh-clone recipe. It has a
sibling Quack fixture build context and mounts extra example catalogs. Check those
paths/configurations before starting; the open portability work is tracked in
[TODO.md](TODO.md#test-ci-and-release-hygiene).

## Build and select matching inputs

From the repository root, with JDK 25 and Docker/Podman Compose available:

```sh
./gradlew pluginAssemble -PtrinoVersion=483
cd compose
PLUGIN_VERSION=483-0.5.4 TRINO_VERSION=483 DUCKDB_VERSION=1.5.6 docker compose config
# After checking the rendered configuration and required checkout paths:
PLUGIN_VERSION=483-0.5.4 TRINO_VERSION=483 DUCKDB_VERSION=1.5.6 docker compose up -d
```

Use 484 in both the build target and container/plugin values for that target. These
overrides matter: the checked-in `.env` contains older `PLUGIN_VERSION` and native
producer defaults. `PLUGIN_VERSION` must equal the actual assembled directory name,
not simply the server version. Do not silently select candidate artifacts.

The stack includes PostgreSQL, MinIO, a bucket initializer, native DuckDB bootstrap,
Quack and Trino. Bootstrap initializes PostgreSQL metadata and optional TPC-H tables.
Health checks/container startup do not establish connector read/write correctness.

## Access

| Service | Host endpoint |
| --- | --- |
| Trino | `http://localhost:9080` |
| PostgreSQL | `localhost:9432`, default database/user/password `ducklake` |
| MinIO API | `http://localhost:9000` |
| MinIO console | `http://localhost:9001`, default credentials `minioadmin` / `minioadmin` |

These are local-development credentials with no production access-control promise.

```sh
trino --server http://localhost:9080 --catalog ducklake
docker compose logs -f trino
docker compose stop
```

JDBC clients still need a user identity; set the JDBC `user` property even when the
local server has no authentication. Optional TPC-H seed scale/schema and service
image/memory settings are in [`compose/.env`](../compose/.env). Existing seed tables
are not regenerated merely by changing a scale setting.

`docker compose down -v` destroys metadata and object-store volumes. Do not run it
to "fix" a failed test or preserve it as an automatic reset step. Restarting services
and rebuilding an image are not a reason to discard lake data.

The [historical workbench walkthrough](archive/designs/LOCAL-STACK-history.md) retains old
commands/defaults for traceability, not as current instructions.
