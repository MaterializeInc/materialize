---
commit: e7ac38b338
updated: 2026-05-06
scope: existing-test-driver-integration
---

# Deployment Topology

This topology is for integrating Materialize's existing test drivers with the
Antithesis Test Composer. Start small to prove the harness, then move to a
broad integration topology for source/sink/config coverage. Every additional
container adds state space, so dependency containers should be introduced in
deliberate tiers.

## Minimal useful topology

| Container | Role | Image source | Runs | Notes |
| --- | --- | --- | --- | --- |
| `materialized` | SUT | Adapt `src/materialized/ci/Dockerfile` | `entrypoint.sh` / `materialized` | Existing image already packages the binary and exposes `environmentd`/`clusterd` symlinks. |
| `test-driver` | Client | New Antithesis workload image | Long-lived entrypoint plus `/opt/antithesis/test/v1/...` commands | Emits `setup_complete` after health checks, then sleeps so Test Composer can run commands. |

Start with only SQL/table/view tests. Add dependency containers only when a
driver command needs them.

## Broad integration topology

For the user's intended coverage across many configs, sources, and sinks, the
useful second topology is intentionally wider:

| Container | Role | Why it exists |
| --- | --- | --- |
| `materialized` | SUT | Primary database under test. |
| `test-driver` | Client | Owns Antithesis test templates, lifecycle signal, assertions, and wrappers. |
| `redpanda` or `kafka` | Dependency | Kafka source/sink coverage. Prefer one broker implementation per run. |
| `schema-registry` | Dependency | Avro/CSR source and sink coverage. |
| `postgres` | Dependency | Postgres CDC and reference checks. |
| `mysql` | Dependency | MySQL CDC. |
| `sql-server` | Dependency | SQL Server CDC. |
| `minio` | Dependency | S3-compatible object storage, COPY, sinks, persist blob/durability testing. |
| `azurite` | Dependency | Azure blob coverage; use in the same run only if it stays cheap, otherwise split. |
| `cockroach` or `postgres-metadata` | Dependency | External persist consensus/metadata and durability scenarios. |

Keep topology families that require different startup settings as separate
Antithesis launches. Examples: file-backed persist vs external metadata,
Minio-backed persist vs Azurite-backed persist, embedded clusterd vs external
clusterd, single `materialized` vs 0dt/multi-`materialized`.

## Optional dependencies by driver coverage

| Dependency | Needed for | Existing source |
| --- | --- | --- |
| Kafka-compatible broker plus schema registry | Kafka sources/sinks and most full `testdrive` corpus coverage | `test/testdrive/mzcompose.py` currently uses Kafka, Redpanda, Zookeeper, and Schema Registry variants. |
| Postgres, MySQL, SQL Server | CDC and connection tests | Used by `testdrive`, `parallel-workload`, and `zippy` compositions. |
| Minio or Azurite | Persist external blob store, S3 COPY, backups, sinks | Existing mzcompose services are already used by `testdrive`, `parallel-workload`, and `zippy`. |
| CockroachDB or Postgres metadata store | External metadata/persist consensus scenarios | Needed for backup/restore, 0dt, and some durability scenarios. |
| Polaris/Iceberg | Iceberg source/sink coverage | Used by `parallel-workload` and `zippy`, not needed for the first integration. |

## Test directories for broad coverage

In the broad topology, use multiple test directories rather than one giant
directory:

```text
/opt/antithesis/test/v1/base_sql/
/opt/antithesis/test/v1/kafka_sources/
/opt/antithesis/test/v1/kafka_sinks/
/opt/antithesis/test/v1/postgres_cdc/
/opt/antithesis/test/v1/mysql_cdc/
/opt/antithesis/test/v1/sql_server_cdc/
/opt/antithesis/test/v1/s3_copy/
/opt/antithesis/test/v1/persist_durability/
/opt/antithesis/test/v1/catalog_ddl/
```

Each directory owns a coherent setup and command set. Shared helper code belongs
under `helper_` paths.

## Client image responsibilities

- Include the Antithesis Python SDK for workload assertions and lifecycle
  signaling.
- Include `testdrive` if running `.td` scripts directly.
- Include the Materialize Python package and selected driver modules if running
  Python workloads (`parallel_workload`, `zippy`, SQLancer orchestration, or
  custom wrappers).
- Install test templates under `/opt/antithesis/test/v1/<template>/`.
- Keep helper code under `helper_` paths so Test Composer ignores it.
- Keep the container running after readiness, for example with `sleep infinity`.

## Readiness

`setup_complete` belongs in the long-lived client entrypoint, not in a
`first_` command. The entrypoint should:

1. Wait for `materialized` SQL and HTTP ports.
2. Wait for any selected dependency services.
3. Run cheap bootstrapping checks, for example `SELECT 1`.
4. Emit the Antithesis lifecycle ready signal.
5. Sleep forever.

## Local validation shape

Local checks should mirror Antithesis command execution:

```text
docker compose -f antithesis/config/docker-compose.yaml up
docker compose -f antithesis/config/docker-compose.yaml exec test-driver /opt/antithesis/test/v1/<template>/<command>
```

Do not run `mzcompose` from inside an Antithesis command. `mzcompose` controls
Docker and host ports; inside Antithesis, Docker Compose is the orchestrator and
test commands should behave like clients of the already-running topology.
