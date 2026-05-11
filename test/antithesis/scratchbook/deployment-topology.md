# Deployment Topology: Materialize

## Approach: mzcompose-Generated Docker Compose

The most straightforward path is to use Materialize's **mzcompose** framework to generate the Docker Compose configuration for Antithesis. mzcompose already defines all the service classes, health checks, environment variables, and dependencies needed to run a complete Materialize test environment.

**Strategy**: Write an `mzcompose.py` file that defines the Antithesis test topology, use mzcompose to generate the Docker Compose YAML, then adapt it for Antithesis (adding test template mounts).

## Topology Overview

```
+---------------------+      +---------------------+
| workload-client     | ---> | materialized        |
| (test driver,       | <--- | (environmentd +     |
|  Antithesis SDK,    |      |  embedded clusterd)  |
|  test templates)    |      |                     |
+---------------------+      +---------+-----------+
                                       |
                    +------------------+------------------+
                    |                  |                  |
                    v                  v                  v
          +----------------+  +----------------+  +----------------+
          | postgres-       |  | minio          |  | redpanda       |
          | metadata        |  | (blob storage) |  | (Kafka-compat) |
          | (consensus)     |  |                |  |                |
          +----------------+  +----------------+  +----------------+
```

## Container Specifications

### 1. postgres-metadata (Dependency)

| | |
|---|---|
| **Role** | Metadata store / consensus for persist and catalog |
| **Image** | `postgres:16` (or mzcompose's `PostgresMetadata` service) |
| **Why** | Default metadata store in modern mzcompose. Lighter than CockroachDB. Sufficient for single-node testing. |
| **Ports** | 5432 |
| **Health check** | `pg_isready -U postgres` |
| **Network connections** | materialized reads/writes catalog and persist consensus |
| **Replicas** | 1 |

PostgreSQL is the default metadata store in modern Materialize testing (`EXTERNAL_METADATA_STORE=postgres-metadata`). CockroachDB is an alternative but adds complexity and state space without benefit for single-coordinator testing.

### 2. minio (Dependency)

| | |
|---|---|
| **Role** | S3-compatible blob storage for persist data |
| **Image** | `minio/minio` (or mzcompose's `Minio` with `setup_materialize=True`) |
| **Why** | Persist stores all durable data (source data, MV data, catalog snapshots) in blob storage. MinIO is the standard test substitute for S3. |
| **Ports** | 9000 (S3 API), 9001 (console) |
| **Health check** | `curl --fail http://localhost:9000/minio/health/live` |
| **Network connections** | materialized writes/reads persist blobs |
| **Replicas** | 1 |
| **Config** | Pre-create `/data/persist` bucket. `MINIO_STORAGE_CLASS_STANDARD=EC:0` |

### 3. redpanda (Dependency)

| | |
|---|---|
| **Role** | Kafka-compatible message broker for stream source ingestion |
| **Image** | `redpandadata/redpanda` (or mzcompose's `Redpanda` service) |
| **Why** | Enables testing the Kafka source ingestion path, which is the most common production use case. Redpanda is lighter than Kafka+Zookeeper and includes a built-in Schema Registry. |
| **Ports** | 9092 (Kafka API), 8081 (Schema Registry) |
| **Health check** | `rpk cluster health` |
| **Network connections** | materialized reads source data; workload-client may produce test data |
| **Replicas** | 1 |

### 4. materialized (Service — SUT)

| | |
|---|---|
| **Role** | The system under test. Runs environmentd (coordinator) with embedded clusterd (compute/storage workers). |
| **Image** | `materialized` (mzcompose's `Materialized` service, built via `mzbuild`) |
| **Why** | This is the core SUT. The embedded clusterd mode runs everything in one process, simplifying the topology while still exercising all three layers (adapter, compute, storage). |
| **Ports** | 6875 (pgwire), 6876-6878 (API/admin), 6879 (persist pubsub), 26257 (pg-compat) |
| **Health check** | `curl -f localhost:6878/api/readyz` (interval 1s, start_period 600s) |
| **Network connections** | postgres-metadata (consensus), minio (blob), redpanda (sources) |
| **Replicas** | 1 |
| **Key environment** | `MZ_NO_TELEMETRY=1`, `MZ_SOFT_ASSERTIONS=1`, `MZ_CATALOG_STORE=persist`, `MZ_BOOTSTRAP_ROLE=materialize`, `MZ_UNSAFE_MODE=1` |
| **Key command args** | `--unsafe-mode`, `--persist-blob-url=s3://minioadmin:minioadmin@persist/persist?endpoint=http://minio:9000/&region=minio`, `--environment-id=...` |
| **Depends on** | postgres-metadata, minio |

**Design decision**: Use embedded clusterd (single process) rather than separate clusterd containers. This reduces state space while still exercising all code paths. Separate clusterd testing can be added as a second topology later.

### 5. workload-client (Client — Test Driver)

| | |
|---|---|
| **Role** | Runs Antithesis test commands. Emits `setup_complete`. Contains test templates. |
| **Image** | Custom image built on top of testdrive or a Python-based client |
| **Why** | Exercises the system via SQL (pgwire), produces Kafka messages, and asserts properties via the Antithesis SDK. |
| **Ports** | None exposed |
| **Network connections** | materialized (pgwire:6875), redpanda (Kafka:9092, SR:8081) |
| **Replicas** | 1 |
| **Test template mount** | `/opt/antithesis/test/v1/materialize/` |

The workload client needs:
1. PostgreSQL client library (psycopg2 or psql) to issue SQL
2. Kafka producer library to push test data
3. Antithesis Python SDK for assertions and lifecycle signals
4. Test command scripts with appropriate prefixes (`first_`, `parallel_driver_`, `eventually_`, `finally_`)

## SDK Selection

| Component | Language | SDK Needed |
|-----------|----------|------------|
| workload-client | Python | `antithesis-sdk` Python package — for assertions, lifecycle signals |
| materialized (optional, future) | Rust | `antithesis-sdk` Rust crate — for SUT-side reachability/safety assertions |

The workload client **must** have the SDK for emitting assertions. SUT-side Rust SDK instrumentation is optional but recommended for deeper coverage of internal invariants (persist CaS correctness, frontier monotonicity, catalog consistency).

## mzcompose Integration Path

### Option A: Static Docker Compose (Recommended for v1)

1. Write an `mzcompose.py` that defines the topology above
2. Run `mzcompose --find antithesis gen-docker-compose` (or equivalent) to emit YAML
3. Add any Antithesis-specific adaptations as needed
4. Place the resulting `docker-compose.yml` in `guest/opt/materialize/`

### Option B: Dynamic mzcompose (Future)

1. Package the entire mzcompose framework into the workload-client image
2. Use a `first_` test command to generate and start the compose topology
3. More flexible but more complex; requires mzcompose to work inside Antithesis

Option A is the pragmatic choice. It generates a compose file that Antithesis can directly manage.

## Workload Design (High Level)

Test commands in `/opt/antithesis/test/v1/materialize/`:

| Command | Type | Purpose |
|---------|------|---------|
| `first_setup.sh` | first_ | Create sources, materialized views, tables. Establish baseline state. |
| `parallel_driver_sql_workload.py` | parallel_driver_ | Continuously run SQL operations: INSERTs, SELECTs, CREATE/DROP views. Assert consistency properties. |
| `parallel_driver_kafka_producer.py` | parallel_driver_ | Produce messages to Kafka topics. Verify they appear in materialized views. |
| `eventually_consistency_check.py` | eventually_ | Verify that all acknowledged writes are visible in materialized views. |
| `finally_invariant_check.py` | finally_ | Final consistency sweep: compare source data with MV contents. |
| `anytime_health_check.sh` | anytime_ | Verify system health endpoint and basic SQL connectivity. |

## Assumptions

- Embedded clusterd (single process) is sufficient for initial testing
- PostgreSQL is the preferred metadata store (simpler than CockroachDB)
- Redpanda is preferred over Kafka+Zookeeper (lighter, built-in schema registry)
- The workload client will be Python-based (leveraging existing testdrive patterns)
- Static Docker Compose generation (Option A) is the right starting point

## Open Questions

- Should we also test with external clusterd processes (separate compute replicas)?
- Should materialized be subject to fault injection, or only the network between it and dependencies?
- What is the best base image for the workload client — extend the existing testdrive image or build from scratch?
- Should the workload client use testdrive's `.td` format or raw SQL via psycopg?
