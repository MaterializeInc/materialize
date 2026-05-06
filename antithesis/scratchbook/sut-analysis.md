# SUT Analysis: Materialize

## System Overview

Materialize is a real-time data integration platform and streaming SQL database written primarily in Rust. It reads change data from PostgreSQL (logical replication), MySQL, Kafka/Redpanda, and webhooks, then maintains materialized views incrementally using differential dataflow. It speaks the PostgreSQL wire protocol, so any psql client or Postgres driver can connect.

The system claims **strict serializability** for interactive queries and provides **incremental, consistent, low-latency** results over streaming data. It does not offer approximate answers or eventual consistency.

## Architecture

### Three-Layer Design

Materialize is organized into three logical layers that run as separate processes:

**1. Adapter Layer (environmentd)**
- Main coordinator process (`src/environmentd/`)
- Hosts pgwire server (port 6875), HTTP API (6878), and internal coordination endpoints
- Parses SQL, plans queries, manages sessions, enforces consistency
- Contains the Catalog (schema metadata) in memory, persisted to durable storage
- Runs a **single-threaded async event loop** on a Tokio runtime for coordination
- Multiplexes ComputeController and StorageController to manage downstream clusters

**2. Compute Layer (clusterd - compute)**
- Worker processes running Timely Dataflow engines (`src/compute*/`, `src/clusterd/`)
- Executes views, maintains materialized views, performs joins
- Stateless — can be rehydrated from storage on crash
- Multiple replicas provide active replication for HA
- Workers parallelize via native OS threads (one per Timely worker)

**3. Storage Layer (clusterd - storage)**
- Worker processes for data ingestion (`src/storage*/`)
- Reads from external sources (Kafka, Postgres CDC, MySQL, webhooks)
- Reclocks source timestamps to Materialize's internal timeline
- Writes to Persist (blob storage + consensus) for durability
- Manages sinks (Kafka sinks with exactly-once semantics)

### Communication Protocols

| Path | Protocol | Details |
|------|----------|---------|
| Client -> Balancerd -> Environmentd | pgwire (PostgreSQL wire protocol) | TLS, port 6875 |
| Environmentd -> Clusterd | CTP (Cluster Transport Protocol) | Length-prefixed bincode over TCP/UDS, ports 2100-2101 |
| Clusterd workers <-> workers | Timely mesh | Generation-epoch protocol, ports 2102-2103 |
| Clusterd -> Persist | HTTP/S3 API | Blob storage writes + consensus CaS |
| Environmentd -> Persist | Direct | Catalog stored in persist shard |
| Clusterd -> Environmentd | Persist PubSub | HTTP on port 6879, state change subscriptions |

### Key Entrypoints

- `src/environmentd/src/environmentd/main.rs` — main server startup
- `src/clusterd/src/bin/clusterd.rs` — compute/storage worker startup
- `src/balancerd/` — stateless connection router
- `src/pgwire/` — PostgreSQL wire protocol implementation
- `src/adapter/` — SQL planning, coordination, session management

## State Management

### Five Tiers of State

1. **Catalog metadata** — table/view/source/sink definitions, roles, clusters
   - Stored in a persist shard (blob + consensus)
   - Reconstructed into `CatalogState` in-memory on startup
   - Mutated via `catalog_transact()` with atomic `TransactionBatch` writes

2. **Source/ingestion data** — rows from Kafka, Postgres CDC, MySQL, webhooks
   - Written to persist shards by storage workers
   - Keyed by Materialize-assigned timestamps (reclocked from source timestamps)

3. **Materialized view data** — output of incrementally-maintained computations
   - Written to persist shards by compute workers
   - Stored as columnar batches in blob storage

4. **Timestamps/frontiers** — read/write boundaries tracking collection completeness
   - `since` (read frontier): minimum time a collection can be read
   - `upper` (write frontier): maximum time written
   - Tracked as `Antichain<Timestamp>` lattice values
   - Global timestamp oracle provides causally-consistent read times

5. **In-flight state** — active dataflow computations, pending peeks, session state
   - Held in memory by compute/storage workers and the coordinator
   - Lost on crash, recovered via replay from persist

### Persistence Architecture

**Blob Storage (S3/MinIO/Azure/Postgres-backed):**
- Immutable data batches (columnar Parquet/Arrow format)
- Rollups (periodic snapshots of shard state for fast recovery)

**Consensus (CockroachDB/PostgreSQL/FoundationDB):**
- Shard metadata: `since`, `upper`, spine structure
- Writer/reader leases with heartbeats
- Sequence numbers (`SeqNo`) for version linearity
- Catalog mutations as `StateUpdate` events

**Atomic Writes:**
- Compare-and-append via `Machine<K,V,T,D>`: writers must match expected `upper` antichain
- Idempotency tokens prevent duplicates on retries
- Fencing via `FenceToken` (deploy generation + epoch) prevents split-brain

## Concurrency Model

### Coordinator (environmentd)
- **Single-threaded event loop** on Tokio runtime
- Processes commands via `tokio::select!` from multiple MPSC channels
- Per-object write locks (`Arc<tokio::sync::Mutex<()>>`) serialize DDL to same object
- Catalog shared as `Arc<Catalog>` for read-only off-thread access; mutations are serialized through the event loop
- Timeline state (`global_timelines`) accessed serially within event loop

### Compute/Storage Workers (clusterd)
- One native OS thread per Timely worker (configurable count)
- Workers coordinate via Timely's internal barriers and distributed snapshot semantics
- Commands received via MPSC channels from controllers
- Worker 0 broadcasts commands to other workers per Timely conventions

### Synchronization Primitives
- `Arc<tokio::sync::Mutex>` for per-object write locks
- `mpsc::UnboundedSender/Receiver` for coordinator internal messaging
- `watch::Sender/Receiver` for per-connection cancellation
- `Arc<Mutex>` (std) for low-contention shared state (metrics, log writers)
- Timely's own worker-to-worker channels for dataflow coordination

## Safety and Liveness Guarantees

### Claimed Safety Guarantees

1. **Strict Serializability** (design doc 20220516): "Transactions in Materialize are strictly serializable with respect to operations inside of Materialize" (SELECT, INSERT, UPDATE, DELETE). All timestamp transitions made durable before response issued.

2. **Definiteness** (design doc 20210831): Collections are "definite" — all uses yield exactly the same time-varying data at each logical time. Data definite for times in range `[since, upper)`.

3. **Exactly-Once Kafka Sinks** (design doc 20200520): Transactional consistency for Kafka sink output with consistency topic.

4. **Acknowledged Writes Survive Failures**: All data written to persist (blob + consensus) before acknowledgment. Catalog mutations durable before response.

5. **Epoch-Based Leader Fencing**: New coordinators increment epoch on startup; old coordinators' transactions fail. Prevents split-brain after coordinator crash.

### Claimed Liveness Guarantees

1. **Persist Reader/Writer Liveness**: "At least one reader/writer can always make progress" even when peers are paused or restarted.

2. **Collection Progress**: "The collection upper advances so long as one writer can make progress."

3. **Active Replication Recovery**: "Masking of recovery delay can only be guaranteed when compute controller can reach at least one non-faulty replica."

4. **Automatic Failover**: Compute replicas automatically rehydrate from storage on crash. Multiple replicas mask recovery latency.

### Limitations
- HA (multi-active replication) is cloud-only; self-managed has single coordinator
- SUBSCRIBE, sinks, and `AS OF` queries may circumvent strict serializability
- No byzantine fault tolerance; system assumes honest coordinator
- Single coordinator bottleneck for timestamp oracle

## Failure and Degradation Modes

### Failure-Prone Areas

1. **Startup/Configuration**: Many `expect()`/`unwrap()` calls in startup path — misconfiguration causes immediate crash rather than degraded operation.

2. **Replica Reconnection**: Infinite retry with exponential backoff (capped at 1s). Can cause minutes-long recovery latency during transient failures. No circuit breakers.

3. **Persist Layer Failures**: No circuit breaker for blob/consensus unavailability. System retries with backoff, creating backpressure rather than failing fast. Bounded retry loops (3-5 attempts) for some storage management operations.

4. **0DT Deployment**: Preflight checks with configurable timeout. Can either panic or proceed degraded if standby doesn't catch up. Read-only promotion before full read-write.

### Health Checking
- `/health/liveness` — always returns 200 (process is alive)
- `/health/ready` — returns 503 until adapter client available; optional `wait=true` blocks
- `curl localhost:6878/api/readyz` used in Docker healthchecks

### Graceful Degradation
- Compute replicas: partial replica failure tolerated; system serves from remaining replicas
- 0DT standby boots read-only, promotes after catching up
- Feature flags return 503 rather than crashing when disabled
- No graceful degradation for metadata store (CRDB/PG) unavailability — system halts

## External Dependencies

| Dependency | Role | Criticality |
|-----------|------|-------------|
| CockroachDB / PostgreSQL / FoundationDB | Consensus for persist + catalog | CRITICAL — system halts without it |
| S3 / MinIO / Azure Blob | Blob storage for persist data | CRITICAL — writes fail without it |
| Kafka / Redpanda | Stream source ingestion | CRITICAL for streaming workflows |
| PostgreSQL (source) | CDC replication source | CRITICAL for CDC workflows |
| MySQL (source) | CDC replication source | Optional |
| Schema Registry | Avro/Protobuf schema management | Required for typed Kafka sources |
| Balancerd | pgwire connection routing | CRITICAL for multi-tenant |

## Existing Test Strategy

### mzcompose Framework (`misc/python/materialize/mzcompose/`)
- Meta-test framework generating Docker Compose files dynamically
- `Composition` class loads `mzcompose.py` files, discovers `workflow_*()` functions
- Pre-built service classes: `Materialized`, `Clusterd`, `Kafka`, `Redpanda`, `Postgres`, `CockroachOrPostgresMetadata`, `Minio`, `Toxiproxy`, etc.
- Granular lifecycle control: `c.up()`, `c.kill()`, `c.stop()`, `c.pause()`, `c.override()`
- Generates YAML on-demand, passes to `docker compose` via file descriptors
- Health-check driven startup with configurable intervals

### Test Frameworks
1. **testdrive (.td)** — declarative SQL test language with timeout assertions and version-conditional tests
2. **sqllogictest (.slt)** — standard SQL logic test format for correctness
3. **Platform Checks** — "write once, run everywhere" tests across upgrade/restart/failure scenarios
4. **parallel-workload** — random concurrent SQL operations stress testing

### Failure Testing Coverage
**Tested**: clusterd crashes/recovery, CockroachDB restarts, network faults (Toxiproxy), failpoint injection, statement timeouts, source/sink resilience, 0DT deployments

**Not tested at scale**: coordinated multi-node cascading failures, deterministic replay of timing-sensitive bugs, property-based invariant testing under adversarial fault injection — this is where Antithesis adds value

## Assumptions
- The mzcompose-based Docker Compose approach is the right integration path (vs. K8s)
- The existing Antithesis K8s-based experiment scripts represent an older approach to be superseded
- Materialize's self-managed/community edition (single-node) is the target, not the cloud multi-tenant version

## Open Questions
- Which mzcompose test suite(s) provide the best starting workload? (platform-checks, parallel-workload, or custom)
- What is the preferred metadata store for Antithesis testing — CockroachDB or PostgreSQL?
- Should we test with multiple compute replicas or single replica?
- Are there specific failure scenarios the Materialize team wants prioritized?
