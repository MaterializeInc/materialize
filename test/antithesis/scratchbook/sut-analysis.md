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

## Appendix A: Kafka Source Ingestion (Detail)

Added 2026-05-11 in response to scoping toward Kafka source properties (append-only + UPSERT envelope).

### Pipeline shape

`KafkaSourceReader` → `ReclockOperator` → (optional `decode`) → (optional `upsert` operator) → `persist_sink`.

The dataflow is rendered in `src/storage/src/render/sources.rs`. The reader and metadata-fetcher are constructed by `SourceRender for KafkaSourceConnection` in `src/storage/src/source/kafka.rs`. Reclocking is in `src/storage/src/source/reclock.rs` plus `reclock/compat.rs` (the persist-backed remap handle). UPSERT logic is in `src/storage/src/upsert.rs` (classic) and `src/storage/src/upsert_continual_feedback.rs` / `upsert_continual_feedback_v2.rs` (continual-feedback variants).

### Source-time vs into-time

* **Source time** for Kafka is `Partitioned<RangeBound<PartitionId>, MzOffset>` (`mz_storage_types::sources::kafka`). The frontier is a multi-partition antichain.
* **Into time** is Materialize's `mz_repr::Timestamp` (ms since epoch). The mapping from source time → into time is the *remap shard*: a persist shard whose contents accumulate to a well-formed `Antichain<FromTime>` at every into-time. See `ReclockOperator` doc comment: "for any time `IntoTime` the remap collection accumulates into an Antichain where each `FromTime` timestamp has frequency `1`."
* On startup the remap operator loads existing bindings, downgrades to the recovered upper, then mints new bindings when `mint()` receives a probe.

### Partition handling

* Partition → worker assignment is round-robin by hash: `((source_id + partition_id) % worker_count) == worker_id` (`kafka.rs`).
* New partitions are picked up by the metadata fetcher and routed through reclocking.
* Per-partition offsets are tracked in `last_offsets`. Code-stated invariant: "if we see offset x, we have seen all offsets [0, x-1] that we are ever going to see" (kafka.rs near line 1005).
* Offsets that arrive `<=` `last_offset` are silently dropped (kafka.rs ~1158). This is the path that protects against rdkafka redelivery on reconnect.
* Negative offsets from an otherwise non-errored message cause `panic!` in `construct_source_message` (kafka.rs ~1193).

### Append-only (NONE envelope) workload shape

Decoded rows flow directly into `persist_sink` keyed by Materialize timestamp. Each `(partition, offset)` produces exactly one row (plus metadata columns if requested). There is no retraction unless an upstream EvalError occurs in a downstream operator.

### UPSERT envelope

`upsert_commands` (render/sources.rs) maps each `DecodeResult` into `(UpsertKey, Option<UpsertValue>, FromTime)`:

* `UpsertKey` is a 32-byte SHA-256 digest of the key bytes; collisions are treated as impossible (probabilistic).
* `Some(value)` is an insert/update for `key`; `None` is a tombstone (delete).
* Key decode failures produce `UpsertError::KeyDecode`; null keys produce `UpsertError::NullKey`; value decode failures produce `UpsertError::Value`. These flow as `Err` values keyed by the (errored) key and can be *retracted* by a subsequent good `(key, value)` for the same key — this is the contract that makes "fix the bad message" recovery possible without dropping the source.

The upsert operator (`upsert_classic` in `upsert.rs`) consults a state store (`UpsertStateBackend`) for the prior value before emitting updates. Two backends ship:

* `InMemoryHashMap` — `BTreeMap<UpsertKey, StateValue>`. Lost on restart.
* `RocksDB` — persistent, with a merge operator. Bug history shows the merge operator must always return `Some` or RocksDB aborts the process (commit 0d8d740b47).

State is reconstructed on restart by replaying the persist *feedback* stream (the output of the upsert operator's previous incarnation) up to the resume frontier. The operator passes through a *snapshot* phase that drains all feedback values for keys at or below the resume frontier, then transitions to normal mint-on-input mode.

Key invariants stated in code:

* `assert!(diff.is_positive(), "invalid upsert input")` (upsert.rs:541; mirrored in `upsert_continual_feedback*.rs`) — the upsert operator never sees retractions on its input; only inserts/tombstones.
* `panic!("key missing from commands_state")` (upsert.rs:636) — the operator's internal dedup table must always contain a key it is about to emit for; missing key is a structural invariant violation.
* Order-key monotonicity within a key is enforced by `consolidate_snapshot_chunk` / `drain_staged_input`. A regression here previously caused a panic that was "as close to data loss as possible" (commit f177db8286, issue materialize#26655). The fix skips violating updates rather than panicking.
* In continual-feedback v2: `assert!(diff.is_positive())` again (v2:315) plus `unreachable!()` on `(None, None)` from joined prior/new state (v2:483) and an empty-output assertion in tests (v2:957).

### Reclock invariants and failure modes

* `compare_and_append` on the remap shard can return `UpperMismatch` if a racing writer (e.g. across restart) has advanced the shard. `ReclockOperator::mint` retries by `sync()`-ing and re-minting (reclock.rs:160-166).
* `panic!("compare_and_append failed: {invalid_use}")` in `reclock/compat.rs:306` catches genuinely invalid persist calls (vs. retryable upper mismatch).
* Reclock's cached `upper` has a known staleness pitfall (commit e3805ad790, issue database-issues#8698) — fixed by always fetching the recent upper for `as_of` calculation.

### Statistics and progress signals

`statistics.rs` reports per-source counters that have correctness invariants of their own:

* `offset_known >= offset_committed` (commit 3e32df1f69 enforces clamping after a regression bug).
* `snapshot_records_known >= snapshot_records_staged`, both decrease to zero (clear) at end of snapshot.

These are user-visible numbers and form weak but easily-checkable correctness signals from the workload side.

### Failure-prone areas relevant to Antithesis

| Area | Risk | Code |
|------|------|------|
| Negative offset from rdkafka | hard panic | kafka.rs:1193 |
| Late offset on reconnect | silent drop (correct behavior, but check via `assert_sometimes!(saw_late_offset)`) | kafka.rs:1158 |
| Topic recreated with fewer offsets | previously panicked on capability downgrade (commit 99ad668af5) | source_reader_pipeline / kafka.rs |
| Upsert key with timestamp regression | previously panicked (commit f177db8286) | upsert.rs:475-487 |
| RocksDB merge returning `None` | SIGABRT (commit 0d8d740b47) | upsert/rocksdb.rs |
| Reclock `compare_and_append` UpperMismatch retry loop | unbounded retry, can block forever under persist outage | reclock.rs:160 |
| Multi-replica `drain_staged_input` double-pass | duplicate retractions (commit 1accbe28b3) | upsert_continual_feedback.rs |
| Persist sink cached upper across concurrent sinks | stale read leads to false errors (commit 505dc96aaa) | render/persist_sink.rs |
| Flag flip mid-append on persist sink | spurious `InvalidBatchBounds` (commit 68e1dfd86d) | render/persist_sink.rs |

These are the seeds for the Kafka-specific property catalog in Category 7 of `property-catalog.md`.
