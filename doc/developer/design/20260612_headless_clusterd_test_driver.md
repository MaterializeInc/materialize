# Headless clusterd test driver

## Summary

This document designs a headless test driver for `clusterd`.
The driver is a generic alternate frontend that replaces `environmentd`'s controller for scripted compute and storage tests.
It hosts the persist infrastructure, drives the cluster command and response protocol over the wire, accesses persist directly, and asserts on responses.
The design separates two layers.
The mechanism is the generic headless frontend and stays free of any particular workload.
A use case is a test written on top of the mechanism, such as building an index over a large shard, and lives outside the core.

## Motivation

`environmentd` couples the cluster protocol to the full SQL and catalog stack, which makes targeted compute and storage experiments slow to set up and hard to control.
A test that wants to drive a specific dataflow against specific shard contents must currently go through SQL, the catalog, the optimizer, and the controller's timestamp and read-hold machinery.
A headless driver removes that coupling by speaking the cluster protocol directly, so a test controls the exact persist state, the exact commands the replica receives, and the exact timestamps.
This gives a faithful exercise of the real worker process and protocol while keeping the test deterministic and scriptable.
The same crate runs both as a `cargo test` for a fast local loop and as an `mzcompose` workflow against a real `clusterd`.

## Layering

The central constraint is that the mechanism is generic and the use cases are not.
The mechanism knows how to host persist, connect to a replica, send any command, read or write any shard, and observe responses.
It does not know about index building, data sizes, or timestamp distributions.
A use case composes those primitives into a workload and its assertions.
This keeps the mechanism reusable for compute and storage tests that have nothing to do with the motivating index scenario.

```mermaid
graph TB
  subgraph uc[use cases: tests and examples]
    S1[index over large shard]
    S2[other compute or storage scenarios]
  end
  subgraph mech[mechanism: Driver]
    PA[persist access + hosted PubSub]
    CH[CTP command channel + handshake]
    SUB[generic command and dataflow submission]
    AS[assertion primitives]
  end
  uc --> mech
```

## Goals

### Mechanism

* Host the persist PubSub server, as `environmentd` does, so the replica's persist clients receive push notifications.
* Provide a persist client for direct shard access: open shards, write batches, read snapshots, downgrade `since`, observe `upper`.
* Connect to a real `clusterd` over the compute Cluster Transport Protocol (CTP) and replicate the controller handshake.
* Send arbitrary `ComputeCommand`s, including hand-assembled `CreateDataflow`s, and optionally `StorageCommand`s.
* Demultiplex responses: track per-id frontiers, route peek responses, surface status.
* Provide assertion primitives: wait for a frontier to advance within a timeout, peek an index, read a shard snapshot, and count rows.
* Run as a `cargo test` and as an `mzcompose` workflow.

### Use cases (out of the mechanism)

* Produce data by either of two strategies, selected per test:
  * write synthetic rows directly to a persist shard via the persist write API, or
  * submit a dataflow whose sink writes to a persist shard, producing data through the cluster itself.
* The motivating scenario: create a shard of a chosen size, at a single timestamp or spread across many, build an index, and measure or assert.

## Non-goals

* The SQL layer, catalog, optimizer, and timestamp oracle.
* The txn-wal system.
  Direct persist writes target the data shard with `txns_shard = None`.
* Baking any specific workload, data size, or timestamp distribution into the mechanism.
* Optimizer-quality plan generation.
  Dataflows are assembled by the caller or by small shared helpers, not by lowering SQL.
* Automatic controller behavior.
  The driver does not issue `Schedule`, `AllowCompaction`, or read-hold management on its own.
  The test drives every side-effecting command explicitly, which is what makes side effects controllable.

## Architecture

```mermaid
graph LR
  subgraph driver[headless driver process: mechanism]
    T[use-case test: Rust builder API]
    W[persist access]
    C[CTP command channel]
    PS[persist PubSub server]
  end
  subgraph cd[clusterd process: real]
    SRV[compute server :2101]
    WK[timely workers]
    PC[persist clients]
  end
  P[(persist: blob + consensus)]
  T --> W --> P
  T --> C -->|ComputeCommand| SRV --> WK
  WK -->|read or write shard| P
  WK -->|ComputeResponse| C
  PC -.subscribe.-> PS
  W -.notify.-> PS
```

The driver and `clusterd` share a persist blob store and consensus, configured in `mzcompose` via CockroachDB and an object store.
The driver hosts the persist PubSub server so the replica's persist clients receive low-latency notifications, matching the `environmentd` deployment.
A use case drives the flow: it writes or arranges persist state through the mechanism, submits commands, and asserts on responses.

## Mechanism

The mechanism lives in a new crate, `src/clusterd-test-driver`, exposing a library with the `Driver` API and a thin binary for `mzcompose` workflows.

### Persist access and PubSub host

* The driver starts the persist PubSub gRPC server, the same server `environmentd` hosts, and configures both the replica and its own client with that URL.
* It exposes a persist client for generic shard access, not a workload-specific writer.
* Primitives: open a writer or reader for a shard given a schema, append a batch at a chosen `[lower, upper)`, read a snapshot at a timestamp, downgrade `since`, and inspect `upper`.
* Schema choices, encodings, and the contents of batches are caller concerns.

### CTP command channel

* It uses `transport::Client<ComputeCommand, ComputeResponse>` directly, because `ReplicaClient` and `SequentialHydration` are `pub(super)` and unavailable to an external crate.
* It replicates the controller handshake: `Hello { nonce }`, then `CreateInstance(InstanceConfig)`, then `InitializationComplete`.
* The protocol version is `BUILDINFO.semver_version()`.
* A background receive loop demultiplexes responses.
  `Frontiers` updates a per-id output-frontier watch.
  `PeekResponse` is routed to a pending peek by `uuid`.
  `Status` is surfaced for diagnostics.
* The storage CTP channel is structurally the same and is added when a use case needs it; the compute channel is the first cut.

### Command and dataflow submission

* The mechanism submits any `ComputeCommand` the caller constructs, including `CreateDataflow`, `Schedule`, `AllowCompaction`, `Peek`, and `CancelPeek`.
* It provides small, optional helpers for assembling a `DataflowDescription<RenderPlan, CollectionMetadata>` from parts: source imports, objects to build, index exports, and sink exports.
* These helpers are generic building blocks.
  The specific shape of a plan, such as an `ArrangeBy` for an index, is supplied by the use case.

### Assertion primitives

* `expect_frontier(id, target).within(timeout)` waits until the tracked output frontier for an id reaches a target, and fails otherwise.
* `peek(id, ts)` sends `Peek` targeting `PeekTarget::Index { id }` and collects the `PeekResponse`.
* `peek_count` is a convenience over `peek`.
* `snapshot(shard, ts)` reads a shard directly through the persist client for cross-checking.

## Use cases

Use cases are tests and examples built on the mechanism, kept out of the core crate's library surface.

### Data production strategies

* Direct persist write.
  The test opens a writer with `open_writer::<SourceData, (), Timestamp, StorageDiff>`, passing the `RelationDesc` as the key schema and `UnitSchema` as the value schema, then appends batches of `(SourceData(Ok(row)), (), ts, +1)` with `txns_shard = None`.
* Dataflow that writes to persist.
  The test submits a `CreateDataflow` whose sink export writes to a persist shard, producing data through the cluster itself rather than out-of-band.

### Motivating scenario: index over a large shard

* Produce a shard of a chosen size, for example roughly 10 GB, using a production strategy above.
* Choose a timestamp distribution: all rows at one timestamp, or rows spread across a timestamp range with one append per step.
* Build an index by submitting a `CreateDataflow` that imports the shard and applies an `ArrangeBy` on the index key, exporting an `index_export` of `IndexDesc { on_id, key }`, then `Schedule`.
* Set `as_of` to the chosen read timestamp, within `[since, upper)`, and `until` above it.
* Wait for the index frontier to advance, then peek and assert the row count.
* Measure timing or memory, or attach a profiler to the `clusterd` process.

### Further scenarios

* Hydration with deep history.
  Produce a shard with `since` held back and many distinct timestamps up to `upper`, then build a dataflow with `as_of` at `since`.
  Hydration must replay the full history rather than a single snapshot, which stresses the catch-up path and exposes its cost.
* Multi-dataflow plans.
  Submit several `CreateDataflow`s, or a single plan spanning multiple dataflows, against the same replica.
  This is expected to fail today; the value is that the mechanism can express it, so the failure is reproducible and documented rather than only reachable through SQL.
* Controlling side effects.
  Drive the side-effecting commands the controller normally issues automatically, on the test's own schedule: when `Schedule` fires, when and how far `AllowCompaction` advances `since`, and when `UpdateConfiguration` changes parameters.
  This lets a test hold a frontier, delay compaction, or reconfigure mid-flight to observe the replica's response deterministically.

### RenderPlan assembly for the index (resolved)

* Hand-building a `RenderPlan` from outside `mz-compute-types` is impossible: `LirId` has a private constructor and `LetFreePlan`'s `nodes`/`root`/`topological_order` fields are private.
  The "hand-build" option in the original design is therefore not available to an external crate.
* The implementation uses the lowering pipeline `environmentd` itself uses.
  It builds a `DataflowDescription<OptimizedMirRelationExpr, ()>` via `import_source` + `export_index`, lowers it with `Plan::finalize_dataflow`, then converts each object with `RenderPlan::try_from` and attaches `CollectionMetadata` to the source import.
  `LirId`s and topological order come from production code, not hand-rolling.
* This was verified end to end: the `mzcompose` workflow built the index over a persist shard, the index hydrated, and a peek returned the expected row count.

## mzcompose integration

A composition runs CockroachDB for consensus, an object store for blob, a real `clusterd`, and the driver binary as a workflow.

* `clusterd` is configured with the compute controller listen address on `:2101` and the driver's PubSub URL.
* The driver binary connects to `:2101`, hosts PubSub, runs a scripted use case, and exits non-zero on assertion failure.
* Without `environmentd`, the driver is the sole PubSub host, so the replica's persist notifications flow through it.

## Testing

* The driver never spawns `clusterd`; `mzcompose` brings up the full stack (CockroachDB, an object store, `clusterd`, and the driver image) and is the faithful end-to-end path.
  The verified scenario writes synthetic rows to a persist shard, builds an index over it, waits for the frontier, and asserts the peek count.
* Crate-level `cargo test` covers the infra-free units (direct persist write round-trip via `mem://`, spread-timestamp write, response demux merge, dataflow structure).
  The end-to-end integration test (`tests/index_smoke.rs`) skips unless `CLUSTERD_COMPUTE_ADDR` is set, so `cargo test` stays green without a running stack.

## Local runs and profiling

`test/clusterd-test-driver/run-local.sh` runs the whole thing on the host without docker images.
It reuses or starts a CockroachDB container for consensus, uses a `file://` blob directory, builds and launches a local `clusterd`, and runs the `headless-driver` against it.
Because every process is on localhost, one PubSub address (`127.0.0.1:6879`) and one persist location serve both the driver and `clusterd`, so none of the container-networking caveats apply.

To profile `clusterd` (heaptrack, perf, samply), set `WRAPPER` to a command the script prepends to the `clusterd` invocation, e.g. `WRAPPER="heaptrack" ./test/clusterd-test-driver/run-local.sh` or `WRAPPER="perf record -g --" …`.
On exit the script terminates the inner `clusterd` rather than the wrapper, so the profiler observes its child exit and flushes its output before exiting on its own.
For full manual control instead, launch `clusterd` yourself using the command the script prints under "clusterd command:", then run with `RUN_CLUSTERD=0` so the script only drives the already-running `clusterd`.
A release build (`cargo build --release --bin clusterd`, then point the profiler at `target/release/clusterd`) gives representative numbers.

## Interoperability notes

* **Protocol version.** The CTP handshake checks the client version against the replica's.
  The driver uses `mz_persist_client::BUILD_INFO` (a release-versioned crate, synced by `bin/bump-version`) rather than its own crate version, which is `0.0.0` and would fail the check.
* **Peek stash.** The replica stashes large peek results in persist by default (above a 10 KB threshold).
  The driver disables `enable_compute_peek_response_stash` via `UpdateConfiguration` during the handshake so peeks return their rows inline; a use case wanting stashed results would read them back from persist.

## Risks

* The `RenderPlan` risk is resolved: the lowering pipeline is used and verified end to end (see above).
* The persist data-shard schema and `SourceData` encoding must match exactly what a compute persist-import expects, or decoding fails at read time.
  This risk is confined to the direct-write use case; the dataflow-write strategy produces correctly encoded data by construction.
* Bypassing txn-wal means a directly written shard has no transactional coordination, which is acceptable for synthetic single-writer tests but must not be presented as production-faithful storage behavior.

## Future work

* **A scripting language for the driver.**
  Encoding interactions in Rust is the first step, but the goal is something easier to iterate on than recompiling a test.
  A dedicated declarative script, or loading Python scripts (e.g. via `pyo3`) that call the `Driver` API, would let a test author describe persist mutations, replica commands, and assertions without a Rust build cycle.
  The mechanism is already a thin, scriptable surface (`send`, `submit_dataflow`, `schedule`, `expect_frontier`, `peek`, `subscribe_raw`), so a scripting layer binds to it rather than replacing it.
