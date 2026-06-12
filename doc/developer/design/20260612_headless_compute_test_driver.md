# Headless compute test driver

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

The mechanism lives in a new crate, `src/compute-test-driver`, exposing a library with the `Driver` API and a thin binary for `mzcompose` workflows.

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

### RenderPlan assembly for the index

* The decision is to hand-build the minimal `RenderPlan::ArrangeBy` node rather than reuse the optimizer's lowering.
  This avoids coupling to the lowering API at the cost of risk that a subtly invalid plan is rejected by the worker.
* This choice is explicitly marked for revisiting.
  If hand-construction proves brittle, the fallback is to construct a small MIR or LIR fragment and run the existing lowering to `RenderPlan`, the same path `environmentd` uses.
* If hand-construction is generally useful, the assembly helper graduates into the mechanism; until then it stays with the use case.

## mzcompose integration

A composition runs CockroachDB for consensus, an object store for blob, a real `clusterd`, and the driver binary as a workflow.

* `clusterd` is configured with the compute controller listen address on `:2101` and the driver's PubSub URL.
* The driver binary connects to `:2101`, hosts PubSub, runs a scripted use case, and exits non-zero on assertion failure.
* Without `environmentd`, the driver is the sole PubSub host, so the replica's persist notifications flow through it.

## Testing

* A `cargo test` runs a use case against a `clusterd` started as a child process for a fast local loop.
* The `mzcompose` workflow runs the same use case against a containerized `clusterd`.
* Step-level verification during development: a direct persist write is confirmed by reading the snapshot back and counting rows; the handshake is confirmed by the replica logging instance creation; a dataflow is confirmed by frontier advance; a peek count is confirmed against the known row count.

## Risks

* A hand-built `RenderPlan` may be rejected by the worker.
  Mitigation is the lowering fallback noted above.
* The persist data-shard schema and `SourceData` encoding must match exactly what a compute persist-import expects, or decoding fails at read time.
  This risk is confined to the direct-write use case; the dataflow-write strategy produces correctly encoded data by construction.
* Bypassing txn-wal means a directly written shard has no transactional coordination, which is acceptable for synthetic single-writer tests but must not be presented as production-faithful storage behavior.
