# Persist Shared Log: Testing & Verification Strategy

## Overview

The shared log is verified through three complementary approaches, ordered
from most abstract to most concrete:

1. **Semi-formal methods (Stateright)**: exhaustive model checking of the
   protocol's state space. Finds protocol-level bugs.
2. **Deterministic simulation testing (DST)**: exercises the real Rust code
   under simulated faults with deterministic scheduling. Finds
   implementation-level bugs.
3. **Stress testing (open-loop)**: exercises the real system under realistic
   production load. Finds performance and scalability issues.

Each layer catches a different class of bugs. Together, they provide high
confidence that the system is correct, performant, and resilient.

## Layer 1: Semi-Formal Methods (Stateright)

### Purpose

Exhaustively verify that the protocol's safety and liveness properties hold
across all reachable states, including adversarial interleavings that are
unlikely to occur in practice.

### What Is Modeled

An abstract state machine that captures:

- CAS operations with seqno-based preconditions
- Truncate operations
- Multiple concurrent writers and readers
- Indeterminate CAS outcomes (the `compare_and_append` call succeeds but
  the caller cannot confirm; modeled as nondeterministic ghost state)
- Batch ordering and within-batch position ordering

### What Is Not Modeled

- Blob storage, actual bytes, the data plane
- Persist internals (compaction, rollups, GC)
- Network topology or RPC specifics
- Performance characteristics

### Properties Verified

See [02_invariants.md](02_invariants.md) for the full list. The Stateright
model verifies all safety properties (L1-L5, C1-C5, T1-T3) and liveness
properties (Lv1-Lv4) within a bounded state space.

### State Space Bounding

The model uses finite bounds to make exhaustive checking tractable:

- `max_seqno`: maximum seqno value (e.g. 3)
- `max_batch`: maximum number of batches
- `max_writers`: number of concurrent writer actors
- `max_shards`: number of client shard keys

Multiple configurations are checked with varying bounds to increase
coverage.

### Relationship to Implementation

The Stateright model is a parallel codebase: it verifies the protocol
design, not the Rust implementation. If the protocol changes, the model must
be updated. The model and implementation are connected through shared
invariant definitions (the properties in
[02_invariants.md](02_invariants.md)) and through DST assertions that check
the same properties on real code.

### Running

```bash
cargo test -p mz-persist-stateright
```

## Layer 2: Deterministic Simulation Testing (DST)

### Purpose

Exercise the real Rust implementation under controlled, reproducible
conditions with fault injection. DST catches bugs where the implementation
diverges from the protocol: off-by-one errors, missed edge cases in async
code, race conditions in the listen/evaluate pipeline.

### Framework

Uses [turmoil](https://github.com/tokio-rs/turmoil) for deterministic
scheduling and network simulation. All randomness is seeded, so any failing
test can be reproduced exactly by re-running with the same seed.

### Architecture

```
turmoil simulation
├── persist infrastructure (in-memory blob + consensus)
├── shared log acceptor (real PersistAcceptor code)
├── shared log learner (real PersistLearner code)
├── writer clients (submit CAS proposals at configured rate)
├── reader clients (issue head/scan reads)
└── fault injector (partitions, delays, restarts)
```

Each writer and reader runs as a turmoil host with its own persist client.
The fault injector is a dedicated client that introduces network partitions
and repairs on a schedule.

### What Is Tested

- **Write uniqueness**: Successful CAS proposals for the same client shard
  produce distinct seqnos.
- **Write ordering**: Committed seqnos for a client shard are strictly
  increasing.
- **Read consistency**: A `head` read returns state consistent with some
  prefix of committed writes.
- **Liveness**: Under faults, writes and reads eventually complete.
- **Linearizability** (planned): Full linearizability checking using
  invoke/respond history analysis.
- **State machine invariants** (planned): Assert the properties from
  [02_invariants.md](02_invariants.md) on every `StateMachine` state
  transition.

### Fault Injection

- **Network partitions**: Isolate acceptor from learner, or learner from
  clients.
- **Learner restarts**: Kill and restart a learner. Verify it rehydrates
  via `listen(as_of=since)` and catches up.
- **Acceptor restarts**: Kill and restart the acceptor. Verify in-flight
  proposals are retried by clients.
- **Message delays**: Introduce latency on specific network links.

### Seed Exploration

Two modes:

1. **Targeted**: Run with a specific seed to reproduce a known failure.
   ```bash
   SEED=42 cargo test -p mz-persist-client --features turmoil -- internal::sim
   ```

2. **Fuzzing**: Run with many seeds to explore the state space.
   ```bash
   cargo test -p mz-persist-client --features turmoil -- fuzz_persist_dst --ignored
   ```
   This runs indefinitely, cycling through seeds.

### Invariant Checking

_Post-hoc_: After the simulation completes, all recorded operations are
checked against invariants. The `OperationLog` collects all write and read
events from all clients, and the checker verifies them.

_Inline_ (planned): Assert invariants in `StateMachine::apply_cas` and
`StateMachine::apply_truncate` on every state transition during simulation.

## Layer 3: Stress Testing (Open-Loop)

### Purpose

Validate that the system meets performance targets under realistic
production load. Unlike DST (which tests correctness under faults), stress
testing measures throughput, latency, and resource utilization under
sustained load.

### Methodology: Open-Loop

Stress tests use an _open-loop_ workload generator: clients submit proposals
at a fixed rate regardless of whether previous proposals have completed.

Closed-loop generators (wait for response before sending next request)
automatically throttle when the system is slow, masking backpressure issues
and queuing effects. Open-loop generators expose these problems: if the
system cannot keep up with the offered load, latency grows unbounded and the
test fails, which is the desired signal.

### Target Workload

| Parameter        | Value                    |
|------------------|--------------------------|
| Writers          | 10,000 concurrent        |
| Write rate       | 10 Hz per writer         |
| Payload size     | 4 KiB per proposal       |
| Aggregate rate   | 100,000 proposals/s      |
| Flush interval   | 20ms                     |
| Duration         | Sustained (minutes+)     |

This represents the target production workload: 10,000 client shards each
ticking at 10Hz with moderately-sized state updates.

### Metrics

**Throughput:**
- Proposals accepted per second (acceptor)
- Proposals evaluated per second (learner)
- Batches flushed per second (acceptor)
- Proposals per batch (batch efficiency)

**Latency (histogram):**
- End-to-end CAS latency (client submit to result received)
- Acceptor flush latency (pending to `compare_and_append` complete)
- Learner lag (acceptor upper minus learner listen frontier, in batches and
  wall time)
- Read linearization latency (read issued to read served)

**Resources:**
- CPU utilization (acceptor, learner, client)
- Memory utilization (learner StateMachine, result cache)
- Network bandwidth (transport, persist pubsub)
- Object storage API calls (writes from persist, reads on rehydration)

### Acceptance Criteria

From [02_invariants.md](02_invariants.md) performance properties:

| Metric                     | Target                 |
|----------------------------|------------------------|
| Aggregate throughput       | 100,000 proposals/s    |
| CAS p50 latency            | < 25ms                 |
| CAS p99 latency            | < 50ms                 |
| Read p50 latency           | < 5ms                  |
| Read p99 latency           | < 15ms                 |
| Learner rehydration        | < 10s                  |
| Batch efficiency           | ~2,000 proposals/batch |

### Known Scaling Considerations

- **Connection pooling**: A single HTTP/2 connection bottlenecks at ~10K
  concurrent streams due to h2 mutex contention. Multiple connections with
  shard-based distribution address this.
- **Learner memory**: The StateMachine holds all client shard state in
  memory. At ~300 entries/shard with 4KiB data, 10K client shards is roughly
  12 GiB.
- **Result cache sizing**: At 100K proposals/s with 10K batch retention, the
  cache holds roughly 2s of results. Clients must retrieve results within
  this window.

## How the Layers Compose

```
Stateright ──── verifies protocol design ────────── finds design bugs
     │
     │ shared invariant definitions
     │ (02_invariants.md)
     ▼
   DST ──────── verifies implementation ─────────── finds code bugs
     │           under faults
     │
     │ same code, realistic load
     │
     ▼
Stress test ─── verifies performance at scale ───── finds scalability bugs
```

A property failure at any layer is a bug:
- Stateright failure: protocol design bug. Fix the protocol.
- DST failure: implementation bug (protocol is sound). Fix the code.
- Stress test failure: scalability bug. Fix the implementation or revise
  targets.

## Ongoing Verification

These are designed for continuous use:

- **Stateright**: Runs in CI on every change to the protocol model. Fast
  (seconds).
- **DST with targeted seeds**: Runs in CI as a regular test. Fast (seconds
  per seed).
- **DST fuzzing**: Runs continuously in the background, exploring new seeds.
  Failures are captured as regression seeds.
- **Stress tests**: Run on demand or in nightly CI against a deployed
  cluster. Reports metrics for trend analysis.
