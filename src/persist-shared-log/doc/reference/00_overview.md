# Persist Shared Log: Architecture Overview

## Problem

Each persist shard forms a linearized log by compare-and-setting against its
own key in a shared Consensus backend (e.g. Postgres). Each shard operates independently:
it appends diffs at the source tick rate, and every append is a separate write
to Consensus, ergo the total number of Consensus writes scales linearly with the number
of shards and the tick rate.

This scale factor, coupled with the capabilities of the readily available managed services
that exist to serve as a Consensus backend today, put a low ceiling on our ability to scale
in # of shards and increase our tick rate.

This document outlines the design for a shared log backend for Consensus that trades
off latency in exchange for vastly increased throughput, allowing a Materialize
environment to scale much wider, with potential for solidly subsecond freshness.

## Core Insight

Rather than having each shard independently write to Consensus, we can
collect all of their CAS operations into a single shared log. A
linearizable log is an excellent backend for consensus: modeling every pending
CAS operation as an entry in the log, and the total order of the log
determining which operations succeed. Using a shared log means that we
only need to focus on scaling the durable CAS operations of a single shard,
vastly reducing the write volume and possible contention to the backing store.

Where might we find something to help build such a shared log? Ah, well, part
fun part madness, persist itself already provides all the building blocks
necessary across durability, linearizability, and high availability.

Indeed, the choice for persist's consensus implementation is a shared log
built on persist.

## Terminology

Two kinds of shards appear in this design:

- _client shard_: A shard from a Materialize environment, typically mapping
  to an object like a source or materialized view. Identified by `ShardId`.
- _log shard_: A persist shard used by the shared log to durably store
  proposals. The log shard is the consensus backend for all client shards.
  Also identified by `ShardId`.

## System Decomposition

The shared log decomposes into two roles, both built around a persist
shard (the log shard): an _acceptor_ that writes proposals and a _learner_
that evaluates them. Together, the acceptor and learner fulfill the
operations of the `Consensus` trait.

### Acceptor

The acceptor receives CAS proposals from clients and appends them to the
log shard. It collects proposals in a pending buffer
and, when flushed, atomically appends the entire batch to the log shard via
`compare_and_append`. It returns a _receipt_ `(batch_number, position)` that
uniquely identifies the proposal in the log.

The acceptor is blind to client shard state. It sequences proposals without
evaluating preconditions. Because proposals for different client shards are
independent, batching N proposals into a single `compare_and_append` makes
the cost O(1/flush_interval) instead of O(client_shards*tick rate).

### Learner

The learner subscribes to the log shard and evaluates proposals as they
arrive. The log provides a deterministic total order over all proposals, and
this total order is what allows the learner to resolve CAS outcomes for
client shards. For each CAS proposal, the learner checks whether the
current seqno of the target client shard matches the expected seqno. If it
matches, the proposal commits; otherwise, it is rejected. Either way, the
result is cached so clients can look it up by receipt.

The learner materializes the full state of all client shards in memory as a
map from client shard key to versioned entries. It serves reads (`head`,
`scan`, `list_keys`) from this materialized state with linearizable
guarantees.

Because CAS evaluation is a pure function of the log's total order, all
learners processing the same log arrive at identical client shard state.

### Log Shard

The log shard is a persist shard that stores proposals. Its schema:

| Dimension | Type                          | Meaning                                                                                |
|-----------|-------------------------------|----------------------------------------------------------------------------------------|
| Key (K)   | `(ShardId, Option<u64>, u64)` | `(shard_id, expected, seqno)` — the client shard, CAS precondition, and proposed seqno |
| Value (V) | `bytes`                       | Serialized data (opaque to the log)                                                    |
| Time (T)  | `u64`                         | Batch number, monotonically increasing                                                 |
| Diff (D)  | `i64`                         | Always `+1`; proposals are append-only                                                 |

Persist provides the total ordering, durability, and read scale-out that
the shared log requires.

### Log Shard Backend

The log shard is itself a persist shard, so it needs its own `Consensus`
implementation. Alas, the turtles bottom out here, and the log shard uses
an external system for its root-level CAS operation, leaning on systems like
such as object storage with conditional writes or OLTP databases.

The full stack:

```
Client shards (Materialize environment)
  └─ persist (Consensus trait)
       └─ Shared log service (acceptor + learner)
            └─ Log shard (persist shard)
                 └─ External Consensus (object storage, OLTP database, ...)
```

## Decoupling

The key structural decision is to push client shard consensus resolution
into the log, rather than resolving each client shard's CAS independently.
Client shard proposals enter the log without precondition evaluation. The
total order of the log is established first; CAS resolution for each client
shard follows as a deterministic consequence.

This decouples writer scaling from reader scaling. Writers only need to
append to the log, and readers only need to subscribe to it. The virtual log
([04_virtual_log.md](04_virtual_log.md)) extends this to multiple log shards
for write throughput scaling.

## High Availability

Persist correctly handles multiple concurrent readers and writers on the
same shard. Running multiple learners or multiple acceptors against the same
log shard is allowed, if possibly inefficient (in the case of acceptors).

Multiple learners subscribe to the same log shard independently. Each
materializes identical client shard state from the deterministic evaluation.

Multiple acceptors can write to the same log shard concurrently, with
`compare_and_append` providing sequencing. However, multiple acceptors alone
don't provide write scale-out, because they contend on the same log shard
upper. Write scale-out requires the virtual log
([04_virtual_log.md](04_virtual_log.md)).

Learners rehydrate by opening a persist read handle. Acceptors are
stateless.

## Distribution

The acceptor and learner are designed as independent, distributable units.
They communicate only through the log shard, and the same abstraction works
whether they run as threads on one machine or as processes across machines.

## RPC Interface

The service exposes an RPC interface that satisfies the `Consensus` trait.
Each `compare_and_set` call appends a proposal via the acceptor, then awaits
the result from a learner. Reads are served directly by the learner.

## Related Reading

- Balakrishnan et al., "Taming Consensus in the Wild" (OSR 2024). Validates
  the acceptor/learner decomposition.
- Balakrishnan et al., "Log-structured Protocols in Delos" (SOSP 2021). The
  virtual log concept for scaling writes across multiple log shards.
- [Persist design doc](../20220330_persist.md). The persist abstraction this
  system builds on.
