# Adapter Guide

General guidance for working on and reviewing the adapter layer
(`src/adapter/`), the coordinator, pgwire frontend, and related crates. This is
a living document: add to it as you discover invariants, pitfalls, or
non-obvious design decisions.

## Architecture & Key Concepts

### Catalog changes and their implications

A DDL or system change flows through three conceptual phases:

1. The sequencer (`src/adapter/src/coord/sequencer/`) decides *what to write
   durably*. It builds a `Vec<catalog::Op>` and calls one of the
   `catalog_transact*` entry points. It should not reach into controllers or
   mutate downstream in-memory state directly.
2. `catalog_transact` (via `catalog_transact_inner`) commits the ops durably and
   applies them to the in-memory `CatalogState`, producing the committed catalog
   diff.
3. The implications phase (`apply_catalog_implications` in
   `src/adapter/src/coord/catalog_implications.rs`) derives downstream effects
   from that committed diff and applies them: in-memory coordinator state,
   compute/storage controller commands, builtin-table updates. The flow is
   `StateUpdateKind -> ParsedStateUpdate -> CatalogImplication`.

The guiding principle: the sequencer decides durable writes, and applying the
catalog implications is when we update everything downstream of those writes.
Implications are derived from the committed diff, not from the input ops and not
from sequencer closures.

Why derive from the diff? So a side effect fires identically whether this node
applied the change or whether it is following a catalog change made by another
writer. This is the same distributed stance as "No local-only assumptions"
below, and a more scalable multi-`environmentd` coordinator depends on it (PR
#29673, `database-issues#8488`). No code applies another node's diff today, but
the framework is built so that capability is achievable.

Two contracts on the implications phase:

- It is treated as infallible. `catalog_transact_with_context` does
  `.expect("cannot fail to apply catalog implications")`, because a committed
  catalog with unapplied downstream effects cannot be recovered in-process and
  is left to restart and bootstrap.
- It requires consolidated updates: at most one addition and one retraction per
  item. See the `apply_catalog_implications` doc comment.

#### Legacy paths being migrated away from

The migration into the implications framework is incremental and unfinished.
These older patterns still exist and are legacy. Do not add new side-effect
logic to them. Extend the implications framework instead.

- `catalog_transact_with_side_effects` runs a sequencer-provided side-effect
  closure. It carries a `TODO(aljoscha)` to migrate its call sites to
  `catalog_transact_with_context`.
- The op-scan plus the block guarded by "No error returns are allowed after this
  point" in `catalog_transact_inner` (for example `update_compute_config` /
  `update_storage_config`) is keyed off the input ops, so it cannot fire for a
  follower observing a committed diff. It is not where new controller pushes
  belong, even though existing system-config pushes happen there today.
- Several `StateUpdateKind`s are not yet represented as implications.
  `parse_state_update` returns `None` for them via its catch-all arm (this
  covers the environment-wide and cluster-scoped system-config kinds, among
  others; the replica-scoped kind is represented as a `ParsedStateUpdateKind`).
  Representing a new kind may require extending `ParsedStateUpdate` /
  `ParsedStateUpdateKind` first.

## Correctness Invariants

### Timestamp selection must respect real-time bounds

For strict serializability, the timestamp assigned to a query must fall within
the query's real-time interval --- between the moment it arrives and the moment
its response is sent. The design doc
(`doc/developer/design/20220516_transactional_consistency.md`) states this as:

> Each timestamp is assigned to an event at some real-time moment within the
> event's bounds (its start and stop).

This means:

- **Never select a timestamp from before the query arrived.** Doing so would
  allow a query to observe a snapshot that predates its own start, violating
  the real-time ordering constraint of strict serializability.

- **Selecting a later timestamp is always safe**, as long as it is still within
  the query's real-time bounds (i.e. the query hasn't returned yet). Pushing a
  timestamp forward only makes the query appear to have executed later, which
  is fine --- the query was indeed still in-flight at that moment.

#### Why the batching oracle is correct

`BatchingTimestampOracle` collects multiple `read_ts` requests that arrive
concurrently and serves them all with a single call to the backing oracle.
Because the backing oracle is called *during* all of their real-time intervals
(after all requests arrived, before any have returned), the returned timestamp
is within bounds for every request. Batching can only push timestamps later,
never earlier. See the comment on the `BatchingTimestampOracle` struct.

#### Why caching an oracle result is not correct

It is tempting to cache or snapshot the most recent `read_ts` result (e.g. in a
shared atomic) and reuse it for subsequent queries without another oracle call.
This is wrong: the cached value was determined at a real-time moment *before*
the new query arrived, so assigning it to the new query places the query's
timestamp outside its real-time bounds. A query arriving after the cached value
was last updated would receive a stale timestamp --- one that predates its own
start --- violating strict serializability.

**Common incorrect argument to watch for:** "Using a slightly older timestamp
just reads an earlier consistent snapshot, which is valid for linearization."
This confuses serializability (some valid ordering exists) with *strict*
serializability (the ordering must respect real-time). Under strict
serializability, the linearization point must fall within the operation's
real-time interval. A timestamp from before the query arrived places the
linearization point before the query started --- outside its real-time bounds ---
regardless of whether the snapshot itself is internally consistent. The
consistency of the snapshot is not the issue; the issue is *when* it was
determined relative to the query's arrival.

This also interacts with the distributed-system invariant below: the cached
value is local-only state, so it cannot reflect writes applied by other
`environmentd` nodes to the shared backing oracle.

### No local-only assumptions in a distributed system

Materialize is designed to run as a distributed system: multiple `environmentd`
instances may share the same backing store (for example CRDB) concurrently. Any
optimization that relies on local-only state --- such as tracking writes with
an in-process counter and assuming no other writer exists --- is incorrect
unless the backing store is also consulted or the invariant is otherwise
guaranteed system-wide. Always ask: "does this still work if another node is
running the same code against the same backing store?"

### Checklist for timestamp-related changes

Before modifying timestamp selection or oracle interaction, verify:

1. **Real-time bounds**: Is the timestamp determined by an oracle call (or
   equivalent) that happens *during* the query's lifetime (after arrival, before
   response)? If the value could have been determined before the query arrived,
   it violates strict serializability.

2. **Distributed correctness**: Does this work when multiple `environmentd`
   nodes share the same backing oracle? Any in-process cache, atomic, or counter
   that is not synchronized through the backing store is suspect.

3. **Monotonicity**: Can a caller ever observe a timestamp go backwards? Even
   with concurrent batches or out-of-order completions?

4. **Write visibility**: After `apply_write(t)` returns, will all subsequent
   `read_ts` calls (including the fast path, if any) return `>= t`? This must
   hold across nodes, not just within the local process.

### Bounded staleness must anchor against the oracle, not wall clock

The `BoundedStaleness(D)` isolation level picks `T >= oracle.read_ts - D` as
its freshness lower bound. The oracle is the only anchor that satisfies the
user-visible contract `oracle_read_ts(now) - T <= D` across:

- Crashes and restarts: a fresh `NowFn()` after a restart can regress (NTP
  step backward, container migration), so a wall-clock anchor would let a
  post-restart query pick `T` past a previously-served timestamp.
- Clock changes during normal operation.
- A future multi-`environmentd` deployment: the shared oracle reflects the
  max clock across nodes, so a wall-clock anchor on a slow-clock node would
  serve `T` outside the `D` contract by the inter-node skew.

`needs_linearized_read_ts` returns `true` for `BoundedStaleness(_)`, so the
oracle is consulted on every bounded-staleness query (one round-trip, the
same one strict serializable already pays).

The current implementation rejects bounded-staleness queries whose timeline
is not `EpochMilliseconds`, in `determine_timestamp_for_inner`. The freshness
math is currently scoped to that timeline.

### The catalog is the source of truth for state that gets rebuilt from it

If a reconcile or refresh path rebuilds downstream state (for example a
controller's per-replica configuration) from the catalog working copy, then the
values it must preserve have to already be in that working copy. Do not leave
authoritative state only in a controller or in-process structure while a
working-copy-driven rebuild can run. The two diverge, and the next rebuild
silently clears the out-of-band state.

Running the side effect inside the implications phase is necessary but not
sufficient. If an implication sources a value from a fresh evaluation and pushes
it straight to a controller without that value also being in the catalog (and
therefore in the diff a rebuild reads), a later rebuild from the working copy
will still drop it.

Concrete shape of the bug. A create-time implication evaluates a per-replica
controller override and pushes it into a controller's per-replica layer, but
does not write it into the catalog working copy. A later reconcile rebuilds the
complete per-replica map from the working copy, which lacks the new replica, and
the controller clears every replica absent from that map, reverting the
override. The override only reappears on the next periodic sync that reconciles
the replica into the working copy. For render-frozen settings that window is a
correctness gap, not just a delay. The fix is to make the catalog the source of
truth: write the value through the create transaction so the diff, and any later
rebuild, include it.

### A compare-and-append must be enforced by the transaction, not by a check before it

A decision computed against a snapshot of catalog state and applied later (for
example a background task that reads state, computes ops off the coordinator
loop, then submits them for the loop to transact) must carry the precondition it
was derived from, and that precondition must be enforced as part of the same
transaction that applies it. Reading the current in-memory catalog, comparing it
to the expected state, and then calling `catalog_transact` is not a
compare-and-append. It is a time-of-check to time-of-use gap.

- It is safe today only by the fragile accident that the coordinator loop does
  not yield between the check and the durable commit. Any await later inserted
  between them, or any move of the check off the coordinator loop, reopens
  the race.
- It does not hold across writers. Another `environmentd` that commits a
  conflicting change to the durable store between this node's in-memory check and
  its durable commit is not detected. The durable layer does not re-validate a
  per-object precondition, so the stale write lands and clobbers. This is the
  same distributed stance as "No local-only assumptions" above.

If you need conflict detection, evaluate the precondition atomically with the
commit, a real compare-and-append against the durable store. A check that merely
precedes the write on the coordinator loop is not that, even when it reads the
right state.

### Catalog mutations must bump the transient revision or stay invisible

Sessions cache catalog snapshots and reuse them while
`Catalog::transient_revision` is unchanged (see
`doc/developer/design/20260709_session_catalog_snapshot_cache.md`). Only
`Catalog::transact` bumps the revision. So when adding a new way to mutate
the Coordinator's in-memory catalog, either route it through `transact` or
keep the change invisible to session-visible catalog reads (name resolution,
planning). Otherwise sessions serve stale catalogs where today they would see
the change.

### The group committer is the single in-process writer to the txns shard

All txns-shard writes (group-commit appends, table registration and forgetting
for DDL) go through the group committer task's queue, one at a time at
monotone oracle timestamps. Do not add another runtime sender to the persist
table-write worker. Queue order is load-bearing: an append to a table must
reach the worker before that table's forget, or the worker has no registered
shard to write to and panics, and only queue order provides this (DROP TABLE
takes no write locks). Single-writer also makes in-process txns-shard
conflicts impossible, so `InvalidUppers` from the worker always means another
process wrote the shard, handled by retrying at a fresh oracle timestamp.
Bootstrap is the one exception (`register_table_collections` and its direct
table appends), safe because nothing else runs during bootstrap. The catalog
shard's analogous conflict protocol lives in the compare-and-append itself
(`commit_transaction` and
`advance_upper` in `src/catalog/src/durable/persist.rs`): empty progress is
rebased over, foreign content surfaces as the graceful `CatalogOutOfSync`.
Full protocol in the module docs of `src/adapter/src/coord/appends.rs`.

## Rejected Optimizations

This section records specific optimizations that have been attempted and found
incorrect. If you find yourself re-proposing one of these, treat it as a strong
signal that the approach is wrong.

### Shared-atomic `read_ts` cache (`peek_read_ts_fast`)

**What:** Add an `Arc<AtomicU64>` to `BatchingTimestampOracle` holding the most
recently observed `read_ts`. Return it from a new `peek_read_ts_fast()` trait
method, bypassing the ticket/watch/CRDB round-trip for 99%+ of reads.

**Why it's wrong:**
- Violates real-time bounds (§ "Why caching an oracle result is not correct"):
  the atomic holds a value from a *previous* oracle call. A query arriving after
  that call completed receives a timestamp from before its own start.
- Violates distributed correctness (§ "No local-only assumptions"): another
  `environmentd` node can `apply_write` to CRDB, advancing the global read
  timestamp. The local atomic has no way to learn about this, so it returns a
  stale timestamp that predates a write already visible to other nodes.
- The safety argument ("reading an earlier snapshot is valid for linearization")
  confuses serializability with strict serializability. See the detailed rebuttal
  in § "Why caching an oracle result is not correct."

**Performance context:** This optimization delivered ~5x throughput improvement
at low concurrency by eliminating oracle round-trips. The performance need is
real, but the solution must maintain strict serializability. Correct alternatives
might include: reducing oracle round-trip latency, colocating the oracle,
using the batching oracle's existing mechanism to serve more callers per batch,
or relaxing the isolation level for queries that opt in.
