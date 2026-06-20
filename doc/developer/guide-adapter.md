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

### One writer per catalog-owned resource

The coordinator is the sole writer of catalog state, and a given catalog-owned
resource (for example a cluster's replica set) must have exactly one
decision-maker at a time. When a change introduces a new owner behind a feature
gate, the same change that lets the new owner act when the gate is on must also
stop the legacy path from acting on that resource. A gate that activates the new
writer without disabling the old one creates two writers. They race,
double-apply, or one silently undoes the other. A controller create that the
legacy reconcile path immediately drops is the typical shape.

Reviewing corollary for stacked changes. Verify that the behavior a change's
tests assert is implemented by that change, not by a later change in the stack. A
test that flips the gate on and asserts the new owner's behavior is only valid if
the same change also gates off the legacy path. Otherwise the test passes only
against the full stack, and the change is not correct on its own, so it cannot be
merged, reverted, or bisected independently.

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

### Group commits and generation handover

At runtime, one group committer per `environmentd` serializes txns-shard operations:

```text
append / register / forget -> FIFO group committer -> table-write worker -> txns shard
```

FIFO ordering prevents an append from overtaking table registration or forgetting. Bootstrap is
the only local exception because it runs before the process serves.

Each runtime command uses this protocol:

```text
shared oracle write timestamp -> advance catalog upper -> compare-and-append txns shard
                                                       | conflict -> retry
                                                       ` success  -> apply write to oracle
```

The successful timestamp is applied to the oracle only after the txns write is durable.

On `environmentd` bootstrap in read/write mode:

```text
catalog fence -> set up and register tables -> txns write advances table uppers
              -> snapshot and reset system tables -> start serving
```

The snapshots cannot complete until the txns write has advanced the table uppers. Therefore:

```text
pre-fence write before barrier -> ordered before the snapshot
pre-fence write after barrier  -> `InvalidUppers` -> retry at a fresh timestamp
                                -> catalog advance observes the fence -> old generation exits
```

A system-table write before the barrier is included in the reset. A user-table write remains
visible to later reads. The retry's `advance_to` is above the stale catalog handle's cached upper,
so its catalog check is durable. An `advance_upper` no-op only checks an already-observed fence.

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

## Reviewing adapter changes

Read this guide when reviewing adapter changes, not only when writing them. The
invariants above are what review most easily misses. Recurring heuristics:

- Compare-and-append and TOCTOU. For a decision computed against a state
  snapshot and applied later, confirm the precondition is enforced by the
  transaction, not by an in-memory check before it. See the invariant above. Be
  suspicious of "this is safe because the loop does not yield here" reasoning.
- One writer. For a change that adds a new writer of a catalog-owned resource
  behind a gate, confirm the same change disables the legacy writer when the gate
  is on, and that a stacked change's tests exercise behavior it implements rather
  than a later change's.
- Message-boundary granularity. When a change adds a request or response over
  the coordinator's internal channel, prefer the narrowest high-level question
  over shipping bulk state across the boundary. Check how the existing or legacy
  path obtains the same signal before adding a heavier one.
- Assumptions that only hold today. Flag merge or combination logic whose
  correctness rests on the current, small set of inputs (for example "the later
  writer wins because the only other writer never sets this field"). Ask for the
  invariant to be made explicit or enforced rather than left implicit.
- Comments and rustdoc follow the repository comment guidelines. No chronology
  ("a later PR will ..."), each comment stands on its own, a comment above a
  struct field is about that field, and a term of art (a module's "kernel", say)
  is defined where it is introduced.
