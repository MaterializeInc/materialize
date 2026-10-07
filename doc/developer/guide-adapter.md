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

Read-protection acquisition can itself publish catalog metadata. Keep pending
timeline acquisitions with timeline maintenance, not inside committed-diff
enactment. Creator birth grants stay live through implications and transfer into
timeline holds during transaction completion. A retryable acquisition must not
turn a committed statement's completion into a reason to replay that statement.

Prepared DDL may retain selected plans and input holds across a definitive
metadata conflict, but not an unfinished protection publication. Recheck the
planning revision and protection incarnation before reuse, and rebuild creator
protection for each commit attempt. A DROP cascade requires the full structural
revision, not merely continued existence of its named dependencies. A same-statement
replan releases its DDL guard and wakes queued statements before reacquiring it.
Queued continuations must reject terminated connections even after their cancel
watches have been removed.

Written-plan preparation may also yield while acquiring protection. Retain the
candidate, selected plans, current optimized replacement and earlier input holds
across metadata contention. Resume the same fixed-timestamp acquisition against
fresh permissions. Only an actual change of usable imports requires optimizing
that replacement again. Validate the planning revision and incarnation before
reuse, and keep preparation separate from permission to commit or execute.

Explicit DDL COMMIT extracts the session transaction before its first attempt.
Its continuation must own the extracted operations and completion effects, not
run transaction cleanup again on retry. Structural invalidation fails that
transaction rather than replanning already acknowledged statements. Finalize
session variables on every terminal outcome, including cancellation, and run
completion effects only after a definitive commit.

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

### Background reconcilers own convergence resource failures

When a catalog mutation writes desired state that a background reconciler
materializes, the sequencer must not predict the reconciler's transient resource
footprint. The prediction would have to reproduce every strategy that contributes
to desired state, along with their sharing and shedding rules. It will diverge as
those strategies evolve.

The sequencer should validate properties intrinsic to the requested state, such
as valid replica sizes, availability zones, and role permissions. The catalog
transaction enforces resource limits against concrete creates. If a reconciler
cannot apply those creates, it owns the response and the durable or logged
observability for that outcome.

Catalog accounting and downstream side-effect ordering must agree. If one
transaction nets replacement drops against creates, its implications must queue
those drops before the creates. An orchestrator can retry a failed create
indefinitely. Queuing the drop behind it would deadlock a replacement against
the same physical quota that catalog accounting correctly considered available.

### Frontend sequencing and worker stacks

`SessionClient::execute` heap-allocates its large execution-attempt future so it
does not inflate every caller's connection state machine. Inline async state can
produce multiple stack copies at each level of a poll chain, including tracing
wrappers. A stack overflow in a leaf planner function need not mean recursive
planning. Keep large sequencing state behind this boundary rather than increasing
worker stack sizes or moving the allocation burden into each frontend protocol.

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

`BatchingTimestampOracle` drains the queued `read_ts` requests and serves each
collected batch with one call to the backing oracle. That call occurs during
every collected request's real-time interval, after each request arrived and
before any returns. Its timestamp is therefore within bounds for every request.
Batching can only push timestamps later, never earlier.

Coalescing is opportunistic. A request that overlaps a backing call but arrives
after the queue is drained waits for a later call. A serial await loop
guarantees that its calls cannot coalesce. When exactly one round trip is
required, use one explicit shared call only if it occurs within every
operation's real-time bounds and satisfies every caller's contract.

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

### System-session replanning does not grant authority

Some DDL paths reconstruct and mutate a stored definition by replanning it with
a system session. The initial authorization check only sees dependencies in the
submitted statement, so it cannot authorize retained dependencies discovered
during replanning. Before reading secrets, performing external I/O, or
persisting the result, authorize the final dependency set against the invoking
session. Check the final set rather than the union of old and new dependencies,
so a caller can remove a dependency they are no longer authorized to use.

This rule applies when reconstructing or mutating a definition. Executing a
fixed connection does not authorize its dependencies separately. For example,
standalone `VALIDATE CONNECTION` is delegated by `USAGE` on the connection and
its containing schema, without requiring `USAGE` on referenced secrets.

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

Runtime commands use this protocol. Ordinary writes allocate T from the shared
oracle, while OCC writes supply a target chosen by the session task:

```text
target T -> advance catalog upper to T+1 -> compare-and-append txns shard at T
                                          | conflict -> report actual txns upper, retry
                                          ` success  -> apply_write(T) -> acknowledge
```

OCC chooses `T = max(retry_lower_bound, min(F, W+1))`, where F is the subscribe
frontier and W the shared oracle write timestamp. Below, R is the oracle read
timestamp and U the txns-shard upper. The initial lower bound is
`as_of+1`. A conflict raises it to the actual txns upper. Each attempt waits for
`F >= T` and refolds all diffs strictly below the new T. The cap `T <= W+1`
holds at choice time unless a reported txns upper raises the lower bound above
it. Oracle allocations alone are not conflicts and must not reject an otherwise
valid target. Persist's compare-and-append decides whether T is still writable.

The successful timestamp is applied only after the txns write is durable.
`apply_write(T)` sets `R' = max(R, T)`, not necessarily `R' = T`. Advancing the
catalog upper to `T+1` preserves its bound above R because runtime paths that
raise R already make the catalog upper exceed their argument. Bootstrap's
`apply_write(catalog_upper)` can temporarily leave R equal to the catalog upper.

#### OCC real-time ordering and completion before acknowledgement

Txns writes and catalog content commits apply their timestamps to the shared
oracle before acknowledging, and strict serializable reads wait for R to reach
their chosen timestamp. Catalog content commits follow durability, completion
(including `apply_write`), then acknowledgement. Heartbeats carry no user-visible
DDL and do not need to apply a timestamp.

OCC selects `as_of >= R_s`, where `R_s` is read from the shared oracle after the
statement begins, and writes at `T > as_of`. Thus every operation acknowledged
before the statement began has timestamp `<= R_s < T`. This is the real-time
ordering proof, not an assumption that T exceeds the current R or W. Without
catalog completion before acknowledgement, an UPDATE following an acknowledged
ALTER could be ordered before that ALTER.

Concurrent reads may have `X >= T`. Reads depending on the txns write wait for
`U > X >= T`, so Persist decides the write before those reads are served. Reads
below T exclude it. Reads not gated by U have content independent of the write,
including a REFRESH view between refreshes whose last refresh is below T.
Linearizing `as_of` before the loop still matters for zero-row answers and for
the initial bound `as_of+1 <= W+1`. This does not eliminate waits for lagging
inputs or for the initial snapshot after catalog progress outpaces the txns WAL.

#### Generation barrier

On `environmentd` bootstrap in read/write mode:

```text
catalog fence -> set up and register tables -> txns write advances table uppers
              -> snapshot and reset system tables -> start serving
```

Bootstrap snapshots use the table-fence timestamp, not a subsequent oracle read.
Catalog publishers can advance the shared oracle without advancing the table WAL.
Waiting for that newer timestamp can deadlock bootstrap before normal table
progress starts.

The snapshots cannot complete until the txns write has advanced the table uppers. Therefore:

```text
stale-generation target T <= B -> may land before barrier, never after it
stale-generation target T > B  -> catalog advance observes fence before txns CAS
```

A system-table write before the barrier is included in the reset. A user-table write remains
visible to later reads. Let `C_f` be the new generation's catalog fence commit
and B its barrier write. Bootstrap applies the catalog upper to the oracle
before allocating B, so `B > C_f`. After the barrier, a stale generation could
land only at `T >= B+1`. Its cached catalog upper is `<= C_f`, hence
`T+1 > C_f >= cached upper`. Its preceding `advance_upper(T+1)` cannot be a
no-op and must observe the fence durably before the txns compare-and-append.
This proof is independent of W and does not require T to become the new R.

### Durable sink progress can precede controller observations

A sink commits external output before advancing its Persist progress shard, then
reports that progress to the controller. A query client can observe the durable
upper before the controller processes the report. Sink alteration must establish
input overlap against durable progress, not reject committed desired state using
a lagging controller observation. Waiting for that report on the coordinator loop
can also block the loop that must process it.

This applies to the sink's output-progress shard, not a source remap shard. Remap
progress alone does not establish completion of source output.

### Planning eligibility is not execution readability

The catalog determines index candidates and transaction eligibility, even
before replica installation. Runtime observations must not change that logical
domain without DDL. Index creation commits a justified initial `as_of` and
compaction bound with protection for both logical inputs and the selected plan's
actual imports. Read acquisition uses that committed protection before fixing a
transaction timestamp, without requiring an installed trace. Neither a bare
catalog entry nor an assumed `MIN` establishes a valid frontier.

The creating adapter commits its timeline requirement in the same transaction as
index admission and adopts the grant into local tokens before another publication.
Timeline maintenance does not wait for installation. Bootstrap acquires these
windows for existing indexes before reconstruction, retaining them through the
ordinary policy handoff. This prevents a scheduled MV's future upper from advancing
an index past the serving oracle window while that client remains protected.

SELECT and nonexecuting EXPLAIN share read-hold acquisition and preserve their
transaction effects. EXPLAIN does not wait for hypothetical execution, including
on zero-replica clusters. Actual execution separately checks import readability
and waits for required progress. Installation and plan changes must honor valid
protection, not skip required history because a chosen import compacted further.

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

### Session records in the durable catalog

**What:** Write a durable catalog record (`StateUpdateKind::Session`) on every
session connect and delete it on close, so that `mz_sessions` becomes a
materialized view over `mz_catalog_raw` and cleanup logic has a durable
session inventory.

**Why it was rejected:** Connection lifecycle events are far more frequent
than DDL, and the catalog shard has a single writer. Every connect became a
timestamp oracle round-trip plus a compare-and-append against the catalog
shard, serialized on the coordinator loop. Startup could not respond before
the record was durable (otherwise temp DDL could race its own session
record), so connect latency was coupled to catalog commit latency, and
connection churn queued real DDL behind session commits. Batching session ops
into shared catalog transactions and bounding the flush rate reduced the
commit count but kept both couplings.

The durable records also bought nothing for garbage collection in the
single-envd world. Cleanup at promotion deletes all ephemeral rows, justified
by the deploy-generation fence alone, and graceful session close knows the
session UUID from in-memory connection metadata.

**The general lesson:** high-frequency per-connection state belongs in builtin
tables written through group commit, which is fire-and-forget from the
coordinator loop, batched with all other builtin writes, and never touches
the catalog shard. Reserve durable catalog writes for state that must be
transactional with DDL. See
`doc/developer/design/20260706_sql_150_durable_temporary_objects.md`.
