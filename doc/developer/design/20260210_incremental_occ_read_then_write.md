# Incremental, OCC-based Read-Then-Write for Concurrent Writers

## Context

Read-then-write operations (DELETE, UPDATE, INSERT...SELECT) in Materialize
currently rely on in-process pessimistic locking. The Coordinator acquires
write locks before reading, holds them through the write, and releases them
only after the write has been applied to the timestamp oracle. This ensures
that no other write can interleave between the read and write phases of a
single operation.

This approach is correct for a single `environmentd` process, but it does not
extend to multiple processes: the locks are in-memory mutexes that cannot be
shared across process boundaries. We need concurrent multi-process writes for:

- **Zero-downtime upgrades v2**: the old and new `environmentd` processes must
  both be able to serve writes during the handover window
- **High availability**: multiple `environmentd` processes must be able to
  serve queries concurrently
- **Physical isolation**: separate serving-layer processes (aka.
  `environmentd`) for different workloads

The locks are also weaker than they look within a single process. They pin only
the selection's direct dependency items, while the statement reads at one
timestamp and commits at a later one, so a write to an input that is not itself
a dependency item can change what the statement should have read. That admits
non-serializable histories today. Moving to OCC fixes this as well, see the
strengthening under Deliberate differences.

This design doc proposes replacing the pessimistic locking approach with
optimistic concurrency control (OCC) for read-then-write operations, backed by
a subscribe that continually tracks the current state of the data.

## Goals

- Make read-then-write operations correct with concurrent writers, including
  writers in different `environmentd` processes
- Provide at least the same user-visible semantics as the current
  implementation: writes are based on the latest committed state of the table at
  the time the write is applied. The OCC path is in fact strictly stronger, see
  the strengthening noted under Deliberate differences
- Don't regress performance within reasonable bounds

## Non-Goals

- High-performance writes under heavy contention. The current implementation
  serializes writes behind a global lock. The OCC implementation serializes
  them via retries. Neither is designed for high write throughput.
- Removing the in-process locks immediately. During rollout, the old lock-based
  path and the new OCC path coexist behind a feature flag. The locks can be
  removed once the OCC path is fully rolled out.
- Mixed read/write transactions. A write that reads persisted state commits at
  the frontier it observed, which it cannot postpone until COMMIT, so it runs
  only as a single statement. A write that reads nothing does compose with
  transactions: its diffs are frontier-independent, so they are buffered as
  session write ops and land when the transaction commits. That covers, for
  example, `INSERT INTO t SELECT generate_series(1, 20000)`, whose values are
  constant but too large to fold into a literal.

## Overview

The core idea is to replace the "lock, peek, write" sequence with a
subscribe-based OCC loop:

1. Open a subscribe on the read expression (the `selection` from the
   `ReadThenWrite` plan), starting at the timestamp determined by the oracle
2. Accumulate diffs from the subscribe
3. When the subscribe frontier advances to T (meaning we have a consistent
   snapshot), attempt to write the accumulated diffs at timestamp T
4. If the write succeeds, done
5. If the write fails because another writer already committed at timestamp T,
   the subscribe will deliver the new state; go back to step 3 with the updated
   diffs

This approach is correct by construction: the subscribe always reflects the
committed state of the data, and the timestamped write mechanism ensures that
the write is applied at exactly the timestamp the diffs were computed for. If
anything changes between the read and the write, the write fails and is retried
with fresh data.

Below we describe the current approach, the proposed approach, correctness
arguments, and performance implications.

## The current lock-based approach

The current `sequence_read_then_write` in the Coordinator works as follows:

1. **Acquire write locks**: For each table involved in the read-then-write, an
   in-process `OwnedMutexGuard` is acquired. All locks are acquired atomically
   (all-or-nothing) to prevent deadlocks. If any lock is unavailable, the
   entire operation is deferred until the lock becomes available.
2. **Peek**: A one-shot peek is executed at `QueryWhen::FreshestTableWrite`,
   reading the current state of the data.
3. **Compute diffs**: The adapter computes retractions and additions from the
   peek results (e.g., for DELETE: negate each row; for UPDATE: negate old rows
   and add new rows).
4. **Linearize** (for strict serializable isolation): Wait for the timestamp
   oracle to confirm that the read timestamp has been linearized.
5. **Write**: Submit the diffs via `send_diffs`, which adds them to the pending
   group commit queue.
6. **Release locks**: Locks are held until after the group commit applies the
   write to the timestamp oracle, ensuring no other write can sneak in between
   the read and the write.

The correctness of this approach depends entirely on the in-process locks. If
the locks were removed or if a second `environmentd` process were to execute a
concurrent write, the read-then-write would be susceptible to lost updates.

The complexity of this locking mechanism is significant:

- `WriteLocks` uses an all-or-nothing builder pattern to prevent deadlocks
- `GroupCommitWriteLocks` merges compatible locks across concurrent blind
  writes
- Deferred write operations must be carefully managed when locks aren't
  immediately available
- The lock validation code in `group_commit()` has multiple branches for
  different lock states (pre-validated, no locks needed, missing locks)

## The proposed OCC approach

### Architecture

The new approach moves read-then-write sequencing from the Coordinator to the
_session task_ (the per-connection async task), similar to how frontend peek
sequencing already works. The session task does the planning, optimization, and
OCC retry loop. It communicates with the Coordinator only for specific
operations that require Coordinator state:

- Creating and dropping the internal subscribe (which needs Coordinator
  bookkeeping for the compute sink)
- Submitting timestamped writes (which go through group commit)

### The OCC loop

```
Session Task                         Coordinator
  |                                      |
  |-- plan & optimize MIR/LIR            |
  |                                      |
  |-- acquire OCC semaphore              |
  |                                      |
  |-- CreateInternalSubscribe ---------> |
  | <------------ subscribe channel -----|
  |                                      |
  |   +-- OCC Loop ------------------+   |
  |   | receive diffs from subscribe |   |
  |   | on frontier advance:         |   |
  |   |   consolidate diffs          |   |
  |   |   AttemptTimestampedWrite -> |-->|-- group_commit()
  |   |   <-- Success/Failed --------|<--|
  |   |   if Failed: continue loop   |   |
  |   |   if Success: break          |   |
  |   +------------------------------+   |
  |                                      |
  |-- DropInternalSubscribe -----------> |
  |                                      |
```

### Timestamped writes

A timestamped write is a write that must be committed at a specific timestamp.
The group commit machinery has to be extended to support this by:

1. Checking if the target timestamp is still valid (hasn't been passed by the
   oracle)
2. Using the target timestamp directly instead of allocating a new one from the
   oracle
3. Advancing the oracle past the target timestamp after the write

Only one timestamped write is processed per group commit round. If multiple
timestamped writes target the same timestamp, one is selected and the others
are failed with a _timestamp passed_ error. This is necessary because
independently computed timestamped writes may be inconsistent with each other:
they were each computed from the state at their respective read timestamps and
could conflict if applied together.

### MIR transformations

The subscribe needs to produce the right diffs directly, rather than raw rows
that the adapter then transforms. We apply the mutation transformation at the
MIR level:

- **DELETE**: Wraps the selection expression in a `Negate`, producing `(row,
  -1)` diffs
- **UPDATE**: Uses a `Let` binding to share the selection. The body unions a
  negated `Get` (old rows with diff -1) with a mapped `Get` (new rows with diff
  +1, applying the assignment expressions)
- **INSERT...SELECT**: The selection passes through unchanged; the subscribe
  naturally emits each row with diff +1

### Concurrency limiting

When multiple read-then-write operations run concurrently, each maintains a
subscribe that continuously receives and processes updates. With N concurrent
OCC loops, whenever one loop succeeds, the other N-1 loops must process the
resulting updates and retry. This leads to O(N^2) total work.

To bound this, a semaphore has to limit the number of concurrent OCC operations
(default: 4). Additional operations wait for a permit before starting their
subscribe.

### Internal subscribes

The subscribes created for read-then-write are internal: they write no
`mz_subscriptions` row, and they move only the internal
`mz_active_internal_subscribes` gauge rather than the public
`mz_active_subscribes`. They do appear in replica introspection like any other
dataflow, named `frontend-read-then-write-subscribe-<sink_id>`. They are
created and dropped via dedicated `Command` variants (`CreateInternalSubscribe`,
`DropInternalSubscribe`).

## Correctness

The correctness argument has two parts: (1) the OCC loop produces the right
diffs, and (2) the timestamped write mechanism ensures they are applied at the
right timestamp.

### The subscribe produces the right diffs

The subscribe starts at the oracle read timestamp and emits the current state
of the selection expression as its initial snapshot. As other writes commit,
the subscribe emits updates that reflect those writes. At any point, if we
consolidate all diffs received so far, we get the current state of the
expression.

The MIR transformations (Negate for DELETE, Let/Union for UPDATE) ensure that
the diffs represent the correct mutation. For example, after consolidation, a
DELETE subscribe contains `(row, -1)` for each row currently matching the
selection.

### The timestamped write ensures atomicity

The write is submitted at the timestamp corresponding to the subscribe's
frontier. The group commit machinery checks that this timestamp hasn't been
passed by the oracle:

- If the timestamp is still valid: the write is committed at exactly that
  timestamp, and the oracle is advanced past it. Any concurrent OCC loops that
  were targeting the same timestamp will fail and retry.
- If the timestamp has already passed (another write committed first): the
  write also fails. The OCC loop continues, the subscribe delivers the updates
  from the intervening write, and the loop retries at the new frontier.

This ensures that the write is always based on the state of the data at exactly
the write timestamp. There is no window for lost updates: either the write
succeeds because nothing changed since the read, or it fails and retries with
fresh data.

### Linearization

Semantically, a read-then-write is a SELECT followed by a write. Normally we
have to linearize reads, ensuring that the oracle read timestamp is at least
the timestamp chosen for a peek, so that results can't "go backwards". With the
subscribe-based OCC loop, we might observe data timestamped beyond the current
oracle read timestamp. However, actually applying the write bumps the oracle
read timestamp to at least the write timestamp, so at write time it holds that
`write_ts <= oracle_read_ts`. The linearization invariant is maintained.

### Single timestamped write per group commit round

Only one timestamped write is processed per group commit round. This is correct
because:

1. Each timestamped write is computed independently, based on the state at its
   own read timestamp
2. Two independently computed timestamped writes could be inconsistent if
   applied at the same timestamp (e.g., both try to delete the same row, but
   after one succeeds the other's diff is stale)
3. After committing at timestamp T, the oracle advances past T, so additional
   writes at T would fail anyway. We fail them early to avoid unnecessary work.

### Timeouts

The lifetime of the OCC loop has to be bounded, both in wallclock time and in
number of retries. With the lock-based approach, a read-then-write could take
arbitrarily long and block the rest of the system. With OCC it can retry
arbitrarily long without ever succeeding, but it does not block the rest of the
system, which is a big benefit.

`statement_timeout` provides the wallclock bound. It is enforced in the session
task, around the whole operation rather than around the loop alone, so it also
covers planning, OCC permit acquisition, timestamp determination, and read
linearization. Any of those can park indefinitely, and a parked operation holds
an OCC permit, so a bound on the loop alone would leave the permit pool
starvable.

`max_occ_retries` provides the retry bound. A statement that keeps losing the
race for its write timestamp fails with a contention error instead of retrying
forever.

### Comparison with the old approach

In the old approach, correctness depends on:
1. No other writer interleaving between read and write (ensured by in-process
   locks)
2. Leadership confirmation via the catalog fencing mechanism

In the new approach, correctness depends on:
1. The subscribe reflecting committed state accurately (guaranteed by the
   compute/storage layers)
2. The timestamped write succeeding only if the target timestamp is still valid
   (guaranteed by the group commit / timestamp oracle)

The new approach is arguably easier to reason about: there is no global lock
state to consider, no deferred operations, no lock merging. The correctness
argument is local to the OCC loop and the group commit mechanism.

## Deliberate differences from the lock-based path

A user must not be able to tell which path sequenced their statement. These are
the places where the two paths do differ, on purpose. They are listed here so
that the next reader does not take them for bugs.

- **Serializability under a changing input (a strengthening).** The lock path
  reads a consistent snapshot at the peek timestamp but commits at a later one,
  and its locks pin only the selection's direct dependency items. For a
  selection over a materialized view that means the view's own id, which no
  writer ever takes, not the ids of its upstream tables. A write to such an
  upstream table can commit into the gap and change what the statement should
  have read, which admits non-serializable histories: a reader inside the gap
  sees the upstream write but not the mutation, while the mutation did not see
  the upstream write, so no serial order explains both. The OCC path closes this
  by making the statement an atomic read-modify-write at a single timestamp.
  When inputs are caught up the lock path's window is milliseconds wide and also
  needs a materially conflicting write plus a reader inside it, which is
  presumably why it went unnoticed.
- **A lagging dependency blocks rather than waits.** This is the price of the
  strengthening above. A selection dependency that persistently lags by more
  than about one `default_timestamp_interval` makes every attempt conflict,
  because the observed frontier is bounded by the lagging input while the write
  timestamp keeps advancing with the oracle. The statement then burns retries
  until `statement_timeout` instead of committing, where the lock path's peek
  simply waited for the input to catch up.
- **Statement lifecycle events.** The frontend path records an
  `optimization-finished` event for a DML, the coordinator path does not,
  because it hands the read-then-write's inner peek a trivial logging context
  and so logs nothing for it. We keep the extra event, it is real information
  about a statement the user did run.
- **`max_result_size` accounting.** The coordinator sums one row length per diff
  entry before consolidation. The frontend recomputes the total from the
  consolidated set, which counts one row length per distinct row and ignores
  multiplicity. So a `DELETE` of a million copies of one row can exceed the
  limit on the coordinator path and succeed on the frontend path. We keep the
  frontend's accounting: it matches what the write actually appends, one entry
  with a large diff.
- **The write-timeline throttle.** A timestamped write does not go through the
  throttle that a blind write's group commit applies, because its timestamp
  comes from an observed subscribe frontier rather than from the clock. See the
  doc comment on `GroupCommitter::commit_timestamped` for the full list of what
  that path skips and why.
- **Zero-row `INSERT ... RETURNING`.** Both paths report `INSERT 0 0` with no
  result set when no rows match, because the coordinator decides the response
  kind from the evaluated RETURNING rows and there are none. Postgres returns an
  empty result set here, with a row description. The frontend path is
  deliberately bug-compatible with the coordinator rather than correct on its
  own: fixing it changes the behavior of the path that ships today, which is a
  separate decision from this change.

## Performance

The goal is not to make writes faster, but to not regress significantly.
Benchmarking a PoC-level implementation of the OCC approach against `main` for
`UPDATE t SET x = x + 1` shows the following:

![update-benchmark](./static/occ_read_then_write/update-benchmark.png)

The benchmark varies concurrency (number of workers) on the x-axis and shows
throughput (left) and latency (right). Key observations:

- At low concurrency (1-7 workers), the result depends on write size. The PoC
  measured a single large `UPDATE`/`DELETE` as comparable or _better_ than
  `main`, because the subscribe streams the mutation diffs directly whereas the
  old path peeks every matched row and then recomputes the diffs. Small writes,
  however, _regress_: every operation installs a subscribe dataflow, waits for
  its snapshot, and tears it down, where the old path uses a cheap fast-path
  peek. This per-operation subscribe overhead makes tiny `UPDATE`s roughly
  1.5-2x slower at low/no concurrency (observed in the nightly feature benchmark
  `ManySmallUpdates` and the scalability `UpdateWorkload`).
- At higher concurrency, performance degrades as expected due to the O(N^2)
  retry behavior: with more concurrent writers, more retries are needed. The
  concurrency semaphore (default 4 permits) bounds this in practice.
- The benchmark is for a worst-case workload (all writers updating the same
  table). Writers on different tables do not reprocess each other's data, since
  a subscribe sees only progress from another table's write. They do still
  contend, in three ways: the concurrency semaphore is process-global across
  tables and clusters, the conflict predicate is the global oracle plus the
  shared txns-shard upper, so two writers that observed the same frontier refuse
  each other, and each timestamped write is its own committer round rather than
  merging into a shared group commit. Every write benchmark is single-table, so
  the cross-table case is unmeasured.

The chart above is from the PoC, which benchmarked `UPDATE t SET x = x + 1` over
a larger table (the regime where OCC wins). It does not capture the small-write
regression noted above, which is an accepted cost: high write throughput is a
non-goal (see Non-Goals).

Measured on the full implementation, with the OCC path on for every mzcompose
suite, the small-write regression is at the bad end of that range. Across nightly
runs the feature benchmark `ManySmallUpdates` is about 3.5x slower (3.4-3.7x over
three runs) and `Update` 1.4x slower (33-45%), and the scalability
`UpdateWorkload` loses 24% throughput at concurrency 1 and about 22% at 8 and 32.

`ManySmallUpdates` is the worst case for this design, and the reason is worth
recording. Its statements set every matched row's `f1` to one shared random
value, which merges a whole residue class, so the class count only shrinks and
roughly 90% of its 100 updates end up matching no rows. A statement that matches
nothing still has to linearize its read, and the oracle advances only when a
group commit applies, so each of those statements needs a commit that has nothing
to write. We ask for one rather than waiting for the periodic keepalive, which
costs a commit round trip per statement instead of up to a full
`default_timestamp_interval`. That is the difference between 3.5x and 157x, but
it is not free, and a workload dominated by zero-row writes pays it on every
statement. The residue is the price of the linearization guarantee rather than a
defect: correctness requires the oracle to advance, and only a commit advances
it.

The PoC's large-write win does not survive here. `Update` is itself a large
mutation, a full-table update over 10^6 rows, and it is 1.4x slower. An `UPDATE`
ships both halves of every diff plus a per-row timestamp prefix, and the
subscribe's data path runs through the coordinator loop, which merges and
re-packs every row. That works against the loop relief this design argues for,
and it works hardest against exactly the large mutations. No large-`DELETE`
scenario is measured, so whether the PoC's win holds for `DELETE` is untested.

`ManySmallUpdates` also steps `memory_clusterd` up by 57-73%, from about 54 MB
to 85-93 MB. The mechanism is unidentified. The obvious candidate, the subscribe
dataflow that each operation arranges on the cluster, does not account for it:
`memory_clusterd` samples after the iteration's dataflows are dropped, so a
transient arrangement cannot explain a persistent step. A single scale point
also distinguishes neither a leak from a plateau.

The performance suites run the OCC path, because that is the configuration we
intend to ship. The write benchmarks therefore record a one-time step, which we
accept for the reasons above. Registering it is a follow-up once the change has
landed and has a commit hash: `ManySmallUpdates` and `Update` go in
`get_ancestor_overrides_for_performance_regressions` and `UpdateWorkload` in
`ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS`, both in
`misc/python/materialize/version_ancestor_overrides.py`. That justification only
applies when the comparison is against a released version, so until the step is
inside the baseline these scenarios report a regression against `main`.

## Rollout

The new path is controlled by a `enable_adapter_frontend_occ_read_then_write`
dyncfg (default: disabled).

If we did a partial rollout where we check the dyncfg per read-then-write
operation, an OCC write could slip between an old-path reader's read and write
phases without the old path detecting it (since the OCC path doesn't acquire
write locks). We therefore must make the flag sticky per `environmentd` process
lifetime (check on bootstrap only) to avoid this, and keep the current
`confirm_leadership` checks.

The same reasoning carries across processes, which makes this a dependency for
0dt v2. That design (`20251219_zero_downtime_upgrades_physical_isolation_high_availability.md`)
runs both `environmentd` generations read-write concurrently, and two lock-path
processes lose updates by design, since the in-memory locks do not cross
processes. Full OCC rollout is therefore a prerequisite for enabling concurrent
read-write upgrades.

In CI the flag defaults to enabled for versions that carry it, so the mzcompose
suites exercise the OCC path even though production keeps it off. The version
gate leaves it disabled for the older versions an upgrade test runs, and
`CI_SYSTEM_PARAMETERS=random` can pick either value, which is how both paths
stay covered.

Once the OCC path is fully rolled out and validated:

1. Remove the old `sequence_read_then_write` code path
2. Remove the in-process write lock machinery (`WriteLocks`,
   `WriteLockBuilder`, `GroupCommitWriteLocks`, deferred write operations)
3. Remove the `confirm_leadership()`-style lock validation in group commit

This removes a significant amount of complexity and uncertainty from the
codebase.

## Alternative: distributed locking service

Instead of OCC, we could extend the current pessimistic locking approach to
work across processes by using a distributed locking service.

The flow would be:

1. Acquire a distributed lock for the target table
2. Peek at `FreshestTableWrite`
3. Compute diffs
4. Write
5. Release the distributed lock

This preserves the familiar lock-based model but has significant drawbacks:

- **Latency**: Every read-then-write would pay the cost of a CRDB round-trip
  (or equivalent) to acquire and release the lock, adding milliseconds to every
  single write operation. With OCC, the common case (no contention) succeeds on
  the first try without extra round-trips.
- **Brittleness**: Distributed locks require careful handling of lock expiry,
  holder crashes, and network partitions. A process that acquires a lock and
  then crashes (or becomes slow) must be fenced out, adding the same kind of
  complexity we already deal with for the `confirm_leadership()` check.
- **Complexity preservation**: The fundamental complexity of the lock-based
  approach remains: deferred operations when locks aren't available, all-or-
  nothing lock acquisition to prevent deadlocks, lock merging for concurrent
  blind writes. We would add distributed systems complexity on top of the
  existing in-process lock complexity, rather than replacing it.
- **Scalability**: The distributed lock would serialize all writes to a table
  across all processes, even when they don't conflict. OCC allows concurrent
  non-conflicting writes to proceed without coordination.

The OCC approach avoids all of these issues. Contention is handled by retrying,
which is simple and local. The cost is paid only when there _is_ actual
contention, and the subscribe ensures that retries are based on fresh data.

## Alternative: an occ loop running on `clusterd`

Instead of sending subscribe results back to `environmentd` and running the OCC
loop there, we could run the OCC loop right on the cluster. This should work,
if we give `clusterd` access to the timestamp oracle. A benefit of this
approach is that we take `environmentd` as much out of the processing path as
possible, and so we get better distribution of work.

Another school of thought will say that we _want_ `environmentd` to be in the
path, because we can maybe be smarter about how we commit data to persist.
There's a separation between data layer, which comes up with the changes and
runs the dataflow, and the control layer, which takes pointers to the changes
and appends them durably, with maybe some smarts in the middle.
