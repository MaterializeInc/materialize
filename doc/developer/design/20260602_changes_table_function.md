# `CHANGES` table function

- Associated: [database-issues#4527](https://github.com/MaterializeInc/database-issues/issues/4527)
- Associated: [database-issues#5094](https://github.com/MaterializeInc/database-issues/issues/5094) (Durable subscribes)
- Associated: [database-issues#1953](https://github.com/MaterializeInc/database-issues/issues/1953) (CDC from two append-only collections)
- Associated: [Subscribe outputs design](./20230418_subscribe_output.md)

## The Problem

Users want the change stream of a collection as a relation they can transform with SQL.
`SUBSCRIBE` already exposes this stream, but only as a top-level streaming statement whose output flows directly to the client.
It cannot be wrapped in a `SELECT`, aggregated, joined, indexed, or materialized.
Consequently users who want to reason over the history of a collection, rather than its current contents, have no in-database mechanism and must reconstruct the history in an external tool.

A representative request is console observability: plotting a metric over a recent time window.

```sql
SELECT
  MAX(memory_percent),
  date_bin(interval '60 seconds', occurred_at, $1) AS bin_start
FROM CHANGES(mz_internal.mz_cluster_replica_utilization, mz_now() - '30m')
GROUP BY bin_start
ORDER BY bin_start DESC;
```

Here the value comes from the changes themselves: the timestamp at which each update happened is data, not metadata.
No maintained view over current contents can answer this, because current contents discard history.

## Success Criteria

A solution is successful if:

* Users can consume the `SUBSCRIBE`-style change stream of a collection from inside a `SELECT`, with the per-update timestamp and diff available as ordinary columns.
* The output is well-defined and reproducible for the operations we choose to support, with no silent divergence between two evaluations of the same query.
* The compaction and read-hold costs the feature imposes on its inputs are explicit, bounded, and attributable, rather than unbounded and surprising.
* The set of contexts in which the feature is allowed (one-off `SELECT`, materialized view, index) is exactly the set in which we can meet user expectations across restarts and version upgrades.

## Out of Scope

* Durable, resumable subscribe sessions surviving restart; tracked separately in [database-issues#5094](https://github.com/MaterializeInc/database-issues/issues/5094).
* General user-controlled compaction as a first-class feature; this design only needs the narrow read-hold behavior it requires, not a full user-facing knob.
* The `DELTA` / coalesced output format discussed in the streaming-SQL committee; the first cut produces the raw `LOG` stream only.
* `ENVELOPE UPSERT` / `ENVELOPE DEBEZIUM` / `WITHIN TIMESTAMP ORDER BY` reshaping; those already exist for `SUBSCRIBE` and can be layered later.

## Solution Proposal

The core observation, workshopped over the issue's lifetime, is that `CHANGES` is just `SUBSCRIBE` usable in a nested context.
The output relation has the columns of the input collection plus `mz_timestamp` and `mz_diff`, identical to `SUBSCRIBE`.
Mechanically each input update `(row, time, diff)` is mapped to a forward-only output update `((row, time, diff), max(time, as_of), 1)`: every change becomes an append at the time it occurred (or at `as_of` for the initial snapshot).
The result is append-only and never retracts.

### Semantics and the time-invariance problem

The rest of the IR satisfies a time-invariance property: the output at time `t` is a pure function of the input accumulated at `t`.
`CHANGES` breaks this.
Its output at `t` depends on the *history* of the input up to `t`, not merely its accumulation.
A collection compacted forward to time `c > as_of` can no longer produce the changes between `as_of` and `c`, so `CHANGES` over a compacted collection is not the compaction of `CHANGES` over the uncompacted collection.

This forces the central implementation constraint: a `CHANGES(coll, as_of)` instance must hold back compaction (the `since` frontier) of `coll` to `as_of` for as long as the instance must be able to reproduce its output.

### Where it is allowed

We propose allowing `CHANGES` only where we can honor that constraint and meet user expectations:

* **One-off `SELECT`**: allowed. A read hold at `as_of` is taken for the duration of the query, exactly as a historical `SELECT ... AS OF` would. This is the safe, always-correct case.
* **Materialized view**: allowed but gated. The change stream is written durably, so we do not need to hold input compaction merely for serving. The hazard is recomputation on version upgrade (see below) and unbounded size, since appended changes never compact unless an aggregation sits on top.
* **Index**: disallowed initially. An index is recoverable only by replaying from the input at the original `as_of`, which would require pinning `since` at `as_of` indefinitely. That is the worst case and we do not start there.

### Bounding the cost with a sliding `as_of`

An unbounded `as_of` (e.g. epoch) pins `since` forever and is the source of most stress.
We propose supporting a `mz_now()`-relative `as_of` so the hold is bounded to a window:

```sql
CHANGES(coll, mz_now() - INTERVAL '30 minutes')
```

The hold then trails real time at a fixed lag rather than growing without bound, and the same temporal-filter machinery that retracts old rows can drop changes that fall out of the window.
This is the form most use cases (observability dashboards, recent-activity views) actually want.

### Advisory `as_of`

To survive recomputation, `as_of` should be advisory rather than a hard guarantee: Materialize may advance it to the input's current `since` frontier when it must start over (a bug fix or behavior change in a new version that would compute different changes).
For a durably written `CHANGES` (materialized view), starting over means retracting the previously written stream and re-emitting from the input's current `as_of`.
For a bounded sliding window this is cheap; for an all-time stream it is the expensive case we want to steer users away from.

### Syntax

The issue surfaced two viable surfaces:

* A table function `CHANGES(collection, as_of)` (this document's primary spelling).
* Nestable `SUBSCRIBE`, e.g. `SELECT ... FROM (SUBSCRIBE coll AS OF '...')` and `CREATE MATERIALIZED VIEW v AS SUBSCRIBE coll AS OF '...'`.

These are surface variants over the same engine work; the table-function spelling reads more naturally inside `FROM` and composes with normal relational syntax.
Note the streaming-SQL committee's caution that `AS OF` has a formal SQL:2011 time-travel meaning distinct from a changelog; we should pick a parameter keyword that does not collide (`FROM <time>` or an explicit `START AT`), even if the underlying mechanism is the read hold of a historical read.

## Minimal Viable Prototype

Frank's suggested cheapest validation path: implement `CHANGES` as a re-interpretation argument on `persist_source` rather than a true table function.
The operator reads the same persist shard but reinterprets each `(row, time, diff)` as the append `((row, time, diff), max(time, as_of), 1)`, deferring the IR/time-invariance questions.
This is enough to:

* Validate the output shape and that downstream SQL (`date_bin`, `GROUP BY`, aggregation) behaves as users expect.
* Measure the compaction-hold cost on a real input.
* Demo the observability query end to end.

Source reinterpretation is independently useful (the progress collection, data-defined CDC in [database-issues#1953](https://github.com/MaterializeInc/database-issues/issues/1953)), so the prototype is not throwaway scaffolding.

Restricting the prototype to inputs that resolve directly to persist collections (sources, tables, materialized views, indexes' backing shards) keeps the first cut tractable.

## Alternatives

* **Client-side reconstruction from `SUBSCRIBE`.** Users already can, and do, maintain history outside the database. This is the status quo; it fails the "transformable in SQL, indexable, materializable" goals and pushes consistency handling onto every client (see the state-machine example in the subscribe-outputs design).
* **True table function in the IR.** Cleanest user model but directly violates time-invariance, touching optimizer assumptions in surprising places. We believe we know how to *produce* `CHANGES` output from uncompacted data; we do *not* yet know how to optimize queries with `CHANGES` in arbitrary positions while meeting user expectations. Deferring this is deliberate.
* **`DELTA` / coalesced output format.** The committee discussed a minimal-delta format that collapses intermediate diffs. Useful, but orthogonal to the core mechanism and deferred.
* **Hard-disallow continuous (view/index) use entirely.** Safe, but forecloses the most-requested case (a maintained rolling-window view). We instead gate it: materialized views with a bounded window, no indexes initially.

## Open questions

* **Upgrade recomputation.** On a version upgrade that changes how changes are computed, do we permanently hold the `since` of all inputs so we can recompute, or do we rely on the advisory `as_of` to restart from the current `since` and accept a discontinuity? The answer differs for bounded windows (restart is cheap) versus all-time streams (restart is lossy or expensive). This is the single biggest unresolved correctness/operability question.
* **Dataflow timestamp selection.** `SELECT COUNT(*) FROM CHANGES(...)` could oblige a one-off query to watch the count climb from zero to current as history replays, depending on whether we pick the dataflow `as_of` from `since` or `upper`. Do we need `CREATE MATERIALIZED VIEW ... AS OF <time>` / a chosen evaluation time so a one-shot read returns a settled answer rather than a moving one?
* **Compaction-hold accounting.** How do we surface and attribute the read hold a `CHANGES` instance imposes, so that an uncompacted input's cost is visible and not silently borne by unrelated readers? Mitigations floated: smarter persist, a "double collection" (uncompacted feeder plus compacted serving copy), and pure expectation management.
* **Parameter keyword.** Settle on the `as_of` spelling that avoids the SQL:2011 `AS OF` time-travel collision while staying intuitive.
* **Metadata columns.** Do we expose only `mz_timestamp` / `mz_diff`, or also committee-style `$Action` / `$IsUpdate`? `$IsUpdate` is not computable in all cases, so it likely stays out of the first cut.
