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

* A table function `CHANGES(collection AS OF [AT LEAST] <bound>)` (this document's primary spelling).
* Nestable `SUBSCRIBE`, e.g. `SELECT ... FROM (SUBSCRIBE coll AS OF '...')` and `CREATE MATERIALIZED VIEW v AS SUBSCRIBE coll AS OF '...'`.

These are surface variants over the same engine work; the table-function spelling reads more naturally inside `FROM` and composes with normal relational syntax.
The lower bound is spelled as an `AS OF` clause immediately after the relation — mirroring `SUBSCRIBE coll AS OF <bound>` — rather than as a second comma-separated argument, since `CHANGES` is a bespoke `TableFactor` (not a catalog function) and the `AS OF` surface reinforces "`CHANGES` is nestable `SUBSCRIBE`".

We deliberately reuse `AS OF` despite the streaming-SQL committee's caution that it collides with SQL:2011's value-time-travel `AS OF`: Materialize's `AS OF` already means "read hold at a logical time" (in `SELECT`/`SUBSCRIBE`), which is *exactly* `CHANGES`'s lower bound, and Materialize does not implement SQL:2011's `AS OF` at all, so there is nothing to collide with. Inventing `START AT`/`FROM <time>` would be less consistent than reusing the spelling users already know. This resolves the "parameter keyword" open question.

#### The relation argument: name or subquery

Naming a relation directly in a function-argument slot (`CHANGES(t ...)`) is not strictly idiomatic SQL — function arguments are value expressions, and relations are named in `FROM`, as subqueries, or (SQL:2016 PTFs) via an explicit `TABLE` keyword. The relevant idiom here is local consistency with `SUBSCRIBE`, which accepts *either* a bare name *or* a parenthesized subquery. So `CHANGES` mirrors it:

```sql
CHANGES(t AS OF ...)                  -- a collection named directly
CHANGES((SELECT * FROM t) AS OF ...)  -- a parenthesized subquery
```

We avoid the standards-correct `TABLE t` spelling precisely because Materialize uses it nowhere else; the `SUBSCRIBE`-style name-or-subquery is the lower-surprise local idiom.

In the initial implementation the subquery must reduce to a *bare read* of a single persist-backed collection (a table, source, or materialized view) — the planner peels an identity projection / empty map and requires a global `Get` — because the changelog mechanism reads a single shard. Anything that filters or transforms is the deferred arbitrary-expression case (it reopens the time-invariance / optimizer-barrier problem) and is rejected as unsupported. The subquery form is the natural entry point for generalizing that later. A view is rejected in *both* forms: a non-materialized view is not inlined at this planning stage (it reduces to a bare `Get` of the view itself, which is not persist-backed), and reading *through* it to an underlying shard is itself the deferred arbitrary-expression case (only an identity view would qualify, and detecting that is fragile).

### The lower bound: two orthogonal axes

The `AS OF` clause varies along two independent axes, which compose into a 2×2:

* **strict (`AS OF`) vs. advisory (`AS OF AT LEAST`)** — whether an unavailable history is an *error* or is *clamped up* to the input's current `since` (aging in). This reuses the existing `AsOf::At` / `AsOf::AtLeast` grammar.
* **fixed (constant) vs. sliding (`mz_now()`-relative)** — a static changelog start vs. a window that trails real time. Detected by whether the bound references `mz_now()`.

The shape of the bound transparently signals the output model: a **constant** bound is a fixed, append-only changelog from a point (no retraction); an **`mz_now()`-relative** bound is a sliding window whose trailing edge retracts via the existing temporal-filter machinery, which is what makes aggregation over it bounded and correct.

```sql
CHANGES(coll AS OF AT LEAST mz_now() - INTERVAL '30 minutes')  -- sliding, ages in (the easy default)
CHANGES(coll AS OF mz_now() - INTERVAL '30 minutes')           -- sliding, strict lag
CHANGES(coll AS OF '2026-06-02 12:00:00')                      -- fixed, strict
CHANGES(coll AS OF AT LEAST 0)                                 -- fixed, from earliest retained
```

### Where it is allowed (pruned by the read-hold constraint)

A lower bound does two jobs: it is the snapshot/read-hold point *and* (when `mz_now()`-relative) the window predicate. A read hold can only stop `since` from *advancing*; it can never recover already-compacted history, so look-back is only possible if the input already retained that much — `CHANGES` cannot manufacture history.

The binding operability constraint is that a **fixed** bound on a **durable** object holds the input's `since` open indefinitely (retained window `= upper - bound`, growing without bound). Only a **sliding** bound keeps the hold bounded (it trails `upper` by a fixed lag). This prunes the matrix by lifetime:

| context | fixed bound | sliding (`mz_now()`-relative) bound |
|---|---|---|
| one-off `SELECT` | allowed — hold released at query end | allowed — resolves against the query time |
| `SUBSCRIBE` | allowed — hold released on disconnect | allowed |
| durable: materialized view / index | **rejected** — unbounded, orphanable hold | allowed — bounded lagging hold |

The discriminator is *"does the hold outlive a user-bounded session?"*: a `SELECT` releases its hold when the query ends and a `SUBSCRIBE` when the client disconnects, so a fixed bound is tolerable there (a long historical read). A materialized view or index holds until `DROP` with no session to end it, so a fixed bound is unbounded *and* orphanable — rejected at plan time. This generalizes the rule that there is no `CREATE MATERIALIZED VIEW ... AS OF <constant>`. (A plain `CREATE VIEW` is inert; the conservative implementation rejects a fixed-bound `CHANGES` at any maintained lifetime, including `View`.)

For the maintained, sliding case, the consumer installs a *standing* lagging read hold on its inputs for its lifetime — like an index/MV holding its dependencies — which must be surfaced in the catalog so its cost on the input is explicit and attributable. Look-back and cross-run repeatability remain opt-in via the input's own retention policy (a deliberate, user-owned cost), since `CHANGES` will not retroactively extend input retention.

### Maintained materialized views: sliding execution (design, not yet implemented)

The one-off `SELECT` path is implemented; the maintained materialized-view path is not. This section records the intended design so it can be built (and runtime-tested) as a unit.

Unlike the peek path, the materialized-view optimizer (`src/adapter/src/optimize/materialized_view.rs`) has **no query-time/`since` context** — an MV runs continuously from an `as_of` chosen at creation and is maintained forever. So the peek-path mechanism (resolve `mz_now()` to the query time, clamp to the determination's `since`) does not transfer directly. The maintained design has four parts:

1. **Gating.** Remove the `OneShot`-only gate for the maintained + sliding combination (a fixed bound stays rejected, per the matrix above). Index remains disallowed initially (its only recovery is replay from the original `as_of`, i.e. an unbounded hold).

2. **Bounded read hold (lagging read policy).** The MV must hold its input's `since` trailing the input's `upper` by the window lag `i`, *for the MV's lifetime*. This is a `ReadPolicy::lag_writes_by(i)`-style policy on the input collection, installed when the MV is created and released on `DROP`. This is the compaction-safety-critical piece: the hold must be attributed to the MV (surfaced in `mz_` catalog relations) so the cost it imposes on the input — slower compaction, larger retained history, borne by *all* readers of that input — is explicit, not silent.

3. **Continuous output temporal filter.** Because the window slides forever, the changelog output must *retract* changes once they age past `mz_now() - i`. This is the existing temporal-filter machinery: install `WHERE mz_timestamp >= mz_now() - i` on the `CHANGES` output. The `mz_timestamp` column being `MzTimestamp`-typed is what makes this a recognizable temporal filter. The output is therefore **not** append-only in the maintained case — it retracts at the trailing edge — which is exactly what bounds the MV's state and keeps aggregations over it correct.

4. **Restart / recompute semantics.** On environment restart or a version upgrade that changes how changes are computed, the MV restarts from the input's *current* `since` (the advisory-`as_of` behavior). Because the window is bounded, this re-reads at most `i` of history — cheap — and the MV retracts its previously written stream and re-emits. (A fixed/all-time bound would make this lossy or unboundedly expensive, which is why it is disallowed for maintained objects.)

The `as_of`/`mz_now()` resolution differs from the peek path: rather than evaluating the bound against a query timestamp, the MV optimizer pins the dataflow `as_of` to the input's current `since` at creation (the window then ages in over `i` of wall-clock — it does not look back before creation unless the input already retained that far, per the read-hold constraint above).

**Why this is deferred:** parts 2–3 touch read policies and temporal-filter installation in the maintained-dataflow path, which are correctness-/compaction-sensitive and only meaningfully verifiable at runtime. They should be implemented and tested together, not stacked speculatively on the one-off path.


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

## Resolved questions

* **Parameter keyword.** Resolved: spell the bound as an `AS OF [AT LEAST] <bound>` clause (see Syntax). We consciously reuse `AS OF` — it already means "read hold at a logical time" in Materialize, and there is no SQL:2011 `AS OF` to collide with.
* **Upgrade recomputation (for the supported scope).** Restricting durable `CHANGES` to a *sliding* bound (fixed bounds are rejected on maintained objects) bounds the restart cost: starting over only re-reads the window, never an all-time stream. One-off `SELECT`/`SUBSCRIBE` compute once and never persist a stream, so there is nothing to recompute across versions. The remaining all-time/unbounded hazard is steered away from rather than solved.

## Open questions

* **Dataflow timestamp selection.** `SELECT COUNT(*) FROM CHANGES(...)` could oblige a one-off query to watch the count climb from zero to current as history replays, depending on whether we pick the dataflow `as_of` from `since` or `upper`. Do we need `CREATE MATERIALIZED VIEW ... AS OF <time>` / a chosen evaluation time so a one-shot read returns a settled answer rather than a moving one?
* **Compaction-hold accounting.** How do we surface and attribute the read hold a `CHANGES` instance imposes, so that an uncompacted input's cost is visible and not silently borne by unrelated readers? For the maintained sliding case the standing lagging hold must appear in the catalog. Mitigations floated: smarter persist, a "double collection" (uncompacted feeder plus compacted serving copy), and pure expectation management.
* **Advisory clamping execution.** Implemented for one-off `SELECT`. The peek's existing read holds already pin the inputs' `since` for the query, and the coordinator threads that read frontier (`determination.since`) into the optimizer. An advisory (`AS OF AT LEAST`) bound is clamped up to `since` (aging in to the earliest available history); a strict (`AS OF`) bound is pinned as written, and if it precedes `since` the dataflow fails to be created, surfacing the unavailable history. (A dedicated, friendlier strict-error message is a possible refinement over the raw dataflow-creation error.)
* **Sliding-bound execution in maintained objects.** A sliding bound is now supported in a one-off `SELECT` (the bound is evaluated at the query time and the changelog start trails it by the lag; no output temporal filter is needed because the peek time is the upper edge). The *maintained* materialized-view case — a lagging read policy on the input plus a continuous output temporal filter, with bounded restart — is designed but not implemented; see "Maintained materialized views: sliding execution".
* **Metadata columns.** Do we expose only `mz_timestamp` / `mz_diff`, or also committee-style `$Action` / `$IsUpdate`? `$IsUpdate` is not computable in all cases, so it likely stays out of the first cut.
