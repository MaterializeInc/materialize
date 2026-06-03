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
The output relation has the columns of the input collection plus `mz_timestamp` and `mz_diff` — the same columns as `SUBSCRIBE`, though appended rather than prepended, with `mz_timestamp` typed as `mz_timestamp` (`SUBSCRIBE`: `numeric`) and `mz_diff` non-null (`SUBSCRIBE`: nullable).
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
With the maintained design's lagged input holds (see "Maintained materialized views" below), an ordinary restart reproduces the window exactly with no retraction; starting over is the fallback, not the common path.

### Syntax

The issue surfaced two viable surfaces:

* A table function `CHANGES(collection AS OF [AT LEAST] <bound>)` (this document's primary spelling).
* Nestable `SUBSCRIBE`, e.g. `SELECT ... FROM (SUBSCRIBE coll AS OF '...')` and `CREATE MATERIALIZED VIEW v AS SUBSCRIBE coll AS OF '...'`.

These are surface variants over the same engine work; the table-function spelling reads more naturally inside `FROM` and composes with normal relational syntax.
The lower bound is spelled as an `AS OF` clause immediately after the relation — mirroring `SUBSCRIBE coll AS OF <bound>` — rather than as a second comma-separated argument, since `CHANGES` is a bespoke `TableFactor` (not a catalog function) and the `AS OF` surface reinforces "`CHANGES` is nestable `SUBSCRIBE`".

We deliberately reuse `AS OF` despite the streaming-SQL committee's caution that it collides with SQL:2011's value-time-travel `AS OF`: Materialize's `AS OF` already means "read hold at a logical time" (in `SELECT`/`SUBSCRIBE`), which is *exactly* `CHANGES`'s lower bound, and Materialize does not implement SQL:2011's `AS OF` at all, so there is nothing to collide with. Inventing `START AT`/`FROM <time>` would be less consistent than reusing the spelling users already know. This resolves the "parameter keyword" open question.

#### The sliding bound is special syntax, not an expression

`mz_timestamp` deliberately supports no arithmetic — in temporal filters, `mz_now()` must be directly compared to a non-temporal expression (see `as_mut_temporal_filter`), and the missing `mz_timestamp ± interval` operators are what steer users to that normal form.
Consequently the canonical sliding bound `mz_now() - INTERVAL '30 minutes'` is not a plannable expression in general contexts.
Rather than introducing `mz_timestamp` arithmetic operators (and reopening that design decision), `CHANGES` blesses exactly the top-level shape `mz_now() - <interval>` as special syntax of the `AS OF` clause: it plans as `(mz_now()::timestamptz - <interval>)::mz_timestamp` using existing casts and `timestamptz` arithmetic.
The bound never becomes a dataflow predicate (the coordinator or the MV optimizer evaluates it), so this widens nothing about what temporal filters accept.

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

### Maintained materialized views: sliding execution (implemented)

Both the one-off `SELECT` path and the maintained materialized-view path (sliding, advisory bound) are implemented.
This section records the design; the implementation map and remaining work follow it.

Unlike the peek path, the materialized-view optimizer (`src/adapter/src/optimize/materialized_view.rs`) has **no query-time/`since` context** — an MV runs continuously from an `as_of` chosen at creation and is maintained forever.
So the peek-path mechanism (resolve `mz_now()` to the query time, clamp to the determination's `since`) does not transfer directly.
The maintained design has the following parts:

1. **Gating.** Remove the `OneShot`-only gate for the maintained + sliding combination (a fixed bound stays rejected, per the matrix above). Index remains disallowed initially, though the machinery below would support it: index recovery becomes a bounded window replay rather than an unbounded one.

2. **Bounded read hold (lagged dependency holds in the compute controller).** The compute controller already holds per-dataflow read holds on a dataflow's inputs (`storage_dependencies` in `src/compute-client/src/controller/instance.rs`), downgraded to the output collection's read frontier as it advances. For a changelog import, the downgrade target is additionally lagged by the window `i`: the input hold trails at roughly `output_frontier - 1 - i`. Properties:
   * The lag is relative to the *output* frontier, not the input's `upper` — strictly safer: if the MV stalls, input retention follows the stall point and restart remains reproducible. The cost is that a stalled MV blocks input compaction by correspondingly more.
   * The holds live and die with the dataflow: `DROP` and environment shutdown release them with no new bookkeeping, and bootstrap re-establishes them through the existing dataflow-shipping path.
   * Alternative considered: install a `ReadPolicy::lag_writes_by(i)` on the input collection at the adapter level, composed with the input's own compaction window via `ReadPolicy::Multiple`, with per-dependent bookkeeping in the coordinator. This is more visible and attributable (catalog introspection of who holds what) but requires new policy-composition machinery; it is the likely future direction once we want hold introspection. The controller-level mechanism localizes the complexity for now.

3. **Restart-exact rendering: skip the snapshot, read below the `as_of`.** A naive restart (read the input snapshot at the changelog start, clamp to it, as the one-off path does) is wrong for a maintained MV: the desired output at `as_of` would be the input's *accumulated state* clamped to `as_of`, while the persisted output holds the *window* `(as_of - i, as_of]` at true timestamps. The persist sink's reconciliation would retract the entire window and emit a giant clamped snapshot that then ages out — a correction storm and an output discontinuity on every restart. Instead, the maintained rendering:
   * reads the input from `as_of - i`, *below* the dataflow `as_of`,
   * skips the snapshot entirely, emitting only the deltas at their true timestamps (the `SUBSCRIBE ... WITH (SNAPSHOT = false)` shape),
   * installs the strict output temporal filter, spelled `mz_now() < mz_timestamp + i` — the `mz_now() BINOP <non-temporal>` normal form the temporal MFP machinery requires (`as_mut_temporal_filter`), with the interval arithmetic on the column side. The MV optimizer constructs it at MIR level once it has extracted the constant lag `i` from the bound; month intervals, whose millisecond lag varies with the calendar, are rejected.

   The desired output at `as_of` is then exactly the persisted contents, and reconciliation is a no-op.
   Creation needs no special case: the input's `since` is recent, so the window simply ages in from there.
   The clamp never fires in the maintained case, so `CHANGES` does not depend on the MV's durable `initial_as_of`.
   The output is **not** append-only in the maintained case — it retracts at the trailing edge — which is exactly what bounds the MV's state and keeps aggregations over it correct.

4. **As-of selection.** Dataflows with changelog imports need a new hard constraint in `src/compute-client/src/as_of_selection.rs`: `as_of >= input_since + i` (the plain upstream constraint is `as_of >= input_since`). Note the zero-slack consequence: with the hold at `output_frontier - 1 - i` and the downstream hard constraint `as_of < output_upper`, exactly one valid `as_of` remains (`output_upper - 1`). Lagging by `i + slack` is cheap insurance; any `since` slippage degrades to the advisory fallback below rather than corrupting the output.

5. **Why restart works (the durability chain).** Read holds are in-memory tokens; the durable artifact is the persist critical `since` capability, which holds only gate the *downgrade* of. Environment death freezes the input's `since` at `output_frontier_at_death - 1 - i`. The existing bootstrap ordering closes the re-acquisition race with no new mechanism: storage collections reopen handles at the frozen `since`, `as_of_selection::run` acquires read holds on all dataflow inputs before any dataflow ships, and the controller's dependency holds exist before the bootstrap holds are dropped and read policies are installed (`bootstrap` in `src/adapter/src/coord.rs`).

6. **0dt upgrades.** Two independent protections: the leader keeps running the MV until fencing, so its lagged holds keep the input's `since` trailing; and the read-only environment's `StorageCollections` opens *leased* persist handles, which the leader's critical-handle downgrades cannot compact past. The read-only instance picks its `as_of` under the same `as_of >= since + i` constraint, reproduces the window deterministically, and runs forward; at cutover the computed state equals the written state.

7. **Advisory fallback.** When exact reproduction is impossible — the `since` slipped past `as_of - i`, or a version upgrade changes how changes are computed — the persist sink's reconciliation degrades to retract-and-re-emit from the input's current `since`: the window ages in again, bounded by `i`. Degradation, not corruption. (A fixed/all-time bound would make this lossy or unboundedly expensive, which is why it is disallowed for maintained objects.)

### Implementation map

Where each piece of the maintained design lives:

* **Import metadata.** `ChangelogMode::{OneShot, Maintained { window, start }}` on `SourceImport` (`src/compute-types/src/dataflows.rs`), replacing the earlier `read_as_changelog: bool`; `set_changelog_start` joins a new start with any existing one, so the start only ever advances. No proto involved — dataflow descriptions are serde-serialized.
* **Gating.** `plan_changes` in `src/sql/src/plan/query.rs`: `OneShot` and `MaterializedView` + sliding + advisory are allowed; fixed bounds on maintained lifetimes keep `ChangesRequiresSlidingBound`; strict sliding and all other maintained contexts are `bail_unsupported!`.
* **Blessed bound syntax.** `plan_changes_bound` (same file) special-cases the top-level shape `mz_now() - <interval>`, planning it as `(mz_now()::timestamptz - <interval>)::mz_timestamp`.
* **Lag extraction + temporal filter.** `changelog_window` in `src/adapter/src/optimize/materialized_view.rs` evaluates the bound at two reference times (2020-03-15 / 2020-04-15, chosen so calendar-dependent month intervals produce differing lags and are rejected) and requires a constant lag. The same pass wraps each `Changes` node in the MIR filter `mz_now() < mz_timestamp + i` and marks the imports `Maintained { window, start: None }`. REFRESH schedules are rejected here.
* **Start resolution (three sites, one formula `join(since, as_of - window)`).** Creation: the sequencer (`create_materialized_view.rs`, after `select_timestamps`). Environment restart: `as_of_selection::run` (`src/compute-client/src/as_of_selection.rs`), which also applies the soft lower-bound constraint `as_of >= since + window` after the downstream hard constraints (unit tests `changelog_storage_constraints{,_conflict}`). Replica reconnect: `ComputeCommandHistory::reduce` (`src/compute-client/src/protocol/history.rs`) advances the start alongside the compacted `as_of`.
* **Lagged dependency holds.** `Instance::create_dataflow` downgrades changelog-import holds to `start` (not `as_of`) and records per-import windows in `CollectionState::changelog_windows`; `apply_read_hold_change` (`src/compute-client/src/controller/instance.rs`) downgrades those holds to `output_read_frontier - window`, joined with the current hold to avoid early regression.
* **Rendering.** `src/compute/src/render.rs` import region: maintained changelog imports read persist at `start` with `SnapshotMode::Exclude`; the reinterpretation advances differential (and error-stream) times to the dataflow `as_of` while the `mz_timestamp` column keeps the true time.
* **Window cap.** The `changes_max_window` system variable (default 1 day) bounds the compaction hold a maintained changelog imposes on its input; wider windows must be opted into explicitly, matching the "explicit, attributable cost" success criterion. Enforced in the sequencer (`create_materialized_view.rs`, alongside start resolution) rather than in `changelog_window` in the optimizer: the optimizer also runs at bootstrap, where erroring would wedge the system if the cap were lowered below an existing view's window — creation-time-only enforcement grandfathers existing objects, mirroring the strict-bounds rationale below.
* **Tests.** `test/sqllogictest/changes.slt` (gating, creation, rejection cases, window cap), `test/testdrive/changes.td` (maintained data round-trip: ages in empty, appends deltas, aggregates), `as_of_selection` unit tests, and the `ChangesMaterializedView` platform check (`misc/python/materialize/checks/all_checks/changes.py`), which verifies restart-exact reproduction end to end — exact multisets catch spurious snapshot rows, cross-phase `mz_timestamp` ordering catches a re-snapshot — across the restart and upgrade scenarios, including 0dt (the leased-handle path).

### Remaining work

Recorded with enough detail to be picked up in a later session.

* **Zero slack in restart as-of selection.** The lagged hold sits at exactly `output_read_frontier - window`, and restart needs `as_of >= since + window` with `as_of <= output_upper - 1` — a single valid point. Any extra `since` slippage degrades to the advisory fallback (short window + sink correction). The `ChangesMaterializedView` platform check passes `RestartEntireMz` without churn, but if broader scenarios show churn in practice, lag the holds by `window + slack` (a small constant or dyncfg) in `apply_read_hold_change`; everything else recomputes from the hold.
* **Month intervals.** Rejected because their millisecond lag varies with the calendar while the hold/start machinery needs a constant. Could be supported later with a conservative constant over-approximation (31 days per month) for the hold and start, while the temporal filter keeps the calendar-correct arithmetic — the hold may then over-retain, never under-retain.
* **Indexes.** Mechanically the same machinery: the controller and rendering are already import-generic, and index recovery becomes a bounded window replay from the input. Work: relax the gate (`QueryLifetime::Index` + sliding in `plan_changes`), factor the `Changes`-marking/filter pass out of `materialized_view.rs` for reuse by the index optimizer (`src/adapter/src/optimize/index.rs`), and wire start resolution where index as-ofs are decided (`create_index` sequencer and `as_of_selection`; note indexes additionally get read-policy and current-time soft constraints, and have no durable output, so the downstream hard constraint — and with it the zero-slack point — differs: full age-in on restart is acceptable, exact reproduction is not required).
* **Strict sliding bounds in MVs.** `AS OF` (vs `AS OF AT LEAST`) should error at creation unless the input retains a full window (`since <= as_of - window`), instead of silently aging in. Needs strictness plumbed to the start-resolution site (either a field on `ChangelogMode::Maintained` or a check in the sequencer against the MIR `Changes` node's `strict` flag). Restarts should stay advisory regardless — erroring an MV at bootstrap wedges the system.
* **SUBSCRIBE.** Structurally the one-off mode with a session-length hold (fixed bounds allowed per the lifetime matrix; hold released on disconnect). Needs the gate relaxed and the subscribe sequencer to resolve the bound at subscribe time, mirroring the peek path; no output temporal filter (the stream's upper edge is "now").
* **Optimizer integration.** `Changes` is deliberately an opaque barrier leaf: every analysis assigns it a conservative explicit value, and the changelog `Get`'s MFP is withheld from the persist source operators (guards in `Plan::refine_source_mfps` and `optimize_dataflow_filters`), so the full shard is read and filtered only afterwards. Sound relaxations, roughly in value order: projection/demand pushdown (only demanded input columns need be read; `mz_timestamp`/`mz_diff` are appended by the reinterpretation); pushdown of non-temporal predicates on input columns, which commute with the reinterpretation (filtering updates whose row satisfies `p` equals reading only rows satisfying `p`) — predicates on `mz_timestamp`/`mz_diff` do not commute and must stay above; per-analysis precise values (arity/types are exact today, but e.g. uniqueness, non-negativity, and cardinality are maximally conservative); statistics for join planning. Bounding the shard read by the window's upper edge is the same idea as `start` bounds the lower edge. This is also groundwork for arbitrary expressions below — a true IR operator needs the optimizer to reason through `Changes`, not around it.
* **Arbitrary expressions.** `CHANGES((SELECT ...) AS OF ...)` beyond a bare read of a persist-backed collection. The subquery form is the syntactic entry point, but anything that filters or transforms reopens the time-invariance problem deferred in "Alternatives" ("True table function in the IR"): we know how to *produce* changelog output from uncompacted data, not how to optimize queries with `Changes` in arbitrary positions while meeting user expectations. `plan_changes` currently peels an identity projection / empty map and requires a global `Get`; generalizing means either materializing the derived query first (today's workaround: `CREATE MATERIALIZED VIEW`, then `CHANGES` over it) or making `Changes` a true IR operator over arbitrary inputs, with the optimizer-barrier consequences catalogued there.
* **Hold introspection (option A).** The lagged holds live inside the compute controller and are invisible in the catalog. The recorded future direction is adapter-level `ReadPolicy` composition (`ReadPolicy::Multiple` with per-dependent bookkeeping) or, more cheaply, an `mz_internal` relation exposing per-dataflow dependency holds, so the cost a `CHANGES` MV imposes on its input is attributable.
* **Sharp edges to keep in mind.** Multiple *maintained* changelog reads of one input use the *widest* window for the import's hold and start while each read's filter enforces its own window. Multiple *one-off* reads of one input share the *earliest* start outright — there is no per-read filter, and one cannot be added: filtering `mz_timestamp` cannot re-derive a later read's snapshot collapse (the snapshot rows would be dropped, not collapsed to the later bound). Distinct *inputs* carry distinct per-import starts (`ChangelogMode::OneShot { start }`). A `Maintained { start: None }` import reaching the controller is a `soft_panic_or_log` that degrades to reading at the `as_of` (empty window aging in). Cached physical plans may carry a stale or `None` start; every decision site recomputes by `join`, which is monotone and therefore safe. The advisory clamp in the one-off path uses the query-wide `since` (the join over all inputs), so an input whose own `since` is older may be clamped further than strictly necessary — conservative, never unreadable. A subquery's column aliases are dropped (`CHANGES((SELECT a AS x FROM t) ...)` exposes `a`): the subquery reduces to a bare `Get` and the rename lives only in the discarded scope. An input column already named `mz_timestamp`/`mz_diff` yields duplicate output column names, as in `SUBSCRIBE`.


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
* **Sliding-bound execution in maintained objects.** Implemented for one-off `SELECT`s (the bound is evaluated at the query time and the changelog start trails it by the lag; no output temporal filter is needed because the peek time is the upper edge) and for materialized views with a sliding, advisory bound (lagged dependency holds in the compute controller, snapshot-skipping rendering, a changelog-aware as-of constraint, and a continuous output temporal filter). See "Maintained materialized views: sliding execution" for the implementation map (restart verification and the window cap are done); indexes, strict sliding bounds, and SUBSCRIBE remain — see "Remaining work".
* **Metadata columns.** Do we expose only `mz_timestamp` / `mz_diff`, or also committee-style `$Action` / `$IsUpdate`? `$IsUpdate` is not computable in all cases, so it likely stays out of the first cut.
