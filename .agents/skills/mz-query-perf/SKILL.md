---
name: mz-query-perf
description: Analyze and optimize a Console/catalog SQL query in user space — diagnose its plan on real relations, then measure candidate rewrites with a faithful synthetic-fleet sweep. Use when asked to find performance improvements for a query that reads mz_catalog / mz_internal relations.
---

# Console query performance analysis

A repeatable method for finding and validating performance improvements to a SQL
query that reads system catalog / introspection relations (the kind the Console
runs against `mz_catalog_server`). Developed on the cluster-utilization queries.

The method has two phases. Phase 1 (plan diagnosis) is cheap, exact, and needs no
data. Phase 2 (latency sweep) is only for candidates that Phase 1 flags, and its
fidelity depends entirely on one thing: **the shadow tables must carry the same
indexes the real relations have on `mz_catalog_server`.**

## Phase 1 — diagnose the plan on the REAL relations (cheap, do this first)

The query reads real builtin relations that already exist on a local
`environmentd`, on `mz_catalog_server`, with their real indexes and view
definitions. So `EXPLAIN` the actual query there. No synthetic data is needed to
read a plan's shape, and this is strictly more faithful than any shadow.

1. Take the query file. Fill in any parameters with literals (cluster ids like
   `'s2'`, timestamps, bucket sizes). Fix template artifacts (e.g. `LIMIT '1}'`
   from Handlebars becomes `LIMIT 1`).
2. Run it through `EXPLAIN` on `mz_catalog_server`:
   ```
   psql "postgres://materialize@localhost:6875/materialize" -c \
     "SET cluster = mz_catalog_server; EXPLAIN <filled query>;"
   ```
   (Use `SET cluster = mz_catalog_server` so the maintained builtin indexes are
   in scope. The default cluster will not have them and gives a misleading plan.)
3. Read the plan for these anti-patterns (each is a known win):
   - **Filter applied after a Top-1 / Reduce, over the whole fleet.** A per-entity
     query (one cluster, one replica) whose `WHERE entity = ?` only appears at the
     end, above `Monotonic Top1` / `Reduce` / a multi-way join that ran over every
     entity. The optimizer will not push a top-level predicate into a CTE shared
     by multiple consumers. Fix: push the filter to where the entity column is
     first introduced (the source read), so the heavy operators are scoped.
   - **`*** full scan ***` / unfiltered `Read` of a large history relation** where
     a key lookup is possible. Often caused by the predicate not reaching the
     indexed column.
   - **Redundant self-join** of a relation already implied upstream (e.g. joining
     `replica_history` again after `replica_metrics_history` was derived from it).
     Drop it; it adds an operator and can fan out.
   - **Missing temporal filter** on an append-with-retention history relation
     (`*_history`). Without `WHERE occurred_at + INTERVAL '...' >= mz_now()` the
     query materializes unbounded history.
   - **Fleet-wide aggregation for a single-entity view** (e.g. `max(occurred_at)
     GROUP BY cluster` across all clusters when only one is shown).
4. Write down the plan finding + a concrete rewrite hypothesis. If there is no
   anti-pattern, record that the query is already well-shaped and stop.

`EXPLAIN OPTIMIZED PLAN AS TEXT FOR ...` gives the operator tree; the `Source ...
pushdown=(...)` / `filter=(...)` footer lines tell you what reached the storage
read. Grep the plan for the entity column (`cluster_id`, `replica_id`) to see
where the filter landed.

## Phase 2 — measure magnitude with a FAITHFUL synthetic sweep (only for candidates)

Plan diagnosis tells you *whether* a rewrite helps; the sweep tells you *how
much*. The real builtin sources can't be populated by hand (the controller writes
them), so shadow them with user tables. The single most important rule:

> **Replicate the production indexes.** `mz_catalog_server` maintains indexes on
> these relations (see `src/catalog/src/builtin/*.rs`, `BuiltinIndex`). E.g.
> `mz_cluster_replica_metrics_history (replica_id)`,
> `mz_cluster_replica_status_history (replica_id)`,
> `mz_cluster_replica_name_history (id)`,
> `mz_cluster_replica_history (dropped_at)`. A sweep on *unindexed* shadows
> measures a regime that does not exist in production — both old and new
> full-scan, so the result is wrong in both directions. Create the same indexes
> on the shadow tables, on the bench cluster, before measuring.

Setup:

1. **Dedicated bench cluster** `SIZE = 'scale=1,workers=1,mem=32GiB'` — one worker,
   matching `mz_catalog_server`; big memory so you measure cost, not OOM. Never
   pass `--reset` to `environmentd` (it wipes local state).
2. **Shadow tables** with the same columns as the real relations, plus the
   matching `CREATE INDEX`es on the bench cluster.
3. **Synthetic fleet** sized to a realistic scrape (replica metrics at 1/min;
   14-day retention = 20160 samples/replica). Sweep fleet size (e.g. 25 / 100 /
   200 clusters, 2 replicas each).
4. **Run both the OLD and the rewritten query as ad-hoc peeks** for a random
   entity, N concurrent clients, fixed duration. Measure p50/p95 and qps.
5. **Use the actual query text** (or, for a Kysely builder, the `.compile()`d SQL),
   not a hand-paraphrase. Hand-rewrites drift from what ships.

Validate before trusting numbers:

- **Output equivalence:** old and new must return identical rows for the same
  entity. Diff them once at small scale.
- **EXPLAIN on the bench cluster** to confirm the shadow reproduces the same plan
  difference you saw in Phase 1 (filter pushed vs not).
- **Memory deltas (if measuring arrangements):** read `mz_object_arrangement_sizes`
  only after the arrangement settles (consecutive stable reads), and measure
  variants COEXISTING in one cluster state, never build→measure→drop→rebuild
  (cross-run compaction noise can flip a small delta's sign).

Interpreting the sweep:

- These are closed-loop peeks: if one query exceeds the measurement window,
  completed-count ≈ concurrency and p50 ≈ "time each user waits when N load at
  once" — a realistic page-load metric, not sustained QPS. Say so.
- A single-worker cluster serializes concurrent peeks, so both variants degrade
  with concurrency; the point is the *ratio* and how it scales with fleet size.
- Log any caps/skips (e.g. skipping the old query at high concurrency because a
  single run takes minutes) rather than leaving silent holes.

## Pitfalls (learned the hard way)

- **Unindexed shadows mislead.** This is the #1 fidelity bug. Match prod indexes.
- **Don't conflate "indexed vs not" with "rewrite vs not."** To isolate a query
  rewrite, both variants must have identical indexing; only the SQL differs.
- **The plan can change with cluster.** A plan EXPLAINed on the default cluster
  won't use `mz_catalog_server`'s indexes. Always EXPLAIN on the cluster the query
  actually runs on.
- **`statement_timeout`** may not bound a long peek as expected; set it and also
  bound the harness wall-clock.
- **Stale base in CI.** A Console-only change can still fail catalog/upgrade CI if
  the branch predates an unrelated builtin-migration bump. Rebase onto fresh main
  before blaming the change.

## Harness shape (for the Phase-2 sweep)

A small Python driver is enough; the moving parts are:

- **fleet generator**: `DROP/CREATE CLUSTER bench SIZE 'scale=1,workers=1,mem=32GiB'`,
  create shadow tables matching the real relations' columns, generate a synthetic
  fleet (e.g. F clusters x replicas/objects x a realistic sample rate), and
  **`CREATE INDEX` on the bench cluster to mirror every production `BuiltinIndex`**
  on those relations (look them up in `src/catalog/src/builtin/*.rs`).
- **concurrent peek driver**: N threads, each opening a connection, `SET cluster =
  bench`, looping the query for a random entity for a fixed duration, recording
  p50/p95 and qps.
- **old-vs-new sweep**: a `build_sql(entity, pushdown: bool)` that emits the two
  query shapes, run across fleet size x concurrency; dump `EXPLAIN` for each shape
  per scale; diff old-vs-new rows once for equivalence.

Whatever scripts you write, keep them next to the analysis output so the numbers
are reproducible. The one non-negotiable: the shadow indexes must match prod.

## Output

Write a markdown report per query (plan finding, rewrite, EXPLAIN before/after,
sweep table if run, recommendation) plus a one-page index summarizing which
queries have wins and their expected size. Keep raw EXPLAIN/sweep output as
supplemental files so conclusions are traceable.
