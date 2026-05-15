# envd DDL scalability audit — working notes

Living document. Append to it as the investigation progresses. Keep this
short and load-bearing — anything worth keeping for the long term graduates
to README.md or a code comment.

## Mission

Audit DDL and catalog-transaction code paths in `environmentd` for code
that scales with the number of catalog objects (O(n) or worse), starting
from empirical scaling measurements, narrowing via tracing, and ending
with concrete design proposals for the worst offenders.

Anchor signal (`test/cluster-spec-sheet`, `envd_scalability_mvs`, branch
`envd-ddl-scalability`): with N MVs in the catalog, `CREATE TABLE` p50
grows from 13 ms at N=1 to 88 ms at N=5000 (~6.8×). Peeks stay flat
(~5 ms), so the regression is on the write/catalog path, not the read
path.

## Working agreement

- Drive autonomously to a good conclusion; ask the user when blocked or
  before doing anything irreversible.
- Commit + push as we go — this machine may go down, so unsaved work is
  lost work. Branch: `envd-ddl-scalability`.
- This file is the durable context. Re-read it on session restart.

## Tooling we built

- `test/envd-ddl-scalability/audit.py` — tight standalone harness that
  connects to a running envd, pads the catalog with N objects of one
  type (`tables`, `views`, `mvs`, `indexes`), and times CREATE / DROP /
  ALTER / RENAME of various object types at each scale point. Captures
  trace IDs from `emit_trace_id_notice` for Tempo lookup.
- `test/envd-ddl-scalability/README.md` — how to run; profiling notes.

## Decisions log

- 2026-05-15 — Use a standalone Python script (not mzcompose) so the
  iteration loop is tight: one envd startup, many harness runs. Profiling
  goes through the canonical `bin/environmentd --optimized --monitoring`
  flow.
- 2026-05-15 — Cover all four padding axes (tables / views / mvs /
  indexes) and CREATE/DROP/ALTER/RENAME up front; we don't yet know
  which axis exposes the worst loops.
- 2026-05-15 — Pad MVs/indexes are sharded across `audit_pad_c_<k>`
  clusters with 400 dataflows per cluster, so small replica sizes can
  host pad load without dataflow pressure dominating the catalog signal.

## Findings log

### 2026-05-15 — source-level survey of the DDL hot path (before traces)

Anchor doc: `doc/developer/generated/flows.md` "Catalog mutation (DDL)"
section. The path for a single DDL statement:

1. `mz_sql::plan::statement::ddl::plan_*` — plans the statement.
2. `mz_adapter::coord::sequencer::inner` — calls `catalog_transact`.
3. `mz_adapter::coord::ddl::catalog_transact_inner`
   (`src/adapter/src/coord/ddl.rs:311`).
4. `mz_adapter::catalog::transact::transact`
   (`src/adapter/src/catalog/transact.rs:420`) →
   `transact_inner` (`:636`).
5. Durable commit via `Transaction::commit` →
   `mz_catalog::durable::persist::commit_transaction`.
6. In-memory `CatalogState::apply_updates`
   (`src/adapter/src/catalog/apply.rs:102`).
7. `Coordinator::apply_catalog_implications`
   (`src/adapter/src/coord/catalog_implications.rs:84`).

**Suspect: doubled in-memory apply.** `transact_inner` applies updates
twice per call: once to a Cow-cloned `preliminary_state` (per op, in
case successive ops in a batch read modified state), and once to a
separate `state` Cow at the end (`transact.rs:696-797`). The comment at
`transact.rs:670` explicitly notes "We won't win any DDL throughput
benchmarks" and acknowledges the doubled work.

**Not the suspect: state cloning.** `CatalogState` (`state.rs:113`) uses
`imbl::OrdMap` for all its catalog maps; `imbl` clones are O(1)
structural (persistent BTrees). So the `Cow::to_mut()` triggers in
`transact_inner` and the `Arc::make_mut(catalog)` in `ddl.rs:475` are
cheap — not the cause of O(n) growth.

**Suspect: builtin-table updates.** Each DDL produces
`builtin_table_updates`. Some of these are derived via
`generate_builtin_table_update` per `StateUpdateKind` and may walk
catalog-wide state (e.g. computing privilege rows). Then
`builtin_table_update().execute(builtin_table_updates).await`
(`ddl.rs:516-519`) appends them to system tables. Worth tracing what
this actually does at scale.

**Empirical slope (from cluster-spec-sheet `envd_scalability_mvs`):**

```
N        p50 CREATE TABLE
1            13 ms
100          15 ms   (~10 ms baseline)
1000         26 ms
3000         58 ms
5000         88 ms
```

Linear fit: ~10 ms baseline + ~15 μs/object. Consistent with a single
O(n) walk somewhere; not an O(n²) so far at N≤5000. We may see O(n²)
appear at higher N if a quadratic term is small but nonzero, or if a
per-object op gets more expensive at scale.

### O(n) suspect 1 — `Transaction::allocate_oids`

`src/catalog/src/durable/transaction.rs:914-1018` walks every database,
schema, role, item, and introspection source on **every OID allocation**,
inserting their existing OIDs into a `HashSet`, then scans integers
starting from `id_allocator[OID_ALLOC_KEY]` until it finds one not in
the set. Comment at line 944-948:

> This is potentially slow to do everytime we allocate an OID. A faster
> approach might be to have an ID allocator that is updated everytime an
> OID is allocated or de-allocated. However, benchmarking shows that
> this doesn't make a noticeable difference and the other approach
> requires making sure that allocator always stays in-sync which can be
> error-prone. **If DDL starts slowing down, this is a good place to
> try and optimize.**

DDL has started slowing down. At N=5000 items, the hashset build is ~50 μs
of pure compute; called once or more per DDL. Worth verifying in traces
whether this accounts for a meaningful fraction of the per-object slope.

### O(n) suspect 2 — `Coordinator::validate_resource_limits`

`src/adapter/src/coord/ddl.rs:1056-1500` runs **before** every catalog
transact. For a CREATE TABLE it does **five** full walks of
`entry_by_id`, one per object type:

| line | call | cost |
| --- | --- | --- |
| 1300 | `for c in self.catalog().user_connections()` | walks all entries, filters connections |
| 1353 | `self.catalog().user_tables().count()` | walks all, counts tables |
| 1360 | `self.catalog().user_sources()...` | walks all, filters sources |
| 1377 | `self.catalog().user_sinks().count()` | walks all, counts sinks |
| 1384 | `self.catalog().user_materialized_views().count()` | walks all, counts MVs |

Each `user_*()` is defined in `src/adapter/src/catalog.rs:1094-1122` as
`self.entries().filter(...)` over `state.entry_by_id` — there's no
type-bucketed index. So at N=5000 items we burn 25k iterations of the
`imbl::OrdMap`, plus filter predicates, plus the `is_user()`
discriminator check. Estimated ~5-15 ms of pure compute per CREATE at
N=5000 — sizable fraction of the ~75 ms slope, but probably not the
whole thing.

Fix would be cheap: maintain per-type sets (a `BTreeSet<CatalogItemId>`
per object type) updated in `apply_*_update`, then turn each
`user_tables().count()` into an O(1) `.len()`. Or just maintain
running counters.

**Tracing instrumentation already present** in DDL path:
- `catalog::transact` (`#[instrument]` at `transact.rs:419`)
- `catalog::transact_inner` (`:636`)
- `coord::catalog_transact_with::finalize` (`ddl.rs:565`)
- per-update `apply_*_update` helpers under `apply.rs` (level=debug)
- `apply_updates_inner` (`:203`)
- `apply_catalog_implications` metric on coord

With `opentelemetry_filter=debug` we get the debug-level spans, so the
per-update applies are visible. That should let us identify which kind
of update is slow and how its self-time scales with N.

## Open questions

- Does the scaling pattern differ across padding axes? If `views` (no
  dataflows) shows the same DDL slowdown as `mvs`, the hotspot is purely
  in the catalog/coordinator. If `mvs` is dramatically worse, controller
  state matters too.
- Are CREATE-side and DROP-side regressions caused by the same loops, or
  different ones (dependency walks tend to live on DROP)?

## Next steps

1. Start the local stack and run the tables-padding pass.
2. Record results here, then expand to views / mvs / indexes.
3. Pull traces for the slowest cells; identify dominant spans.
4. Read the source at those spans; find the O(n) / O(n²) loops.
5. Draft design proposals; check in with user before any implementation.
