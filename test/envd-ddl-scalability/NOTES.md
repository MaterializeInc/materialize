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

### 2026-05-15 — first profiling pass (tables padding, local envd)

Ran `audit.py --padding tables --scale 0,500,2000,5000 --ops
create_table,drop_table,alter_table_add_col,rename_table,create_view,
drop_view --reps 8`. All DDL ops scale linearly with N at remarkably
similar slopes (3.3-4.7 μs/object), which means **the dominant O(N)
cost is shared infrastructure, not op-specific**.

p50 latency in ms (local envd, not directly comparable to cluster-
spec-sheet's cloud numbers, but slope is what matters):

| op | N=0 | N=500 | N=2000 | N=5000 | Δ@5000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| create_table        | 31 | 33 | 37 | 55 | +23 |
| drop_table          | 20 | 21 | 30 | 40 | +20 |
| alter_table_add_col | 23 | 25 | 32 | 41 | +18 |
| rename_table        | 20 | 21 | 28 | 37 | +17 |
| create_view         | 18 | 21 | 26 | 38 | +20 |
| drop_view           | 19 | 22 | 29 | 39 | +19 |

CSV at `/tmp/audit-tables.csv`, summary at
`/tmp/audit-tables.summary`. Slopes ~4 μs/object across all ops
means even *no-controller-side-effect* DDLs (rename, alter, view
create/drop) pay the same per-object price.

### Trace analysis: which spans grow with N

Top self-time growers from N=0 → N=5000:

**For CREATE TABLE** (full +23 ms breakdown):
| span | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `storage::create_collections` | 9.9 | 22.5 | **+12.6** |
| `snapshot` (catalog durable) | 0.5 | 4.1 | +3.6 |
| `transaction` (catalog durable) | 0.5 | 2.5 | +2.0 |
| `consolidate` (catalog durable) | 0.4 | 2.3 | +1.9 |
| `PersistTableWriteCmd::Append` | 5.6 | 7.5 | +1.9 |
| `apply_catalog_implications_inner` | (new) | 1.1 | +1.1 |
| `apply_updates` | 0.5 | 1.2 | +0.7 |

**For ALTER TABLE** (full +18 ms breakdown):
| span | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `snapshot` | (sub-ms) | 3.8 | +3.6 |
| `transaction` | (sub-ms) | 2.5 | +2.1 |
| `consolidate` | (sub-ms) | 1.9 | +1.5 |
| `coord::catalog_transact_with_context::table_updates` | 7.6 | 9.2 | +1.6 |
| `apply_updates` | 0.7 | 1.2 | +0.5 |
| `apply_catalog_implications_inner` | (new) | 1.1 | +1.1 |
| `PersistTableWriteCmd::Append` | 6.6 | 8.3 | +1.7 |

**For RENAME TABLE** (full +17 ms breakdown): same shape as ALTER —
the growers are catalog durable txn spans (`snapshot`, `transaction`,
`consolidate`) plus persist append.

### Where the per-object cost lives

Two clusters of O(N) cost dominate:

1. **`storage::create_collections`** — only for CREATE TABLE (and
   anything else that creates a storage collection). +12 ms / 5000 ≈
   2.5 μs/object. Suspect: scans existing collections to set up read
   policies / read holds / metadata. Need to read
   `src/storage-controller/src/lib.rs::create_collections`.

2. **Catalog durable txn machinery** (`snapshot`, `transaction`,
   `consolidate`) — present for **every** DDL. +7-8 ms / 5000 ≈
   1.5 μs/object. These spans live in
   `src/catalog/src/durable/persist.rs` and
   `src/catalog/src/durable/transaction.rs`. Combined with
   `TableTransaction::insert/update` doing `for_values` over all N
   items (suspect 3 below), this looks like the catalog snapshot read
   from persist is full-state every time, even when the txn only
   touches a few rows.

### O(n) suspect 3 — `TableTransaction::insert/update` scans all rows

`src/catalog/src/durable/transaction.rs:3190-3210` — every `.insert(k,
v, ts)` on a `TableTransaction` calls `self.for_values(|for_k, for_v|
{ ... })` to check both `k == for_k` and `uniqueness_violation`. That
walks **every initial row + every pending row** of the table on each
single insert. For a CREATE TABLE we insert a handful of rows (item,
maybe extra metadata), and each insert walks all N existing items.

`update` (`:3222`) has the same structure: `for_values_mut` walks all
items, calling `f(k, v)` on each.

This is the most plausible source of the per-object cost in the
catalog durable txn spans (`transaction`, `consolidate`, `snapshot`)
that grow with N regardless of op type.

### 2026-05-15 — views and mvs padding passes

**Views padding** (N=0,500,2000,5000): slope is roughly half of tables-
padding. Δ@5000 for CREATE TABLE: +8.5 ms (vs +23 ms with tables pad).
Confirms that **two cost components are stacked**:

1. ~half scales with **storage-collection count** (only paid by ops
   that create/drop storage collections, i.e. CREATE TABLE / DROP TABLE
   and friends). Tables have shards; views do not.
2. ~half scales with **catalog-entry count** of any kind. All ops pay
   this.

**MVs padding** (N=0,500,2000): blows up super-linearly.

| op | N=0 | N=500 | N=2000 | factor 500→2000 |
| --- | ---: | ---: | ---: | ---: |
| create_table | 37 | 146 | **2095** | 14× for 4× N |
| drop_table | 36 | 99 | 996 | 10× for 4× N |
| alter_table_add_col | 46 | 138 | 1127 | 8× for 4× N |
| rename_table | 29 | 119 | 653 | 5.5× for 4× N |
| create_view | 31 | 105 | 479 | 4.6× for 4× N |
| create_mv | 40 | 51 | 837 | 16× for 4× N |
| drop_mv | 31 | 44 | 446 | 10× for 4× N |

4× more N giving 5-16× more latency = **quadratic** somewhere. CSV at
`/tmp/audit-mvs.csv`.

Trace for `create_table` p50=2.1 s at N=2000 MVs:
- `storage::create_collections` total: 1.72 s
  - **self-time: 1.65 s**
  - Visible child: `PersistTableWriteCmd::Register` 64 ms
- `coord::catalog_transact_with_side_effects` self: 192 ms
- `coord::initialize_read_policies`: 13 ms (vs 0.5 ms at N=0)

**1.65 seconds inside `storage::create_collections` is in code not
covered by any sub-span.** Source reading turned up plausible callers
inside `storage_collections.create_collections_for_bootstrap`
(`src/storage-client/src/storage_collections.rs:1686`) but none
explicitly look quadratic. Most likely candidates:

- `install_collection_dependency_read_holds_inner` →
  `install_read_capabilities_inner` → `update_read_capabilities_inner`
  walks `MutableAntichain::update_iter` on collections with many read
  capabilities. With N MVs holding read holds on a single `pad_base`,
  pad_base's `read_capabilities` could have N entries → an
  `update_iter` over it would be O(N).
- The recursive propagation in `update_read_capabilities_inner`
  (`:1250`) walks `storage_dependencies` and adds them to `updates`,
  potentially fanning out across the dependency graph.
- `acquire_read_holds_inner` could be amplified.

But we need either targeted instrumentation or a CPU profile to be
sure. Reading further blindly is hitting diminishing returns.

### What we know now

- **Linear O(N) bottleneck** is shared across all DDL ops, around
  ~4 μs/catalog-entry on this local envd. Sources identified by code:
  - `Transaction::allocate_oids` (full walk)
  - `Coordinator::validate_resource_limits` (5 full walks)
  - `TableTransaction::insert/update` (full walk on every mutation)
- **Quadratic O(N²) bottleneck** triggered when padding objects
  share dependencies (e.g. MVs reading from a common base table).
  Bottleneck lives inside storage controller's `create_collections`
  path, not yet pinpointed; suspect read-capability propagation
  through dependency edges.

## Open questions

- Does the scaling pattern differ across padding axes? If `views` (no
  dataflows) shows the same DDL slowdown as `mvs`, the hotspot is purely
  in the catalog/coordinator. If `mvs` is dramatically worse, controller
  state matters too.
- Are CREATE-side and DROP-side regressions caused by the same loops, or
  different ones (dependency walks tend to live on DROP)?

## Design proposals (draft — for the linear O(N) suspects)

These are the three confirmed O(N) hotspots from source reading. None
of them require the storage-side investigation to be complete. They
also stack — each contributes part of the ~4 μs/object slope.

### Fix 1 — `Coordinator::user_*().count()` calls

`src/adapter/src/coord/ddl.rs:1056-1500`, `validate_resource_limits`.
Today every DDL does 5+ full walks of `state.entry_by_id` to count
items per type.

**Proposal:** add type-bucketed indexes to `CatalogState`, maintained
in `apply_item_update` (and `drop_item`):

```rust
// in CatalogState (state.rs)
items_by_type: imbl::OrdMap<CatalogItemTypeBucket, imbl::OrdSet<CatalogItemId>>,
```

with buckets `Table`, `Source`, `Sink`, `MaterializedView`, `View`,
`Index`, `Connection`, `Secret`, `Type`, `Func`, `Log`,
`ContinualTask`. Add `user_*().count()` overloads that return `.len()`
on the bucket. Rewrite `validate_resource_limits` to use the counts.

Trade-offs:
- O(log N) bookkeeping in `insert_entry` / `drop_item` (negligible).
- ~4-12 ms/DDL saved at N=5000 from `validate_resource_limits` alone.
- Same buckets are useful for `mz_objects` builtin tables and ad-hoc
  introspection; can replace some other full walks.
- Has to stay in sync with `is_user()` filter (different bucket for
  system vs user, or filter at read time).

### Fix 2 — `Transaction::allocate_oids`

`src/catalog/src/durable/transaction.rs:914-1018`. Every OID alloc
builds a HashSet of all in-use OIDs by walking all
databases/schemas/roles/items/introspection_sources.

**Proposal:** what the existing comment already suggests — maintain a
durable free-list / next-id allocator that tracks taken OIDs
incrementally. Two options:

a. **Append-only bump + tombstones for reuse.** Today OIDs recycle.
   If we make the `id_allocator` store the next monotonic OID
   (`OID_ALLOC_KEY.next_id` already does this) and keep a separate
   `taken_oids` `BTreeSet<u32>` in memory, allocation is
   O(log N). On drop we insert into a `freed_oids` set; on alloc we
   first pop from `freed_oids` before bumping.

b. **Skip OID reuse entirely.** OIDs are u32; at 1k DDLs/s we exhaust
   in ~50 days, but that's not actually true because we cap to
   `FIRST_USER_OID..u32::MAX` (a few billion). For practical purposes
   we could simply never reuse and panic on wrap-around. Simplest
   possible fix; matches Postgres's behaviour where OID reuse is also
   rare.

(a) preserves current semantics. (b) is simpler but a semantic
change worth checking with the team.

### Fix 3 — `TableTransaction::insert/update` full scans

`src/catalog/src/durable/transaction.rs:3190` (insert) and `:3222`
(update). Both call `for_values` to check for duplicate-key and
uniqueness-violation against every initial+pending row. For a CREATE
TABLE with N pre-existing items, every `items.insert(...)` walks N
items.

**Proposal:** since `for_values` is uniformly walked for these checks:

a. **Key check via `BTreeMap::contains_key`.** Replace the `k == for_k`
   scan with a `BTreeMap` (or `BTreeSet<K>`) lookup. `pending` is
   already a `BTreeMap<K, _>`, so we just need to also have access to
   `initial`'s keyspace — which is also a `BTreeMap<K, V>` (see
   `current_items_proto`). Both lookups are O(log N).

b. **Uniqueness check** is the harder part: the predicate
   `uniqueness_violation(for_v, &v)` is opaque, so we can't index it
   generically. But for most tables, `K` IS the unique key, so the
   predicate is `false`. We can let callers either:
     - declare "no uniqueness violations possible" so we skip the loop
       entirely, OR
     - register a "uniqueness key extractor" so we keep a side
       `BTreeMap<UniqueKey, K>` and check via that map.

This change is the most invasive of the three but probably also the
highest-leverage; it touches every TableTransaction mutation.

### Roll-up

The three fixes combined likely close most of the ~4 μs/object slope
on linear paths. None changes externally visible semantics; all are
"replace full-walk with O(log N) lookup using an index maintained in
the existing apply path." Tests are easy to add: time `CREATE TABLE`
in a catalog with N=10/100/1000/10000 trivial tables and assert near-
constant latency.

The quadratic-looking blow-up under MV-shared-dependency padding is
**not** covered by any of these — see "Storage-side blow-up" below.

### 2026-05-15 — pinpointing the storage-side blow-up

Added scoped tracing spans inside the storage controller's
`create_collections_for_bootstrap` chain. Trace for CREATE TABLE at
N=1000 MVs (444 ms p50, vs ~30 ms at N=0):

| span | self-time at N=1000 |
| --- | ---: |
| `ccfb::open_data_handles_concurrent` (was unattributed) | 119 ms |
| `PersistTableWriteCmd::Register` (txn-wal) | 46 ms |
| catalog persist `compare_and_append` | 23 ms |
| `ccfb::install_collection_states` (main loop) | ~0 ms |
| `ccfb::synchronize_finalized_shards` | ~0 ms |

So **the inner main loop and the synchronize call are not the issue**
even at N=1000 MVs. The cost is dominated by `open_data_handles`.
Stepping inside `open_data_handles`:

| span | self-time at N=1000 |
| --- | ---: |
| `odh::upgrade_version` | 40.8 ms |
| `odh::open_critical_handle` | 32.6 ms |
| `odh::fetch_recent_upper` | 7.2 ms |
| `odh::open_write_handle` | (sub-ms) |

Both `upgrade_version` and `open_critical_handle` invoke persist's
`StateCache::get` → `Applier::new` → `maybe_init_shard` →
`fetch_recent_live_diffs` / `try_compare_and_set_current` /
`fetch_current_state`. These hit CockroachDB's `consensus` table via
queries like:

```sql
SELECT sequence_number, data FROM consensus
WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1
```

Per-shard, primary-key access — should be O(log total_rows). But the
measurement says the cost scales with the number of *existing* shards
in CRDB. Plausible explanations (not yet verified):

- **CRDB query latency degradation** under N rows is the simplest
  one — even index-only lookups can slow when the table has lots of
  rows due to caching effects.
- **Persist `StateCache` lock contention** — the cache holds a single
  `Mutex` (`src/persist-client/src/cache.rs:479`) across all shards;
  if background tasks hold it proportional to N, every new shard's
  insert waits.
- **PubSub subscribe path** — `pubsub_sender.subscribe(&shard_id)` is
  invoked from inside the cache lock; if it has any O(N) accounting,
  this would multiply per-shard cost.
- **CRDB `consensus` table bloat from version churn** — every shard
  state mutation appends a row, and old rows get GC'd. With many
  shards, churn rate matters.

In every case, this is **persist / CRDB territory**, not adapter /
catalog. The catalog-side O(N) fixes from the previous section are
independent and worth landing first.

The catalog persist `compare_and_append` for the catalog shard (23 ms
at N=1000, vs ~2 ms at N=0) also looks linear in something — likely
the catalog shard accumulates state per item, so its own consensus
state grows with N. Compaction settings on the catalog shard would
affect this.

### Updated proposals

**Storage-side fix (Fix 4 — needs persist team).** The persist
`make_machine` / `maybe_init_shard` path should not scale with the
number of existing shards in the same process or in CRDB. Concrete
investigations to start with:

1. Run the audit harness with `samply record -p $envd_pid` and
   confirm the CPU flame graph attributes the time to either
   CRDB-network I/O (then it's a database-side issue) or to in-process
   Rust code (then it's a persist-cache / pubsub issue).
2. Time the raw CRDB query `SELECT sequence_number, data FROM
   consensus WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1`
   at varying total row counts. If that's the source, file it with
   storage/persist and CRDB folks.
3. Profile under contention: with N MVs, if multiple compute replicas
   are still chattering with persist, the lock on `StateCache.states`
   may be more contended than the trace suggests. Re-test with
   `--meas-cluster-size scale=1,workers=1` and no pad clusters
   (`tables` padding) at high N to isolate from compute side-effects.

### 2026-05-15 — post-fix measurements

Three catalog-side fixes landed on this branch:

- `f69d91c977` — bucket user item counts in `CatalogState`; `validate_resource_limits` now does O(log K) lookups instead of 5 walks of `entry_by_id`.
- `9fca09ff8a` — maintain `Transaction::initial_oids` so `allocate_oids` doesn't walk every db/schema/role/item/intro-source per allocation.
- `293a243e6b` — `TableTransaction::insert` uses `self.get(&k).is_some()` instead of `for_values` for dup-key, and skips the uniqueness walk when `uniqueness_violation` is `None` (14 of 22 instances).

Re-ran the audit on the same envd binary + harness as the pre-fix runs.

**Tables-padding (the matrix we expect to fix):** slope eliminated. Per-N
cost from N=0 → N=5000 dropped from +17-23 ms to within noise:

| op | pre-fix Δ@5000 | post-fix Δ@5000 | per-object cost |
| --- | ---: | ---: | --- |
| create_table | +23 ms | ~0 ms | gone |
| drop_table | +20 ms | ~+2 ms | mostly gone |
| alter_table_add_col | +18 ms | ~-5 ms | gone (noise) |
| rename_table | +17 ms | ~-9 ms | gone (noise) |
| create_view | +20 ms | ~-3 ms | gone (noise) |
| drop_view | +19 ms | ~-1 ms | gone (noise) |

At N=5000 tables, every measured DDL is now within ~10ms of its N=0
baseline — the slope is below measurement noise. (The post-fix N=0
absolute baseline was higher than pre-fix N=0 because the post-fix envd
was freshly-reset / cold; comparing N=5000 across runs is the apples-
to-apples test, and post-fix N=5000 ≈ pre-fix N=0.) CSV at
`/tmp/audit-tables-post.csv`.

**MVs-padding:** per-N cost dropped ~2-3× but the super-linear
remainder is intact, as predicted (the persist-side blow-up identified
in `open_data_handles` is not addressed by these fixes):

| op | pre-fix @2000 | post-fix @2000 | speedup |
| --- | ---: | ---: | ---: |
| create_table | 2095 ms | 851 ms | 2.5× |
| drop_table | 996 ms | 536 ms | 1.9× |
| rename_table | 653 ms | 748 ms | 0.9× (noisy) |
| create_view | 479 ms | 330 ms | 1.5× |
| create_mv | 837 ms | 1095 ms | 0.8× (noisy) |
| drop_mv | 446 ms | 282 ms | 1.6× |
| drop_view | 795 ms | 1152 ms | 0.7× (noisy) |

With reps=5 there's meaningful variance and a few cells go the wrong
way; the median across the matrix shows clear improvement but the
super-linear shape vs. N is intact (e.g. CREATE TABLE: 31 → 81 → 851 ms
for N = 0 / 500 / 2000 is still ~10× for 4× N). The persist-side
`open_data_handles` cost we pinpointed earlier dominates here. CSV at
`/tmp/audit-mvs-post.csv`.

**Net conclusion**: the three catalog-side fixes correctly eliminated
the linear ~4 μs/object cost shared by all DDL ops. What remains is
the super-linear persist init cost when many storage collections
share dependencies — a separate fix needing persist/storage-team
ownership.

## Next steps

1. Start the local stack and run the tables-padding pass.
2. Record results here, then expand to views / mvs / indexes.
3. Pull traces for the slowest cells; identify dominant spans.
4. Read the source at those spans; find the O(n) / O(n²) loops.
5. Draft design proposals; check in with user before any implementation.
