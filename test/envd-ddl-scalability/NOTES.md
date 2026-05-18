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

### 2026-05-15 — post-fix measurements (CORRECTED)

Three catalog-side fixes landed on this branch:

- `f69d91c977` — bucket user item counts in `CatalogState`; `validate_resource_limits` now does O(log K) lookups instead of 5 walks of `entry_by_id`.
- `9fca09ff8a` — maintain `Transaction::initial_oids` so `allocate_oids` doesn't walk every db/schema/role/item/intro-source per allocation.
- `293a243e6b` — `TableTransaction::insert` uses `self.get(&k).is_some()` instead of `for_values` for dup-key, and skips the uniqueness walk when `uniqueness_violation` is `None` (14 of 22 instances).

**First post-fix run was misleading.** The pre-fix tables audit ran on
an envd that had been up ~30 minutes (warm caches); the first post-fix
audit ran on a freshly-reset envd with cold caches and showed N=0 at
~60 ms vs pre-fix N=0 at ~30 ms. That made the post-fix N=5000 number
(~50 ms) look like a flat line vs N=0, suggesting "slope eliminated."
It wasn't — the slope just looked flat because N=0 was inflated.

**Apples-to-apples comparison on a warm envd:** the slope is essentially
unchanged.

| op | pre-fix N=0 / N=5000 / Δ | post-fix N=0 / N=5000 / Δ |
| --- | --- | --- |
| create_table | 31 / 55 / **+23 ms** | 30 / 51 / **+21 ms** |
| drop_table | 20 / 40 / +20 ms | 20 / 41 / +21 ms |
| rename_table | 20 / 37 / +17 ms | 19 / 39 / +19 ms |

CSV at `/tmp/audit-tables-warm.csv`. The three catalog-side fixes are
theoretically correct (they eliminate the named O(N) loops), but their
combined contribution to the ~4 μs/object slope is **lost in noise** at
N=5000. Implication: the named loops were not the slope's dominant
source. There's another O(N) elsewhere in the per-DDL path that the
audit hadn't pinpointed.

Candidates not yet ruled out:
- The `snapshot` span (catalog durable txn read) grows linearly with N
  in the traces — 0.5 → 4 ms across the range. We attributed this to
  `TableTransaction::insert/update` for_values, but `insert` is now
  fast. So the per-DDL `snapshot` cost has another source.
- `compare_and_append` for the catalog shard grows similarly (2 ms →
  ~5 ms). The catalog shard accumulates a row per item; persist's
  per-row consensus operation might genuinely scale.
- `apply_updates` self-time in the in-memory catalog grew mildly
  (0.5 → 1.2 ms). Some bookkeeping there scales we haven't seen.
- Things in `apply_catalog_implications_inner` that we didn't dig into.

Need to fetch a fresh post-fix high-N trace and rank growers again.

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

**Net conclusion (corrected above)**: the three catalog-side fixes are
each individually correct but eliminate a small fraction of the slope.
The dominant per-DDL O(N) cost is elsewhere — see the trace ranking
below.

### 2026-05-15 — post-fix high-N trace ranking (where the slope actually lives)

Re-ran the audit on a warm, post-fix envd with `--padding tables
--scale 0,5000 --ops create_table,drop_table,alter_table_add_col,
rename_table --reps 8`. CSV at `/tmp/audit-trace-postfix-N5000.csv`.
Latencies (p50):

| op | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| create_table | 30.0 | 47.3 | +17.3 ms |
| drop_table | 18.6 | 42.6 | +24.0 ms |
| alter_table_add_col | 22.3 | 53.0 | +30.7 ms |
| rename_table | 17.4 | 43.6 | +26.2 ms |

Span self-times averaged over 4 traces per cell, ranked by N=0→N=5000
growth. Numbers are self-time at N=5000 (the delta from N=0 is the
relevant signal but cells without entries at N=0 mean the span had
sub-threshold self-time). Pulled via
`test/envd-ddl-scalability/summarize_traces.py`.

**CREATE TABLE** — explains roughly 13 of the +17 ms slope:

| span | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `snapshot` (catalog durable) | 509μs | 4.1ms | **+3.6ms** |
| `group_commit_apply::append_fut` | 5.0ms | 7.6ms | +2.6ms |
| `PersistTableWriteCmd::Append` | 5.0ms | 7.6ms | +2.6ms |
| `transaction` (catalog durable) | 426μs | 3.0ms | **+2.6ms** |
| `consolidate` (catalog durable) | 449μs | 2.1ms | +1.7ms |
| `apply_catalog_implications_inner` | <0.5ms | 1.2ms | +1.2ms |
| `apply_updates` | 536μs | 1.4ms | +0.9ms |

**ALTER TABLE** — explains roughly 17 of the +31 ms slope:

| span | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `coord::catalog_transact_with_context::table_updates` | 7.3ms | 12.9ms | **+5.6ms** |
| `group_commit_apply::append_fut` | 6.2ms | 10.9ms | +4.7ms |
| `snapshot` | 551μs | 4.3ms | **+3.8ms** |
| `transaction` | 453μs | 3.2ms | **+2.7ms** |
| `consolidate` | 447μs | 1.9ms | +1.5ms |
| `apply_catalog_implications_inner` | <0.5ms | 1.3ms | +1.3ms |

**DROP TABLE** and **RENAME TABLE** have the same shape (snapshot,
transaction, consolidate, append, apply_catalog_implications_inner
all grow), and don't add new growers.

### Mapping growers to code

1. **`snapshot` + `transaction` + `consolidate` — the structural target.**
   - `with_snapshot` (`src/catalog/src/durable/persist.rs:766`) walks
     the consolidated trace and rebuilds a `Snapshot { databases:
     BTreeMap, schemas: BTreeMap, items: BTreeMap, ... }` from scratch
     on every transaction. With N=5000 items, that's 5000 inserts into
     the items BTreeMap alone.
   - `Transaction::new` (`transaction.rs:128-232`) then walks every
     row of every snapshot table and calls `TableTransaction::new(...)`
     for each, which itself does
     `initial.into_iter().map(RustType::from_proto).collect()` — a
     full O(N) proto→Rust conversion + BTreeMap construction.
   - `consolidate` (`persist.rs:706`) re-consolidates the in-memory
     trace via `differential_dataflow::consolidation::
     consolidate_updates`. Trace grows with N → consolidation cost
     grows with N.
   - **Combined growth**: +7-8 ms / 5000 ≈ 1.5 μs/object. The single
     biggest source of the per-DDL slope.
   - The proposed design (Arc'd `DurableCatalogData` + per-txn overlay)
     eliminates all three: starting a transaction becomes
     `Arc::clone(&self.data)`, reads probe overlay then base, commits
     emit only delta keys, no full state materialisation per txn.

2. **`group_commit_apply::append_fut` / `PersistTableWriteCmd::Append`
   — persist-side, partly out of scope.** Growth here is the catalog
   shard's consensus append getting bigger as the catalog accumulates
   rows. Some of this should drop when the durable txn emits only
   delta keys (because the per-txn batch is smaller), but a meaningful
   chunk is persist's own state machinery scaling with shard history.
   Treat as a follow-up; the structural fix above does *some* of the
   work indirectly.

3. **`coord::catalog_transact_with_context::table_updates` (+5.6 ms
   for ALTER) — separate hot path.** This is the wrapper span around
   the catalog transact for table-mutating DDL. Its self-time grew
   most for ALTER. Worth instrumenting deeper inside before
   implementation — there may be a smaller, easily-fixed loop hiding
   in here.

4. **`apply_catalog_implications_inner` (+1.2-1.3 ms across ops).**
   `src/adapter/src/coord/catalog_implications.rs:182`. Iterates the
   per-DDL `implications` list (which is delta-sized, so itself O(1)),
   then has handlers per kind. The growth implies one of the handlers
   reads catalog-wide state. Should be auditable in a focused pass.

5. **`apply_updates` (+0.9 ms).** Small but real. The in-memory
   `CatalogState::apply_updates` path; should be O(delta) by design,
   but something inside scales mildly. Worth auditing in the same
   pass as item 4 above.

### Revised design priorities (confirmed by trace)

The trace confirms the proposed design's primary target is the right
one. In priority order:

1. **Shared, indexed durable state + per-txn overlay.** Kills the
   `snapshot` + `transaction` + `consolidate` triple. Estimated
   payoff: ~1.5 μs/object → roughly halves the slope at N=5000.
2. **Indexes for name/OID/namespace lookups.** Eliminates the
   `for_values` walks; mostly already done via Fix 3 (insert) but
   `update`/`for_values_mut` and 8 of 22 `insert` callers still walk.
   Estimated payoff: small at N=5000 (already covered by Fix 3
   partially) but compounds at higher N.
3. **Storing the durable txn overlay in `TransactionOps::DDL` and
   stopping op replay.** Highest-leverage for multi-statement DDL
   transactions; doesn't show up in our single-statement audit, so
   not visible in the slope numbers above. Still worth doing.
4. **Audit `apply_catalog_implications_inner` and `apply_updates`.**
   Both grow mildly with N; find the loop, fix it. Likely small,
   focused changes.
5. **Point APIs for storage metadata.** As designed.
6. *Out of scope for now:* persist-side `open_data_handles` /
   `compare_and_append` cost. Real but separate workstream.

`apply_catalog_implications_inner` and `apply_updates` together add
~+2 ms at N=5000 — small enough to leave for the audit pass per
item 4 above, but worth fixing for completeness.

The OID-set cache (Fix 2) is correctly noted as *not* moving the
needle for single-DDL transactions, because building the set on
every txn start is O(N) regardless of how fast lookups are.
Single-DDL is the common case. The cache only pays off if it's
shared incrementally across transactions — which the proposed design
provides via the Arc'd durable state.

## Next steps

1. Land the structural fix: `DurableCatalogData` (`Arc<imbl::OrdMap>`
   per table) maintained incrementally in `PersistHandle`, with
   per-txn overlay reads and delta-only commits.
2. Audit `apply_catalog_implications_inner` and `apply_updates` for
   the residual ~+2 ms growth.
3. Storage point APIs for catalog-side storage metadata reads.
4. Move durable txn overlay into `TransactionOps::DDL` to kill op
   replay across multi-statement DDL transactions.

### 2026-05-15 — post-design measurements (after steps 1-7)

Steps 1-7 of `doc/developer/design/20260515_ddl_catalog_o_delta.md`
landed. Re-ran the audit on a warm release envd:

```
bin/environmentd --release
python3 test/envd-ddl-scalability/audit.py \
    --padding tables --scale 0,5000,10000 \
    --ops create_table,drop_table,alter_table_add_col,rename_table \
    --reps 8
```

p50 latency in ms:

| op | N=0 | N=5000 | Δ@5000 | N=10000 | Δ@10000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| create_table        | 26.3 | 34.0 | +7.7 | 41.1 | +14.8 |
| drop_table          | 17.2 | 24.8 | +7.6 | 34.4 | +17.2 |
| alter_table_add_col | 20.7 | 27.1 | +6.4 | 36.6 | +15.9 |
| rename_table        | 17.1 | 22.8 | +5.7 | 31.3 | +14.2 |

Pre-design baseline (the "post-fix high-N trace ranking" section
above), reps=8, warm envd:

| op | pre-design Δ@5000 | post-design Δ@5000 | reduction |
| --- | ---: | ---: | ---: |
| create_table        | +17.3 | +7.7 | 55% |
| drop_table          | +24.0 | +7.6 | 68% |
| alter_table_add_col | +30.7 | +6.4 | 79% |
| rename_table        | +26.2 | +5.7 | 78% |

Slope dropped from ~3-6 μs/object to ~1.4-1.7 μs/object — a roughly
half to a quarter of the pre-design rate. The design's success
criterion #1 was "≤ +5 ms"; we land at +5.7 to +7.7 ms, very close
but not quite there. Criterion #2 ("invariant under add-only growth")
is not satisfied either — N=10000 still adds ~7 ms over N=5000.

The residual slope is almost certainly the persist-side cost on the
catalog shard (`PersistTableWriteCmd::Append` / `compare_and_append`)
that the design explicitly lists as out-of-scope (§"Out of Scope" →
"Persist-side scaling of the catalog shard"). The trace in the
pre-design ranking attributed +2.6 ms / 5000 to those two spans
alone; that cost is now the dominant per-object grower.

The remaining handful-of-ms gap to the success target needs a
follow-on persist/storage workstream, not more catalog-side work.

### 2026-05-15 — bogo-consensus probe (does the residual slope live in CRDB?)

Cherry-picked `819a69a6d1` ("persist: add bogo-consensus, an in-memory
gRPC Consensus backend") onto this branch as the experiment vehicle.
The hypothesis was: if the post-design residual ~1.5 μs/object slope
is dominated by persist's `compare_and_append` round-trip to CRDB,
swapping the consensus backend for an in-memory gRPC service should
reduce or eliminate that slope, giving us cleaner signal for the next
round of catalog-side work.

Setup: `bin/environmentd --optimized` against
`bogo://127.0.0.1:6882`, CRDB still in the loop for the timestamp
oracle. Default 4 MiB gRPC message size cap is too small for the
catalog shard once history accumulates; bumped client and server to
256 MiB (server: `BogoGrpcServer::new(...).max_decoding_message_size`
/ `max_encoding_message_size`; client: same on `TonicClient`).
Without this fix the catalog shard's `scan` retries-with-backoff
indefinitely once history grows past 4 MiB; the audit completes but
the latencies are dominated by retry sleeps, not actual work.

Numbers (warm envd, `--padding tables --scale 0,5000 --reps 8`),
side by side with the optimized CRDB baseline:

| op | CRDB N=0 | CRDB N=5000 | CRDB Δ | bogo N=0 | bogo N=5000 | bogo Δ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| create_table        | 31.8 | 39.4 | +7.6  | 104.8 | 150.6 | +45.8 |
| drop_table          | 19.0 | 31.2 | +12.2 |  46.9 |  70.3 | +23.4 |
| alter_table_add_col | 23.8 | 34.0 | +10.2 |  62.6 | 119.3 | +56.7 |
| rename_table        | 18.9 | 29.9 | +11.0 |  57.9 |  76.3 | +18.4 |

Per-object slope under bogo (≈3.7-11.3 μs/object) is **higher**, not
lower, than under CRDB (≈1.5-2.4 μs/object). Absolute latencies are
also 2-4× worse. Padding the catalog (5000 sequential CREATE TABLEs)
took 551 s under bogo vs 193 s under CRDB.

That's an informative negative result. Interpretations:

- The residual slope is **not** dominated by CRDB query latency. If
  it were, replacing CRDB with a local in-process state machine would
  drop it. It doesn't — it grows.
- The bogo backend's `Vec<VersionedData>`-per-key model with a single
  global `Mutex` doesn't beat CRDB for this workload. Each persist
  scan currently transfers history-shaped state over gRPC + protobuf;
  the cost grows with shard history, and the global mutex serialises
  every persist op. CRDB's row-level locking and binary client
  protocol are tighter at this size.
- The persist-side residual on CRDB is likely in `Applier::new` /
  `maybe_init_shard` / `compare_and_append`'s own state-machine
  bookkeeping (state encoding, version walks, listen plumbing) rather
  than in the SQL roundtrip itself. That matches the earlier finding
  in this file that `open_data_handles` dominated MV padding.

What this means for the next round of catalog-side work: there isn't
a hidden CRDB-latency factor masking a deeper O(n) loop in adapter or
catalog. The residual ~1.5 μs/object on CRDB really does look like
persist-internal cost. Further reductions need to come from either
(a) persist-side scaling work (smaller per-DDL batches, smarter state
caching), or (b) batching multiple DDL operations into a single
persist `compare_and_append` — the design's step 5 already does this
for multi-statement DDL transactions, but single-statement DDL still
pays once per statement.

### 2026-05-18 — why was bogo slower? HTTP/2 flow control on small RPCs

The earlier conclusion ("the global mutex serialises every persist op")
was wrong. A focused microbench against `BogoConsensus` vs `MemConsensus`
(see `src/persist/src/bogo.rs::bogo_consensus_microbench`) showed three
things:

1. **The Mutex was never the bottleneck.** Concurrent CAS on 64 shards
   hits 30-50k ops/s on bogo — far short of single-mutex contention
   limits; the in-memory work under the lock is ~1 μs/op, leaving the
   lock idle.
2. **Per-op gRPC overhead is the floor.** On loopback, the smallest
   bogo RPC (head with no data) takes ~100 μs round-trip. That alone
   is ~3× a typical CRDB consensus call latency under load, before
   any payload is in scope.
3. **`tonic` defaults Nagle and HTTP/2 flow control windows to the
   spec minimum (65 KiB).** Per-call CAS latency on the bench rises
   linearly with payload size at ~130 ns/byte (≈ 7 MB/s effective
   throughput) — because every request whose payload spans the
   connection's send window stalls waiting for the server's
   WINDOW_UPDATE round-trip. The catalog shard's appends grow well
   past 64 KiB once history accumulates.

The fix: bump `initial_stream_window_size` to 8 MiB and
`initial_connection_window_size` to 16 MiB on both client and server,
and enable `tcp_nodelay(true)` on the server. With those, 16 KiB CAS
drops from 2204 μs/op to 241 μs/op (9×), and concurrent throughput at
concurrency=16 rises from 21 K ops/s to 50 K ops/s. Landed in
`4fe0d584e2` ("bogo-consensus: raise HTTP/2 flow control windows; add
microbench").

### 2026-05-18 — post-window-fix DDL audit

Re-ran the audit on a warm release envd, exact same shape as the
pre-window-fix bogo numbers above. Side-by-side, including the
optimized CRDB baseline so we can see how much of the gap the window
fix closed:

| op | CRDB Δ | bogo pre-fix Δ | bogo post-fix Δ | slope reduction |
| --- | ---: | ---: | ---: | ---: |
| create_table        |  +7.6 | +45.8 | +18.3 | 60% |
| drop_table          | +12.2 | +23.4 |  +7.1 | 70% |
| alter_table_add_col | +10.2 | +56.7 | (noisy) | — |
| rename_table        | +11.0 | +18.4 | (noisy) | — |

`alter` and `rename` got too noisy at reps=8 to interpret (a few
single-rep stalls dominate the p50/p95). `create_table` and
`drop_table` are the cleanest signal and show ~60-70% of the bogo
slope coming out — bogo now within ~2× of CRDB's slope, where before
it was 3-6×.

Absolute N=0 latency improved too — bogo `create_table` N=0: 104.8 →
54.3 ms — but bogo's baseline is still 1.5-2× CRDB's at N=0. Looking
at envd's persist-client metric for `consensus_cas` under bogo (199 K
calls over the audit run): **p50 ≈ 500 μs, p95 ≈ 64 ms, max 256 ms**,
mean 10.2 ms. Compare to CRDB on the same envd build: p50 ≈ 1.8 ms,
p95 ≈ 4 ms, max ~32 ms, mean 1.8 ms. So bogo's p50 is actually
*better* than CRDB's, but a fat 5-10% tail at 32-64+ ms drives up the
mean and dominates DDL latency.

The bogo server itself is fast across the same period (server-side
mean ~1 μs per CAS, no spikes above 64 μs). So the tail lives in
the gRPC client path / network / persist's `Tasked` task-hop, not
in bogo. Most plausible culprit: HTTP/2 head-of-line blocking on the
single shared connection — a multi-MB scan response stalls the TCP
receive buffer and all sibling streams (small CAS responses) wait
behind it. Server-side metrics show the scan/CAS bytes total roughly
matches the mean op latency × count gap, consistent with HoL.

### Next moves for bogo perf (if we want to keep pushing)

1. **Multiple parallel gRPC channels** in `BogoConsensus`, round-robin
   per-RPC. Removes HoL blocking between large scans and small CAS
   requests. Probably the single biggest remaining win.
2. **Stream scans** instead of returning a `Vec<VersionedData>`.
   Server pushes entries incrementally; client assembles. Reduces
   peak buffer occupancy and protobuf-encode cost.
3. **Move bogo metrics off the hot path.** The
   `update_state_metrics` walk runs under the global lock on every
   CAS/truncate, and four `with_label_values(&[...])` lookups
   happen per RPC; small but adds up at high RPS.
4. **UDS instead of loopback TCP.** Drops kernel TCP overhead;
   tonic supports it via `tower::service_fn`.

None of these change semantics; all are isolated to
`mz-bogo-consensus` + the `mz_persist::bogo` adapter.

### 2026-05-18 — multi-channel client (item 1 above) landed

`15f741cf73` ("bogo-consensus: fan out client RPCs across multiple
gRPC channels") opens 8 independent tonic `Channel`s in the
`BogoConsensusClient` and round-robins RPCs across them.

Microbench delta (`bogo_consensus_microbench`, same machine,
release):

|                          | single channel | 8 channels |
| ---:                     | ---:           | ---:       |
| serial CAS, 16 KiB       | 240 μs         | 146 μs     |
| concurrent CAS, conc=64  | 32 K/s         | 67 K/s     |

Single-op serial latency on the smallest payloads is flat — the
loopback round-trip floor (~90 μs) doesn't depend on connection
count.

### 2026-05-18 — DDL audit, multi-channel bogo (this is the headline)

After both fixes (HTTP/2 windows + 8-way channel fan-out), bogo is
**faster than CRDB across the board**, both N=0 and N=5000.

p50 latency in ms (warm envd, `--padding tables --reps 8`):

| op | CRDB N=0 | bogo+mc N=0 | CRDB N=5000 | bogo+mc N=5000 |
| --- | ---: | ---: | ---: | ---: |
| create_table        | 24.6 | **14.8** | 39.4 | **24.0** |
| drop_table          | 15.1 | **11.0** | 31.2 | **21.9** |
| alter_table_add_col | 17.8 | **13.7** | 34.0 | **22.1** |
| rename_table        | 13.7 | **10.5** | 29.9 | **20.1** |

Bogo wins every cell. The win at N=0 is 20-40%; the win at N=5000 is
about the same in absolute terms (~8-15 ms), so the deltas hold even
as the catalog grows. The 5000-table pad itself took 184 s under bogo
(was 551 s pre-fix, 193 s under CRDB).

Why this works despite the consensus_cas mean from envd's persist
client *not* having dropped (still ~11 ms): with multiple connections,
the slow ops in the long tail no longer block sibling RPCs on the
same connection. The p50 CAS is still sub-millisecond; the tail still
exists but it now runs concurrently with the rest of the DDL work
instead of serially gating it. Eliminating the head-of-line blocking
is the real fix even though the mean per-op number looks unchanged.

### Done

The original probe asked whether the residual post-design slope was
CRDB-bound. The first answer (pre-fix bogo: it's not, bogo is even
slower) was misleading because bogo itself was broken on the gRPC
client path. With bogo actually fast, the probe re-runs and confirms
the underlying signal: bogo cleanly beats CRDB on every op and every
scale point measured. The next round of catalog-side work can use
bogo as a clean low-floor reference.

### 2026-05-18 — slope study at N=5000 / 10000 / 15000 (bogo + file blob)

Used a small `bench_profile.py` driver (one-shot, lived under `/tmp`,
not checked in): pads the catalog incrementally with empty tables,
snapshots prometheus + envd RSS, then runs 100 reps of
`CREATE TABLE m_tmp (a int)` + `DROP TABLE m_tmp` while samply is
attached to envd. Blob backend `file://`; consensus backend
multi-channel bogo on loopback; timestamp oracle still CRDB.
`ALTER SYSTEM SET max_tables = 30000` and
`enable_alter_table_add_column = true` set up-front.

Headline per-rep latencies (warm envd, 100 reps each):

| N      | create p50 | create p95 | create mean | drop p50 | drop p95 | envd RSS |
| ---:   | ---:       | ---:       | ---:        | ---:     | ---:     | ---:     |
|   5000 |   25.9 ms  |   69.9 ms  |   34.3 ms   |  23.3 ms |  32.8 ms | 1618 MB  |
|  10000 |   37.4 ms  |   84.6 ms  |   45.0 ms   |  32.6 ms |  78.3 ms | 2511 MB  |
|  15000 |   52.7 ms  |  125.7 ms  |   67.6 ms   |  45.0 ms | 105.3 ms | 3208 MB  |

Slope: `create_table` p50 adds ≈13 ms per +5000 user tables; p95
grows faster (≈+28 ms per +5000), tail is widening. RSS grows ~700–900
MB per +5000 = ~160 KB per pad table held in memory.

#### envd-internal metric deltas during the 100 reps

(differences between `/metrics` scrapes taken immediately before and
after each measurement window)

| metric                                                                          |   N=5000  |   N=10000 |   N=15000 |
| ---                                                                             |    ---:   |     ---:  |     ---:  |
| `catalog_transact_seconds{method="catalog_transact_with_ddl_transaction"}` mean |  31.95 ms |  41.37 ms |  60.44 ms |
| `catalog_transact_seconds{method="catalog_transact_with_side_effects"}` mean    |  31.95 ms |  41.36 ms |  60.44 ms |
| `consensus_cas` count over 100 DDL reps                                         |    5 744  |    8 450  |   12 100  |
| `consensus_cas` mean (across *all* shards in process)                           |   4.83 ms |  17.41 ms |  46.53 ms |
| `blob_set` count over 100 DDL reps                                              |      702  |      761  |      766  |
| `blob_set` mean                                                                 |   1.29 ms |   1.39 ms |   1.32 ms |
| `mz_catalog_syncs` count                                                        |      630  |      630  |      630  |
| `mz_catalog_transactions_started` count                                         |      210  |      210  |      210  |
| `mz_catalog_transaction_commit_latency_seconds` ∑                               |   0.513 s |   0.838 s |   1.200 s |
| `mz_catalog_sync_latency_seconds` ∑                                             |   0.481 s |   0.823 s |   1.234 s |
| `audit_log` collection entries (live)                                           |   15 472  |   20 682  |   25 892  |
| `storage_collection_metadata` entries                                           |    5 089  |   10 089  |   15 089  |
| `item` entries                                                                  |    5 001  |   10 001  |   15 001  |

`with_ddl_transaction` and `with_side_effects` track exactly together
because for a single-statement CREATE the outer just delegates to the
inner — no explicit DDL transaction.

The numbers that **do not** scale with N are encouraging:

* `blob_set` count and mean are flat (≈7 puts/DDL, ≈1.3 ms each).
  Local file blob with `fsync` is not the slope.
* Counter deltas for `mz_catalog_syncs`, `mz_catalog_transactions_started`,
  `mz_catalog_transaction_commits`, and
  `mz_persist_state_fetch_recent_live_diffs_fast_path` are constant
  across scale (6.3 syncs/DDL, 2.1 commits/DDL, ≈1.4 fast-path live-diff
  fetches/DDL). The *number* of catalog operations per DDL is stable.

The numbers that **do** scale with N (the smoking gun):

* `consensus_cas` count per DDL: 57 → 85 → 121. Linear in N
  (≈ +27 CAS per +5000 user tables).
* `consensus_cas` mean: 4.83 → 17.41 → 46.53 ms. **Super-linear** —
  ratios are 3.6× then 2.7× while N only doubles then 1.5×'s. The mean
  is bagging in tail samples whose individual latency grows worse than
  linearly.
* `catalog_transact_with_ddl_transaction` mean: 31.95 → 41.37 → 60.44 ms.
  Linear-ish (~9–19 ms per +5000), tracking the wall-clock slope.
* `mz_catalog_transaction_commit_latency_seconds` rises ~2× from N=5k
  to N=15k, but commit is only ~5–12 ms of the 32–60 ms DDL — it's not
  the dominant slope inside `catalog_transact`.

Important caveat: `consensus_cas` is **per-RPC across all shards in
the process**, not just the catalog shard. The headline 4.83 → 46.53
mean is dominated by *more*, *slower* CAS on the user-table shards —
each user table is a persist shard, with its own writer doing periodic
maintenance, and at N=15000 we have 3× more shards doing it. The
catalog-shard CAS that DDL actually waits on is just one entry in
that histogram. We'd need per-shard / per-kind labels on
`consensus_cas` to isolate it cleanly.

#### Flame-graph picture at N=5000 (samply attached during the 100-rep window)

Coarse self-CPU breakdown across all envd threads (custom
`app_frames.py` that buckets each sample by the deepest matching app
area on the stack root→leaf):

| area                                            | self %  |
| ---:                                            | ---:    |
| tokio\_fs (fsync / open / rename on blob)       | 64.07%  |
| tokio\_runtime (scheduler / park-unpark)        | 18.35%  |
| mz\_persist\_client                              |  8.25%  |
| tonic\_grpc                                      |  3.83%  |
| libc\_misc                                       |  2.63%  |
| mz\_storage\_controller                          |  0.83%  |
| mz\_compute\_client                              |  0.80%  |
| mz\_adapter::coord                               |  0.42%  |
| alloc                                            |  0.37%  |
| mz\_adapter::catalog::transact / mz\_catalog     |  0.22%  |
| planner / optimizer                              |  ~0%    |

Two surprises in this:

1. **Almost no on-CPU work is in adapter / catalog code paths**
   (`adapter_coord` 0.42%, `catalog_transact` 0.22%, planner 0%). The
   DDL critical path is mostly *waiting*, not computing — coordinator
   work serializes on awaits for persist / controller responses, so
   the CPU profile doesn't tell us much about wall-time.
2. **Two thirds of the process's CPU during the audit is in
   `tokio::fs::*` blocking-pool tasks** doing `fsync`, `__open64`, and
   `rename` against the local-file blob backend. That's a property of
   the `file://` blob URL used here; with `s3://` the CPU mix would
   shift, but the wall-time critical path through `blob_set` (1.3 ms
   per put, flat in N) would be similar.

#### So what scales? Best current hypotheses

1. **Catalog shard's persist state grows linearly with the number of
   catalog updates.** Each CREATE / DROP TABLE writes one diff. The
   single catalog shard accumulates history; every CAS apply has to
   walk that history when reconstructing state on a sync. State
   compaction / rollups eventually truncate it, but in the audit
   window we're racing ahead of compaction.
2. **`apply_catalog_implications` and the in-memory catalog state
   updates** still have some O(N)-per-DDL walks beyond the read-holds
   path that was already fixed in `11be652bf3` (timeline read holds
   made O(delta)). On-CPU under those frames is currently ~0.6%
   combined, so the per-DDL CPU is small — but a 1 ms walk over
   15 000 entries does match the observed slope.
3. **Audit-log writes are appended into a persist shard whose state
   grows linearly with N.** Every DDL appends a row to `audit_log`.
   The shard's batch list / spine grows. Even though `blob_set` and
   `consensus_cas` *counts* per DDL are stable, the per-op cost on
   the `audit_log` shard rises with its history depth.
4. **Per-shard background traffic.** Each user table is a persist
   shard; each shard does writer heartbeats / rollups / live-diff
   fetches in the background. With more shards the *total* CAS rate
   in the process is higher, and these background CAS sit in the
   same histogram as the DDL ones, inflating the mean we see.

(1) and (3) are state-machine cost on specific shards; (2) is
in-memory catalog walk cost on the coordinator; (4) is observation
bias on the histogram, not real DDL latency, but it's still real
work the process is doing.

#### Next moves, in order

1. **Add `shard_kind` labels to `mz_persist_external_op_latency`** —
   break out the catalog shard, the `audit_log` shard, and "user
   collections" separately. That tells us within minutes which kind
   of shard is contributing the slope and which is observation bias.
2. **If catalog shard is the offender**: look at state apply cost in
   the persist client — does the catalog shard hit
   `state_apply_spine_slow_path` more as it grows? Check rollup
   write cadence on the catalog shard at large state sizes.
3. **If audit-log shard is the offender**: aggressive truncation /
   compaction of the `audit_log` shard. Old audit entries are read
   rarely; we don't need to keep the full history hot.
4. **Independently**, hunt remaining O(N) walks per DDL inside
   `catalog_transact_inner` / `apply_catalog_implications`. The
   recent read-holds fix removed one; given on-CPU under those frames
   is 0.6%, any remaining walks should be cheap CPU-wise but still
   show up in wall time.

#### Reproducing the run

`/tmp/bench_profile.py` was intentionally not checked in — it's a
thin psycopg driver that does the padding / measurement loop and
shells out to `samply`. Sketch:

* Spin up bogo on `:6882` and a `--persist-consensus-url=bogo://…`
  envd against `file:///…/blob`.
* On the mz_system port: `ALTER SYSTEM SET max_tables = 30000;
  ALTER SYSTEM SET enable_alter_table_add_column = true;`.
* For each scale point in `[5000, 10000, 15000]`: incrementally pad
  via `CREATE TABLE IF NOT EXISTS audit_pad.pad_t_<i>`; snapshot
  `/metrics` and `/proc/<envd>/status` VmRSS; start
  `samply record -p <envd> -s -o /tmp/profile_N<n>.json.gz`; run 100
  reps of `CREATE TABLE audit_meas.m_tmp (a int)` /
  `DROP TABLE audit_meas.m_tmp` while timing each statement;
  `SIGINT` samply; snapshot `/metrics` again.
* Analyze with `samply load <profile.json.gz>` for the flame graph,
  and diff the before/after metrics scrapes for histogram deltas.

Note that on this host `perf_event_paranoid` had to be lowered to 1
and `perf_event_mlock_kb` raised to 128 MiB before samply could
attach.

### 2026-05-18 — shard-attributed slope study (bogo + file blob)

The previous slope study showed `consensus_cas` count and mean both
growing with N, but the single histogram couldn't tell us *which
shards* the slope came from. This run adds an investigation-only
metric `mz_persist_external_op_latency_by_shard_kind` (HistogramVec,
labels `[op, shard_kind]`) and a small in-process registry mapping
`ShardId -> shard_kind` populated at `Applier::new` time. The
shard_kind classifier is closed-set:

| shard_name (from `Diagnostics`) | shard_kind |
| --- | --- |
| `catalog` | `catalog` |
| `txns` | `txns` |
| `builtin_migration` | `builtin_migration` |
| `expression_cache` | `expression_cache` |
| `storage-usage` / `storage_usage` | `storage_usage` |
| anything else | `user_data` |
| (pre-registration ops) | `unknown` |

No samply this time: the new label is enough to attribute the slope
without flame-graph overhead. Ladder cut to N=5000 / 10000 (15000
dropped) — the per-shard `file://` blob backend filled the host
disk on the longer run.

#### Latency per rep (ms)

| N | create_p50 | create_p95 | drop_p50 | drop_p95 |
| ---: | ---: | ---: | ---: | ---: |
| 5,000 | 29.8 | 73.9 | 28.2 | 35.9 |
| 10,000 | 46.5 | 113.0 | 41.9 | 91.5 |

#### `consensus_cas` by shard_kind, per-DDL

`count/DDL` = total CAS delta during the 100-rep window ÷ 100.
`mean ms` = total CAS time delta ÷ count, so it reflects only ops
that happened during the burst (not lifetime). Numbers are from one
clean run; the absolute mean values shift between runs but the
*shape* is stable.

| op | kind | N=5k count/DDL | N=5k mean ms | N=10k count/DDL | N=10k mean ms |
| --- | --- | ---: | ---: | ---: | ---: |
| consensus_cas | catalog   |  5.58 |  0.53 |  5.58 |  1.85 |
| consensus_cas | txns      |  6.63 |  0.44 |  6.64 |  1.30 |
| consensus_cas | user_data | 43.21 |  6.76 | 69.83 | 38.72 |
| blob_set      | catalog   |  0.57 |  0.99 |  0.56 |  1.39 |
| blob_set      | txns      |  2.62 |  0.96 |  2.63 |  1.18 |
| blob_set      | user_data |  3.77 |  1.11 |  3.86 |  1.06 |

For comparison, the previous (unsharded) numbers from the same
ladder had `consensus_cas count` growing from 57 → 85 per DDL with
mean 4.83 → 17.41 ms. With the label, we can now see *that growth
is almost entirely `user_data`*: those CAS are background work on
the pre-existing user_data shards (compaction, GC, rollup writes),
not synchronous per-DDL work.

#### What we learned

The slope is NOT in user_data shard work. That work is mostly
asynchronous background activity on the 5,000-10,000 pre-existing
shards — high count but doesn't gate DDL completion.

The slope IS in **catalog and txns shard CAS getting more expensive
per-op as those shards' states grow**:

* catalog CAS count/DDL is flat at 5.58 across both scales —
  per-DDL DDL doesn't generate more catalog CAS as N grows.
* catalog CAS mean **grows 3.5× from N=5k to N=10k** (0.53 → 1.85 ms).
* txns CAS count is similarly flat at ~6.6, mean grows 3×
  (0.44 → 1.30 ms).
* The synchronous catalog+txns CAS budget per DDL therefore grew
  from 2.96+2.92 = 5.9 ms at N=5k to 10.3+8.6 = 18.9 ms at N=10k,
  i.e. **+13 ms of synchronous CAS work** out of +16.8 ms total
  wall-time growth. That accounts for almost all the slope.

This points at persist state-apply / rollup cost on the **catalog
shard**, with a parallel — and not actually surprising — slope on
the **txns shard**: every table is registered in the txns shard,
so its state size grows linearly with N. catalog and txns are thus
the same shape (singleton-shard state growing with table count),
not two distinct mysteries.

Neither has anything to do with the user_data shards' state or how
many of them exist; both are about the *size* of two specific
singleton shards' own histories.

#### Next moves

1. **Persist-only microbenchmark.** Build a tiny harness *outside*
   the envd/catalog stack that opens a single persist shard, writes
   into it in a pattern that mimics what the catalog actually does
   per DDL (small diff, occasional rollup, similar key/value
   shapes), grows the shard's state to a target SeqNo / state-byte
   size, then measures `compare_and_set` latency at that size. Run
   the ladder cheaply at multiple sizes. Once that reproduces the
   per-CAS slope outside envd, we can profile / trace / mutate it
   in isolation — no clusterd, no CRDB-TS-oracle, no measurement
   noise from 10k user_data shards doing background work. Likely
   lives in `src/persist-client/src/bin/persist_cas_bench.rs` or a
   sibling of `persist-client/examples/`. Same trick applied
   separately to the txns-shard write pattern (`mz_txn_wal`)
   isolates that slope too.

   **Important: run the microbench against BOTH consensus backends**
   — real CRDB (`postgres://...?options=--search_path=consensus`)
   and bogo-consensus (`bogo://127.0.0.1:6882`). The split tells us
   *where* the per-CAS cost lives:
   * If bogo flattens the slope but CRDB doesn't, the cost is below
     persist — in the consensus impl's per-row / per-state-blob
     handling as the shard's row in `consensus` table grows.
   * If both show the same slope, the cost is *above* the consensus
     trait — in persist's `Applier` / state-apply / diff-fetch /
     rollup-cadence path, and we'd fix it in persist itself.
   The prior end-to-end runs already use bogo, so we know bogo
   shrinks total DDL latency. The microbench is the cleanest way
   to factor whether bogo *changes the slope shape* or just lowers
   the floor.
2. **Once the microbench reproduces the slope**: profile / trace
   the slow CAS to see where time goes inside persist state apply
   — spine slow-path, rollup write cadence, decode cost, encoded
   diff size. The flame-graph from inside envd is too contaminated
   by everything else to read cleanly; the microbench output
   should be much sharper.
3. **Once we know *what* is slow**, fix it — likely some
   combination of more aggressive rollups on the catalog and txns
   shards (`mz_persist_state_apply_spine_slow_path` /
   `mz_persist_shard_seqnos_since_last_rollup` are the relevant
   counters to watch), or a cheaper apply path for the specific
   diff pattern the catalog produces.
4. **De-prioritize**: user_data CAS volume is a non-issue for DDL
   latency. It matters for *total CPU* but not for the wall-time
   slope we've been chasing.

#### Reproducing

Scripts live in `/home/ubuntu/envd-ddl-investigation/` (driver
`bench.py`, analyzer `analyze.py`, launchers `start_envd.sh` /
`reset_state.sh`, cluster-replica JSON). Build with
`cargo build --profile=optimized` — full `--release` triggers an
LTO link that OOMs this 23 GiB VM (the earlier "VM went
unresponsive" was that, plus zero swap). Mitigations applied:

* Added 8 GiB swapfile (`/swapfile`, swappiness=60).
* Cap CRDB container memory: `docker run --memory=2g`.
* Deleted `target/debug` and `target/release` before re-running
  (the `file://` blob backend needs disk room too).
* Bench checkpoints results after each scale; if envd crashes
  mid-padding, the previous scale's data is preserved on disk
  (`results/timings_N{N}.csv`, `metrics/{before,after}_N{N}.prom`).

### 2026-05-18 — persist-only CAS microbench (single-shard ladder)

Built and ran the persist-only CAS microbench described in the
previous section's "Next moves" #1. Binary:
`src/persist-client/examples/persist_cas_bench.rs`. Driver scripts +
collected data live under `/home/ubuntu/envd-ddl-investigation/cas_bench/`.

Each ladder rung opens a *fresh* shard, pre-fills it with `size`
catalog-shaped `compare_and_append`s (one small batch of one row per
iteration), then takes 200 timed `compare_and_append`s at that state
size. Ran the same ladder against:

* `mem` consensus + `mem` blob — control, no I/O.
* `bogo://` consensus + `file://` blob — the same backend our envd
  end-to-end study used.
* `postgres://` (CockroachDB v24.2) + `file://` blob — the real
  production-shape consensus.

Each backend was run twice: once with the production rollup cadence
(`persist_rollup_threshold = 128`) and once with rollups effectively
suppressed (`persist_rollup_threshold = 1_000_000`) so persist state
genuinely accumulates between rungs.

#### Per-CAS latency (p50 ms, after pre-fill)

| backend | rollup | N=0 | N=1k | N=2.5k | N=5k | N=10k | N=20k |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| mem + mem | default | 0.08 | 0.08 | 0.09 | 0.08 | 0.11 | 0.11 |
| bogo + file | default | 0.63 | 0.59 | 0.63 | 0.59 | 0.61 | 0.66 |
| bogo + file | none | 0.59 | 0.57 | 0.58 | 0.62 | 0.59 | — |
| crdb + file | default | 1.10 | 1.10 | 1.06 | 1.09 | 1.24 | 1.39 |
| crdb + file | none | 1.06 | 1.05 | 1.01 | 1.24 | 1.47 | — |

(Full samples in `cas_bench/results.csv`; per-tag table in
`cas_bench/summary.md`.)

#### What we learned

**The single-shard CAS slope is essentially zero — under all three
backends, including with rollups disabled.** Per-CAS p50 grows by at
most ~0.4 ms (CRDB, 0→10k, no rollups) and is *flat* on bogo and mem.

For comparison, the envd end-to-end study at the same scale showed
the `catalog`-shard's `consensus_cas` *mean* growing from 0.53 ms
(N=5k) to 1.85 ms (N=10k) — a +1.3 ms slope per CAS. **This
microbench does not reproduce that slope.** Whatever is making
catalog-shard CASes slow in envd is *not* "the consensus row /
state blob is bigger because we have more SeqNos." It survives
even when state accumulates without rollups.

Specifically:

* **Bogo flat → cost is not in the consensus implementation.** Even
  with rollups disabled, bogo's per-CAS latency does not respond to
  shard state size. If the slope were in the consensus row's
  storage / row-count path, bogo would have shown some movement at
  10k–20k.
* **CRDB shows a tiny ~0.4 ms slope without rollups** which is the
  expected cost of more rows accumulating in the `consensus` table
  for a single shard (the `INSERT ... WHERE (SELECT ... ORDER BY
  sequence_number DESC LIMIT 1)` plan has to skip more
  PK-suffix-suffix rows as truncation falls behind). That's still
  far short of the envd slope.
* **Mem + mem at 0.08 ms** confirms the persist-client overhead
  itself is tiny — the rest is real I/O against the chosen backend.

So the +13 ms catalog+txns slope we saw end-to-end is **not** simply
the cost of doing a CAS against a shard whose history is N seqnos
long. It comes from something the microbench is *not* exercising.

#### Smoking gun in the existing metrics dump

Going back to the envd `/metrics` snapshots from the previous run
and pulling counters the histogram doesn't expose, there's a clear
super-linear signal:

| | N=5k window | N=10k window |
| --- | ---: | ---: |
| `state_apply_spine_flattened` Δ (per 100 reps) | 1,719 | 9,753 |
| per-DDL spine flattens | **17.2** | **97.5** |
| `cmd_cas_mismatch_count` (compare_and_append) | 2 (lifetime) | 2 (lifetime) |
| `shard_seqnos_since_last_rollup{name="catalog"}` | 121 | 30→72 (oscillating, normal) |

Spine flattens per DDL grew **5.7×** as N doubled (17 → 97). Each
flatten is "rebuild the trace's spine from scratch" work that
happens during state apply — CPU on the persist-client side, *not*
the consensus RPC. `state_apply_spine_fast_path` stayed at 0
throughout — every state apply is going through the flattened
(slow-ish) path.

The retries counter (`cmd_cas_mismatch_count`) stayed at 2 across
both windows, so the slope is not from cas retries. The rollup
cadence on the catalog shard is normal (~120 seqnos between
rollups). What's blowing up is state-apply work *around* the CAS,
not the CAS itself.

This reconciles the microbench result. The single-shard microbench
opens one shard, drives one shard's spine — flatten cost per CAS
is tiny because there's only one batch shape per timestamp. envd
has 10k user_data shards' worth of batches threading through the
*same* `Applier::apply_unbatched_cmd` code path on every state
apply, and the per-flatten cost grows with whatever is shared
(shared state cache, shared trace structures, allocator pressure,
or simply scheduling latency). That's why the slope shows up in
the `consensus_cas` *wall-time histogram* even though it isn't
"the CAS RPC got slower" — `MetricsConsensus` wraps the RPC future
with `metrics.consensus.compare_and_set.run_op(...)`, which times
the *whole future* including the time spent waiting on Tokio's
scheduler. Under shared-runtime pressure, that wait time *is* the
slope.

#### Hypotheses for what the microbench is missing

The microbench opens **one** shard and writes to it sustained.
envd at N=10k has:

* ~10,000 user_data shards, all opened in the same `PersistClient`,
  all doing their own background work (GC, snapshot reads, rollup
  writes, compaction posting CAS) concurrently with the catalog
  CAS we're trying to measure.
* A `txns` shard whose state grows linearly with table count (every
  table is registered there), getting CASed on every DDL.
* A shared `StateCache` keyed by ShardId; lookup / update cost
  could grow with shard count.
* A shared `IsolatedRuntime` and a shared gRPC connection pool to
  the consensus server — contention on either could throttle
  catalog CAS specifically.
* A shared `Metrics` (with our new `external_op_latency_by_kind`)
  whose histograms see traffic from every shard.

Any of these could be what makes a *catalog* CAS in envd at N=10k
take 1.85 ms when the same CAS in isolation takes 0.6 ms. The
microbench data rules out "row/state blob size" as the cause; what
remains is shared-resource contention or shared-cache work that
scales with shard count.

#### Next moves (updated)

1. **Multi-shard contention variant.** Extend `persist_cas_bench`
   (or add a sibling) with a `--num-bg-shards N` flag: open N
   background shards on the same `PersistClient` and have each one
   trickle in writes (or just hold open handles to grow the state
   cache), then measure the foreground catalog-shaped shard's CAS
   latency. Same ladder against bogo + CRDB. If this reproduces
   the slope, the bottleneck is shared per-client state (StateCache
   contention, gRPC channel sharing, GC/compaction scheduling).
2. **Run the microbench *while envd is busy*.** Connect a fresh
   `persist_cas_bench` process to the same bogo + same blob dir as
   a running, populated envd and measure foreground CAS. This is
   the closest possible reproduction without splitting the slope
   into "client work" vs "server work."
3. **Inspect persist's internal counters in the envd metrics
   dump** for catalog-shard signals the histogram doesn't see:
   `mz_persist_state_apply_spine_slow_path_count`,
   `mz_persist_shard_seqnos_since_last_rollup`,
   `mz_persist_cmd_cas_mismatch_count` — these would tell us
   whether catalog CAS in envd is bottlenecked on state-apply CPU
   or on retries, not on the consensus RPC.
4. **The original next-step from above remains valid** but is now
   second priority: profiling the bare CAS path won't help if the
   per-CAS slope only shows up under multi-shard load.

#### Reproducing this section

```
# Build (cheap, no LTO):
cargo build -p mz-persist-client --example persist_cas_bench --profile=optimized

# Run a ladder against one backend:
target/optimized/examples/persist_cas_bench \
    --consensus bogo://127.0.0.1:6882 \
    --blob file:///home/ubuntu/envd-ddl-investigation/cas_bench/bogo \
    --sizes 0,500,1000,2500,5000,10000,20000 \
    --measurements 200 \
    --out /home/ubuntu/envd-ddl-investigation/cas_bench/results.csv \
    --tag bogo-file-default-rollup
```

Tag rows in the CSV so multiple runs can share one output file.
Use `--rollup-threshold 1000000` to suppress rollups. The
`summarize.py` script in `cas_bench/` produces the table above.

### 2026-05-18 — fine-grained state_apply attribution, full envd rerun

The microbench from the previous section ruled out "per-CAS scales
with state size" as the cause of the envd slope. To close the loop,
added `mz_persist_state_apply_latency_by_shard_kind` (HistogramVec
with labels `[stage, shard_kind]`, stages `total`/`flatten`/
`unflatten`/`decode`) so each `State::apply_diff` invocation gets
timed and attributed to a shard_kind. Then re-ran the full
envd N=5k/N=10k bench with the new build.

#### End-to-end DDL latency (re-run, same backend, fresh build)

| N | create_p50 | create_p95 | drop_p50 | drop_p95 | create_mean |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 29.27 | 74.77 | 26.71 | 39.55 | 34.05 |
| 10,000 | 47.41 | 120.24 | 42.85 | 48.81 | 61.02 |

Create p50 slope: **+18.14 ms** from N=5k→N=10k. Matches the
previous run within run-to-run variance.

#### `consensus_cas` mean per CAS, per shard_kind

| N | catalog | txns | user_data |
| ---: | ---: | ---: | ---: |
| 5,000 | 0.54 ms | 0.85 ms | 6.45 ms |
| 10,000 | **2.48 ms** | 0.92 ms | **41.41 ms** |

* Catalog CAS RPC: **+1.94 ms × 5.6 calls/DDL = +10.9 ms/DDL**.
* Txns CAS RPC mean is essentially flat (+0.07 ms × 6.65 = +0.5 ms).
* User_data CAS mean blew up 6× (6.45 → 41 ms) — but those are
  off the critical path of any single DDL. Still telling: bogo
  is taking ~40 ms per CAS on average for *background* shards at
  N=10k.

#### `state_apply_latency_by_shard_kind` — the new metric

`total` count is the number of `apply_diff` invocations per
shard_kind in the bench window (×0.01 reps = per-DDL). Mean is
the per-call latency.

| N | shard_kind | total count/DDL | total mean | flatten count/DDL | unflatten count/DDL | decode count/DDL |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 5,000 | catalog | **0.01** | 0.01 ms | 0 | 0 | 0.01 |
| 5,000 | txns | 0 | — | 0 | 0 | 0 |
| 5,000 | user_data | 92.14 | 0.01 ms | 80.64 | 80.64 | 16.20 |
| 10,000 | catalog | **335.70** | 0.01 ms | 332.56 | 332.56 | 1.42 |
| 10,000 | txns | **167.84** | 0.01 ms | 115.72 | 115.72 | 0.06 |
| 10,000 | user_data | 49.25 | 0.01 ms | 35.54 | 35.54 | 30.31 |

**At N=10k the catalog shard does 335 apply_diff calls per DDL,
each ~10 µs.** At N=5k it does ~0. The txns shard makes the same
~0 → 168/DDL jump. Aggregated:

* Catalog state-apply budget: 0 → **3.4 ms/DDL** (NEW slope
  component, completely invisible to the previous "consensus CAS"
  metric).
* Txns state-apply budget: 0 → **1.7 ms/DDL** (also new).

99.9% of apply_diff calls finish in <64 µs (lowest bucket). flatten
is sub-precision; unflatten is the line item that actually adds up.
Decode count is small (~1–2/DDL on catalog, ~30/DDL on user_data)
so the StateDiff::decode cost is not the slope either.

#### Full slope reconciliation

Putting all per-DDL persist budgets side by side:

| component | N=5k ms/DDL | N=10k ms/DDL | Δ ms/DDL |
| --- | ---: | ---: | ---: |
| catalog `consensus_cas` (5.6 × mean) | 3.04 | **13.91** | **+10.87** |
| txns `consensus_cas` (6.65 × mean) | 5.67 | 6.12 | +0.45 |
| catalog `consensus_scan` (2.1 × mean) | 3.28 | 2.79 | −0.49 |
| catalog `blob_set` | 0.79 | 0.65 | −0.14 |
| txns `blob_set` | 2.84 | 3.79 | +0.95 |
| catalog `state_apply` (total) | 0.00 | **3.36** | **+3.36** |
| txns `state_apply` (total) | 0.00 | **1.68** | **+1.68** |
| user_data `state_apply` (total) | 0.92 | 0.49 | −0.43 |
| **sum** | **16.54** | **32.79** | **+16.25** |

Create-table p50 slope: +18.14 ms. Sum-of-persist-pieces slope:
+16.25 ms. **The remaining ~2 ms is run-to-run variance / minor
in-process work** (catalog walks, JSON re-encoding, etc.). The
persist-attributable slope is essentially the whole slope.

#### What we learned

1. **The slope is fully accounted for.** Catalog `consensus_cas`
   RPC growth (+10.9 ms) + catalog state-apply (+3.4 ms) + txns
   state-apply (+1.7 ms) + txns blob_set growth (+0.95 ms) ≈
   the whole +17–18 ms wall-time slope per DDL. No mystery
   residual.
2. **There are two distinct cost growths.** The catalog `consensus_cas`
   RPC itself got 4.6× slower (0.54 → 2.48 ms) — that's a server-
   side cost (bogo + shared client / scheduler contention from 10k
   user_data shards' background activity). And separately, the
   *client*-side state-apply work grew from ~0 to 335 invocations
   per DDL on the catalog shard.
3. **The state-apply count explosion is the more surprising one.**
   The catalog shard's actual `cmd_succeeded` count stayed flat at
   5.6/DDL across both scales. So the catalog's true SeqNo only
   advanced ~5.6 times per DDL. But we called `apply_diff` 335
   times. That means ~60 `apply_diff` invocations per real SeqNo
   advance, each doing little work (~10 µs) but the aggregate
   reaches 3.4 ms/DDL. Something is calling the state-apply path
   far more often than the shard's history requires.
4. **Single-shard microbench can't reproduce this** because it
   doesn't have the 10k user_data shards generating the
   pubsub broadcast / scheduler load that drives the catalog
   state-apply replay frequency up.

#### Hypotheses for the apply_diff count explosion

`apply_diff` only fires for diffs that pass the filter inside
`apply_encoded_diffs` (`x.seqno == state_seqno.next()`), so each
call is at least nominally trying to advance the local cache.
Plausible sources of the 60× per-CAS multiplier on catalog:

* **`cache.rs::push_diff` (pubsub broadcast)** — each call applies
  one diff. The catalog shard's `pubsub_diff_applied` count
  delta is only ~2/window though, so this is *not* the source.
* **`apply.rs::fetch_and_update_state`** (line 669) — calls
  `apply_encoded_diffs(diffs_to_current)`. The
  `update_state_fast_path` counter delta is only ~0.57/DDL.
  Even if each fast_path firing applied ~60 diffs, that gives
  ~34/DDL — short of 335.
* **`state_versions.rs::fetch_current_state`** (line 466) — the
  slow path, replays all live diffs from the latest rollup.
  Would have to fire ~5/DDL on catalog applying ~67 diffs each
  to explain the count. Need to instrument call sites to confirm.
* **`state_versions.rs:1140`** (`StateVersionsIter`) — used by
  GC/audit/inspect to walk historical states. If GC on the
  catalog walks N live states, each walk applies N diffs and
  counts in our metric.

The next surgical metric is to add a call-site label to the
`state_apply_latency_by_shard_kind` histogram (or a separate
counter), so we can split the catalog's 335/DDL by which of the
four call sites is responsible. That's a one-line dispatch per
site and falls out into the same bench harness.

#### Next moves

1. **Add a `source` label to the state-apply metric** (or a
   separate counter `apply_diff_calls_by_source_and_kind` with
   sources `cas_update`, `slow_refetch`, `pubsub_push`,
   `state_iter`). Rerun the bench. Pinpoint the call site that
   creates the 335/DDL on catalog.
2. **Investigate why catalog `consensus_cas` mean grew 4.6×** even
   though the microbench shows no growth in bogo RPC at the same
   state size. The likely answer is shared-client / scheduler
   contention — many user_data shards' CASes queued up in front of
   the catalog CAS on the bogo gRPC connection (the pool was
   bumped to 50 channels recently, but at N=10k there may still be
   head-of-line blocking). To confirm, instrument
   `MetricsConsensus::run_op` with an *outer* timer that wraps the
   future before Tokio scheduling, and compare against the inner
   timer that wraps just the inner consensus call.
3. **Now that the slope is fully accounted for**, decide which of
   the two pieces to fix first. The catalog `consensus_cas`
   growth (+10.9 ms) is the bigger lever (2× larger than apply_diff
   growth). It's likely also easier to fix — multi-channel fan-out
   per shard, or a dedicated per-shard concurrency limit, would
   stop the user_data CAS load from queuing in front of catalog
   CASes.

#### Reproducing

* New build: `cargo build --profile=optimized --bin environmentd --bin clusterd --bin mz-bogo-consensus`
* `/home/ubuntu/envd-ddl-investigation/reset_state.sh` to wipe
  blob / mzdata / scratch / CRDB.
* `/home/ubuntu/envd-ddl-investigation/start_envd.sh` to launch.
* `/home/ubuntu/envd-ddl-investigation/bench.py` runs the full
  N=5k → N=10k ladder; ~10 minutes wall.
* `/home/ubuntu/envd-ddl-investigation/analyze.py` parses the
  before/after `/metrics` snapshots and prints the per-shard_kind
  breakdown tables above.

### 2026-05-18 — apply_diff source attribution: it's GC

Added a per-call-site `source` label to the apply_diff path
(`mz_persist_state_apply_calls_by_source_shard_kind` with
`[source, shard_kind]`). Four runtime sources are now distinguishable:

* `cas_update` — `apply.rs::Applier::fetch_and_update_state` fast path.
* `slow_refetch` — `state_versions.rs::fetch_current_state` full
  rollup+replay (the fast-path-fallback "we got fenced too far").
* `pubsub_push` — `cache.rs::push_diff` from PubSub broadcasts.
* `state_iter` — `state_versions.rs::StateVersionsIter::next` walks
  (used by GC + storage-usage audit + admin inspect).

#### Three runs, one consistent story

Ran the bench three times to characterize variance: run 1 (clean
reset, fresh envd), run 2 (re-measured on the same envd without
reset — bench `pad_to` is idempotent so this re-measures at N=10k
twice, kept as a contrast point), run 3 (clean reset again).

| run | reset? | catalog apply_diff/DDL at N=5k | catalog apply_diff/DDL at N=10k |
| --- | --- | ---: | ---: |
| 1 | yes | 0 | 0.01 |
| 2 | no (warm) | 95.19 | 88.98 |
| 3 | yes | 0.02 | 172.29 |

And the per-source breakdown for run 3 N=10k catalog:

| source | count/DDL |
| --- | ---: |
| **`state_iter`** | **172.29** |
| `slow_refetch` | 0.71 |
| `cas_update` | 0.01 |
| `pubsub_push` | 0 |

Run 2 N=5k catalog (warm-envd contrast):

| source | count/DDL |
| --- | ---: |
| **`state_iter`** | **94.03** |
| `slow_refetch` | 1.14 |
| `pubsub_push` | 0.01 |
| `cas_update` | 0.01 |

In every case where catalog apply_diff exists, **>99% of it is
`state_iter`**. The original mystery is solved: it's
`StateVersionsIter::next`, the per-diff walker used by GC.

#### The GC fingerprint confirms it

Run 3 N=10k window deltas on the catalog shard:

| counter | before | after | Δ |
| --- | ---: | ---: | ---: |
| `shard_gc_finished{name="catalog"}` | 9 | 10 | **+1** |
| `shard_gc_live_diffs{name="catalog"}` | 19,676 | 17,229 | (gauge) |
| `shard_cmd_succeeded{name="catalog"}` | 33,661 | 34,221 | +560 |
| `shard_seqnos_since_last_rollup{name="catalog"}` | 24 | 69 | (gauge) |

**Exactly one GC fired on the catalog during the 100-rep N=10k
window**, and that one GC walked **17,229 live diffs** (the
`gc_live_diffs` gauge after that GC). The `state_iter` counter
delta is 17,229 — the same number. One GC = one `fetch_all_live_states`
= 17,229 `StateVersionsIter::next` calls, each one an `apply_diff`
on the catalog shard.

For N=5k, GC didn't fire on the catalog during the measurement
window (`gc_finished` delta 0), so `state_iter` was 0. For run 1
N=10k, GC also happened not to fire during the window. For run 2
both windows happened to coincide with GC firings.

#### What this means for the slope

Per-call work is ~10 µs. **17,229 calls × 10 µs ≈ 172 ms** total
GC work on the catalog over the 100-rep window. That's 1.72 ms of
catalog GC work *per DDL of wall time*, but it runs on background
tasks, so its contribution to *DDL-critical-path latency* is at
most the Tokio-scheduler tax (single-digit %).

Slope decomposition for run 3 (clean reset):

| component | N=5k ms/DDL | N=10k ms/DDL | Δ ms/DDL |
| --- | ---: | ---: | ---: |
| catalog `consensus_cas` × 5.6 | 3.04 | **10.28** | **+7.24** |
| txns `consensus_cas` × 6.64 | 3.31 | 7.36 | +4.05 |
| catalog `consensus_scan` × 2.08 | 1.10 | 2.43 | +1.33 |
| txns `blob_set` × 2.63 | 2.66 | 3.51 | +0.85 |
| catalog `state_apply` (GC, all `state_iter`) | 0.00 | 1.72 | +1.72 |
| **sum** | **10.11** | **25.30** | **+15.19** |

create_p50: 31.26 → 45.45 ms = **+14.19 ms**. Sum-of-persist-pieces:
+15.19 ms. Within noise. The slope is dominated by the CAS RPC
times growing on catalog (+7.2 ms) and txns (+4.0 ms), with GC
state-walk overhead a distant third (+1.7 ms).

#### Run-to-run variance is high

Comparing the three runs side-by-side, the *split* of the slope
across components changes a lot, even though the *total* slope is
consistently +14–18 ms:

| run | catalog CAS Δ ms | txns CAS Δ ms | catalog state_apply Δ ms |
| --- | ---: | ---: | ---: |
| 1 | +10.87 | +0.45 | +0.00 |
| 3 | +7.24 | +4.05 | +1.72 |

The previously reported "catalog state_apply slope of +3.4 ms" was
from run 1's *previous build* — a third run we can't compare
apples-to-apples to. The reproducible story is:

1. **GC's `state_iter` walks ARE the source of all catalog/txns
   apply_diff calls.** PubSub, cas_update, and slow_refetch
   together contribute <1% of catalog calls in every run.
2. **Whether catalog state-apply appears as a slope component
   depends on whether GC fires during the measurement window.**
   When it fires, it walks all live diffs (17 k+ at N=10k) but the
   work is cheap (~10 µs/call). Total contribution: 1–4 ms/DDL,
   third-tier behind the two CAS RPC slopes.
3. **The dominant slope is catalog `consensus_cas` RPC time** —
   +7–11 ms/DDL across runs. Second is txns CAS at +0–4 ms.
   Together those are 70-90% of the DDL slope every time.
4. **The deeper question is why catalog CAS RPC itself slows
   down.** The single-shard microbench (previous section) rules
   out "consensus row gets bigger as state grows." The most
   plausible remaining cause is shared-runtime / gRPC-pool
   contention from the 10k user_data shards' background CAS
   load (user_data `consensus_cas` mean was 35–44 ms at N=10k vs
   7 ms at N=5k — a 5× growth that head-of-line blocks every
   other CAS on the same bogo gRPC connection pool).

#### Next moves

1. **Confirm the bogo head-of-line hypothesis** with an outer-vs-
   inner timer split around `MetricsConsensus::run_op`: the outer
   timer captures Tokio-scheduler + connection-acquire wait; the
   inner timer captures only the RPC wire time. Slope in
   outer-minus-inner = contention.
2. **The GC-walks-17k-diffs pattern is itself a backlog signal.**
   With one envd running for ~30 minutes, GC fires every ~10
   minutes and finds a big backlog (17 k diffs). If GC ran more
   often it'd find smaller backlogs each time. Worth checking
   whether GC scheduling on the catalog shard scales appropriately
   with command rate, or whether something throttles it.
3. **Source attribution is solved.** No follow-up metric needed
   for this question.

## 2026-05-18 — bogo's `update_state_metrics` was eating all the CAS slope

### tl;dr

The "catalog `consensus_cas` RPC mean grows 2-3× at N=10k" slope was a
bench artifact, not a Materialize issue. The bogo-consensus server's
`update_state_metrics` was iterating every shard's `Vec.len()` inside
the mutex on every CAS to recompute `versions_total`. At 10k shards
that's a ~100 µs O(N) hold on the lock that serializes every operation.
Removing the per-call iteration (incremental counters instead) makes
catalog CAS mean go FLAT across N=5k → N=10k.

### Step 1: split outer (`MetricsConsensus::run_op`) from inner (gRPC wire)

Added `mz_persist_consensus_wire_seconds_by_shard_kind`, recorded
inside `BogoConsensus` around `self.client.compare_and_set(...)`. Same
axes (op + shard_kind) and buckets as the existing
`external_op_latency_by_shard_kind`, so subtraction is meaningful.

**Run before any other fix, N=5k → N=10k:**

| layer | N=5k mean ms | N=10k mean ms | mean Δ ms |
| --- | ---: | ---: | ---: |
| catalog external_op (post-spawn, around run_op) | 0.77 | 1.79 | +1.02 |
| catalog wire (inside BogoConsensus around gRPC) | 0.77 | 1.79 | +1.02 |
| user_data external_op | 7.26 | 35.10 | +27.84 |
| user_data wire | 6.68 | 34.95 | +28.27 |

**Outer === inner** to within sampling noise. The post-spawn wrapper
(`run_op` counter incs + the bogo adapter's status_to_external map) is
free. **The CAS slope is in the gRPC call itself.** This rules out
spawn-side overhead and any wrapping overhead inside MetricsConsensus.

### Step 2: scrape the bogo server's `rpc_seconds` and notice it grows

Enabled `--metrics-listen-addr` on the bogo binary and added a scrape
in `bench.py`. Server-side `mz_bogo_consensus_rpc_seconds` for
`compare_and_set` (aggregated across all shard kinds — the server
doesn't have a shard_kind classifier):

| N | server compare_and_set mean ms | client wire user_data mean ms |
| --- | ---: | ---: |
| 5k | 0.60 | 6.68 |
| 10k | 2.15 | 34.95 |

Server mean grew 3.6×. The bogo server holds a single
`std::sync::Mutex` around its `BTreeMap` for every op. Suspicious.

### Step 3: read the server and find the smoking gun

`src/bogo-consensus/src/server.rs::update_state_metrics`, called from
every `compare_and_set` (both Committed and ExpectationMismatch paths)
and every `truncate`:

```rust
fn update_state_metrics(&self, store: &BTreeMap<String, Vec<VersionedData>>) {
    let shards = i64::try_from(store.len()).unwrap_or(i64::MAX);
    let versions: i64 = store.values()
        .map(|v| i64::try_from(v.len()).unwrap_or(i64::MAX))
        .sum();
    self.metrics.shards_total.set(shards);
    self.metrics.versions_total.set(versions);
}
```

That `store.values().map(...).sum()` is **O(num_shards) under the
mutex on every CAS**. At N=10k with ~100 concurrent CAS in flight from
the 10k user_data shards' background work, the mutex queue depth
grows. Catalog CAS waits behind it.

### Step 4: replace with incremental counters and rerun

Fix: `bump_state_gauges(shards_delta, versions_delta)` called *after*
dropping the mutex. CAS that creates a new key bumps `shards` by 1;
every successful CAS bumps `versions` by 1; truncate decrements
`versions` by the count it removed. Constant time per call, no
iteration.

**Same bench, fixed bogo:**

| | N=5k mean | N=10k mean | mean Δ |
| --- | ---: | ---: | ---: |
| catalog wire mean ms | 0.27 | 0.29 | +0.02 |
| txns wire mean ms | 0.42 | 0.27 | -0.15 |
| user_data wire mean ms | 0.93 | 0.92 | -0.01 |
| server `compare_and_set` mean ms | 0.00 | 0.01 | +0.01 |
| create_p50 ms | 26.86 | 41.17 | +14.31 |

Catalog and user_data CAS means are now **flat** across the scale
jump. The previous +28 ms/call user_data slope was 100% the bogo
metric-update O(N) artifact. The catalog +1 ms/call slope was the same
artifact contending on the shared mutex.

### What remains of the create_p50 slope (+14.31 ms/DDL)

With CAS basically free, the per-DDL slope decomposes to:

- catalog state_apply (this run GC walked 20k catalog diffs): +2.02 ms
- catalog `consensus_scan`: +0.82 ms (mean 0.45 → 0.84 ms × 2.08/DDL)
- catalog blob_set: +0.06 ms
- catalog `consensus_cas`: +0.12 ms
- txns `consensus_cas`: -1.0 ms (decreased)
- **Sum of measured CAS+blob+scan+apply slope: ~+2 ms/DDL**
- `catalog_transact_seconds` slope: +9.87 ms/DDL
- create_p50 slope: +14.31 ms/DDL

So `catalog_transact_seconds` itself has a +9.87 ms slope but only ~+2 ms
of that comes from persist external ops we instrument. The other +8 ms
is inside the catalog transact path between persist calls
(catalog state munging, builtin migration checks, etc.). That's the
next layer to investigate if we want to keep peeling.

The +4.4 ms gap between catalog_transact slope (+9.87) and create_p50
slope (+14.31) is outside catalog_transact — in adapter coordination
or driver-side.

### Takeaways

1. **The "catalog consensus_cas grows with N" headline was a bogo
   artifact, not a Materialize finding.** Bogo's per-CAS work was O(N)
   in the number of shards because of an in-mutex metric update.
2. **Wire == outer.** No measurable overhead inside
   MetricsConsensus's `run_op` wrapper or the bogo adapter — the
   slope was always in the actual gRPC call.
3. **With the fix, bogo is a much better CRDB proxy.** Per-call mean
   stays flat from N=5k to N=10k for all shard kinds. The remaining
   DDL-level slope is in adapter/catalog code paths, not persist.
4. **Bench correctness lesson:** anything that proxies a production
   service for perf work needs to itself be O(1) in the dimension
   being scaled. `update_state_metrics` looked innocent but actively
   distorted every comparison since the bogo work started.

## 2026-05-18 — CRDB-backed sanity check at N=5k/10k/15k

### tl;dr

Re-ran the bench against CRDB consensus (same machine, same envd
binary, just `--persist-consensus-url=postgres://…/consensus`) at
three scale points. The **Materialize-side slope reproduces on
CRDB**: ~+15 ms/+5k tables on create p50, basically the same shape
we see on post-fix bogo. CRDB adds a modest extra ~2-5 ms/+5k on top
because its catalog CAS RPC mean grows mildly with state size
(1.88 → 2.11 → 3.80 ms across 5k → 10k → 15k); bogo's was flat.

So the post-fix bogo conclusion holds: the dominant slope is in
adapter/catalog code, not persist. Switching backends doesn't move
that slope.

### Bench setup

- `start_envd_crdb.sh` — same as the bogo flavour but with
  `--persist-consensus-url=postgres://root@localhost:26257/materialize?options=--search_path=consensus`.
- `bench.py` + `analyze.py` take a `BENCH_MODE` env var that
  suffixes `metrics_<mode>/` and `results_<mode>/`. Bogo data lives
  in `metrics_bogo/` / `results_bogo/`; CRDB data in
  `metrics_crdb/` / `results_crdb/`.
- 100 reps × CREATE+DROP at each scale point. Padding is
  incremental (5k → 10k → 15k = 15k total CREATE TABLEs).
- Resource ceiling held throughout: envd RSS topped at 2.9 GiB at
  N=15k; CRDB container stayed under 1 GiB. No memory pressure.

### Headline: create_table p50 by backend

| N       | bogo p50 | CRDB p50 | Δ (CRDB-bogo) |
|---------|---------:|---------:|--------------:|
| 5 000   |    26.86 |    54.82 |        +27.96 |
| 10 000  |    41.17 |    72.24 |        +31.07 |
| 15 000  |      —   |    87.47 |          —    |

**Slope per +5k tables:**

- bogo: +14.31 ms (5k → 10k)
- CRDB: +17.42 ms (5k → 10k), +15.23 ms (10k → 15k)

The Materialize-side slope (the part bogo is also paying) is ~14
ms/+5k. CRDB adds ~2-3 ms on top of that.

CRDB sits ~28 ms above bogo at every scale — that's a flat
"CRDB tax" from the actual consensus RPCs being ~2 ms each instead
of <0.5 ms. ~5.6 catalog CAS + 6.6 txns CAS = 12 RPCs/DDL × (2 ms -
0.3 ms) ≈ +20 ms; close enough to the +28 we observe.

### CAS per-call means: bogo stays flat, CRDB drifts

`mz_persist_external_op_latency_by_shard_kind` mean for `consensus_cas`:

|        | bogo 5k | bogo 10k | CRDB 5k | CRDB 10k | CRDB 15k |
|--------|--------:|---------:|--------:|---------:|---------:|
| catalog|  0.27   |  0.29    |  1.88   |  2.11    |  3.80    |
| txns   |  0.42   |  0.27    |  2.02   |  2.68    |  2.27    |
| user_data| 0.93  |  0.92    |  8.28   | 33.24    | 79.57    |

- bogo: catalog/txns CAS mean is flat across scales. This was the
  whole point of the `update_state_metrics` fix.
- CRDB catalog: grows ~2× from 5k to 15k. Counts are unchanged
  (5.6 per DDL), so this is per-RPC slowdown — CRDB is doing more
  work per CAS as the consensus table grows. Plausibly index
  size, query plan, or just SQL parsing/round-trip overhead under
  load.
- CRDB user_data: the big numbers are dominated by background
  compaction load (count 49 → 124 per DDL); ignore for the
  create-path discussion.

### catalog_transact_seconds tracks create p50

CRDB `catalog_transact_with_ddl_transaction` mean:

| N       | mean (ms) | slope per +5k |
|---------|----------:|--------------:|
| 5 000   |     53.38 |       —       |
| 10 000  |     70.15 |     +16.77    |
| 15 000  |     89.67 |     +19.52    |

So roughly the entire create_p50 slope is inside `catalog_transact`
on CRDB too — same conclusion as bogo.

### Padding throughput tells the same story

CREATE TABLE rate during the pad phase, end of each segment:

- N=5k:  19.1 tbl/s
- N=10k: 15.6 tbl/s
- N=15k: 11.9 tbl/s

Roughly 1/p50: a 5k-table-rich envd is doing ~52 ms/CREATE during
padding vs ~85 ms at 15k. Same slope.

### Takeaways

1. **The Materialize-side scaling slope is real and backend-
   independent.** Going from bogo to CRDB doesn't make it go away;
   CRDB just shifts the absolute floor up and adds a mild extra
   per-CAS cost.
2. **CRDB has its own mild CAS-mean slope** (catalog 1.88 → 3.80 ms
   across 5k → 15k). Probably worth a follow-up to confirm whether
   that's the consensus table index growth or SQL-side, but it's a
   secondary effect at these scales.
3. **The bogo work was the right setup.** Now that its in-mutex
   metric update is fixed, bogo CAS mean is flat across the range,
   so anything bogo still shows as slope is genuinely Materialize-
   side. The CRDB run confirms that the bogo slope reproduces on a
   real backend.
4. **15k is well within budget on this machine** — envd hit 2.9
   GiB RSS, CRDB stayed under 1 GiB. We can keep going if needed.

## 2026-05-18 — Splitting `catalog_transact` into phases

tl;dr: the `catalog_transact_with_ddl_transaction` slope is **mostly
outside** `Catalog::transact`. The inside-transact slope is real but
modest (~3-5 ms/+5k). The outside-transact slope is ~6-10 ms/+5k and
lives somewhere in `Coordinator::catalog_transact_inner` — the
wrapper that does `Arc::make_mut(catalog)`, calls
`catalog.transact`, then ships builtin-table updates and runs the
finalize block.

### What we added

A new histogram, `mz_catalog_transact_phase_seconds{phase=...}`,
that times each phase inside `Catalog::transact`:

* `transact_inner` — total time inside the inner method (a
  cross-check / super-timer for the four phases below).
* `op_loop` — the for-each-op loop body (`transact_op` + per-op
  `preliminary_state.apply_updates`).
* `final_apply_updates` — the combined `apply_updates` call on the
  final state, after the op loop.
* `prepare_state` — `storage_collections.prepare_state(...)`
  (storage controller side).
* `post_prepare_apply_updates` — the second final `apply_updates`
  after `prepare_state`, draining any new tx updates that emerged.
* `tx_commit` — `tx.commit(&mut **storage, oracle_write_ts)`
  (the persist CAS path).
* `assign_state` — `self.state = new_state` (drops old `CatalogState`).

Wired through `Catalog` as `Option<HistogramVec>`, set once from
`Coordinator` startup. `transact_incremental_dry_run` doesn't get
the metric — DDL-txn dry runs are a different code path and
polluting the measurement bucket would muddy the bench.

### Headline timings (bogo backend, fresh-from-scratch)

`mz_catalog_transact_seconds{method="catalog_transact_with_ddl_transaction"}`
mean (ms, per DDL = one CREATE *or* one DROP, 200 obs/scale):

| N       | mean (ms) | slope per +5k |
|---------|----------:|--------------:|
| 5 000   |     33.80 |       —       |
| 10 000  |     42.81 |     +9.01     |
| 15 000  |     57.81 |    +15.00     |

Create p50 (CREATE side only) tracks: 34.31, 43.91, 60.37.

### Phase split — mean per single DDL

`mz_catalog_transact_phase_seconds`, mean over 200 observations/scale:

| phase                       | 5k ms | 10k ms | 15k ms | Δ 5→10 | Δ 10→15 |
|-----------------------------|------:|-------:|-------:|-------:|--------:|
| transact_inner (total)      |  2.04 |   3.14 |   5.79 |  +1.10 |   +2.64 |
|  ↳ op_loop                  |  1.07 |   1.53 |   2.19 |  +0.46 |   +0.66 |
|  ↳ final_apply_updates      |  0.51 |   0.72 |   1.04 |  +0.21 |   +0.32 |
|  ↳ prepare_state            |  0.04 |   0.19 |   1.41 |  +0.15 |   +1.22 |
|  ↳ post_prepare_apply_upd.  |  0.17 |   0.29 |   0.45 |  +0.12 |   +0.16 |
| tx_commit                   |  2.47 |   3.79 |   5.51 |  +1.32 |   +1.72 |
| assign_state                |  0.34 |   0.62 |   0.99 |  +0.28 |   +0.37 |
| **inside-transact sum**     |  4.85 |   7.55 |  12.29 |  +2.70 |   +4.74 |
| outside-transact remainder  | 28.95 |  35.26 |  45.52 |  +6.31 |  +10.26 |
| `catalog_transact_with_ddl` | 33.80 |  42.81 |  57.81 |  +9.01 |  +15.00 |

(Children of `transact_inner` sum to ~80-90% of the parent; the gap
is small per-phase Cow setup, lock acquisition, mode match — not
worth its own metric.)

### Takeaways

1. **The dominant slope is outside `Catalog::transact`.** Of the
   +9 ms/+5k jump from N=5k→10k, only +2.7 ms is in the timed
   phases; +6.3 ms is in the Coordinator wrapper layer. At
   10k→15k it gets worse: +4.74 inside, +10.26 outside. The
   "+8 ms unattributed" that motivated this iteration is the
   *outside* component, not something hidden inside `transact_inner`.
2. **`tx_commit` is the biggest inside-transact slope component**
   (~half of the inside-transact rise). The catalog CAS RPC mean
   is flat (we fixed bogo's update_state_metrics earlier), so
   tx_commit's growth has to be in serialization, batching, or
   the txns/user_data CAS work that runs synchronously inside
   `tx.commit`.
3. **`prepare_state` has a hockey-stick at 15k** — 0.04 → 0.19 →
   1.41 ms. The storage_controller's `prepare_state` does
   per-collection bookkeeping; at 15k user collections, something
   in there is starting to bite. Worth a dedicated look.
4. **`op_loop` and `final_apply_updates` grow modestly** — both
   accumulate cost from in-memory state-diff application. This
   matches our earlier finding that catalog state-apply does
   ~335 invocations per DDL.
5. **`assign_state` grows linearly** — 0.34 → 0.62 → 0.99 ms.
   This is dropping the old `CatalogState`; the cost is proportional
   to state size. Cheap per-DDL but not zero.

### Wrapper-layer suspects (outside `Catalog::transact`)

`Coordinator::catalog_transact_inner` does, in order:

* Pre-walk ops to classify them (cheap).
* `validate_resource_limits(&ops, ...)` — O(ops).
* `Arc::make_mut(catalog)` — **if any other holder of the catalog
  Arc exists, this clones the entire `Catalog` (≈ full
  `CatalogState` clone).** Highly suspect — would scale linearly
  with N. Catalog Arcs are held by every active session for catalog
  snapshots, so under any concurrent activity this can fire.
* `catalog.transact(...)` — the part we now have phase metrics for.
* `cluster_replica_statuses` updates (no per-table loop, cheap).
* `builtin_table_update().execute(builtin_table_updates)` — writes
  rows into mz_objects, mz_tables, etc. Scales with the number of
  builtin tables touched by the DDL, which grows with N via the
  derived/dependent rows.
* The finalize block (configs, replanning) — only fires for
  system-config ops, not bare CREATE/DROP TABLE.
* Segment audit-log dispatch (no-op in this bench).

Then the outer wrappers `catalog_transact_with_side_effects` /
`catalog_transact_with_ddl_transaction` add
`apply_catalog_implications` (controller side effects) and the
side-effects-fut join.

The biggest two on-paper suspects are:
* `Arc::make_mut(catalog)` cloning the catalog on every DDL. Need
  to confirm there's a second Arc holder during a typical CREATE
  TABLE.
* `builtin_table_update().execute(...)` writing per-object rows;
  the table row count grows ~linearly with N.

### Where we'd go next

Add a second phase histogram around the **outside** layer:

* `coord_pre_transact` — from method entry to `catalog.transact()`.
* `coord_arc_make_mut` — wrap just the `Arc::make_mut(catalog)` call.
* `coord_post_transact` — from `catalog.transact()` end through
  builtin-table execute.
* `coord_finalize` — finalize block.
* `coord_apply_implications` — outer wrapper's
  `apply_catalog_implications` call.

That should split the +6-10 ms/+5k outside slope into named pieces.
Also worth a peek at `prepare_state` in `storage_controller` to
explain the 15k hockey-stick.

## 2026-05-18 — Wrapper-layer phase split: `builtin_table_update().execute` is the slope owner

tl;dr: outside-transact slope is almost entirely
`builtin_table_update().execute()`. At N=15k it's 16.85 ms/DDL —
nearly half the total DDL latency — and it grows ~4.3 ms per +5k
tables. `Arc::make_mut(catalog)` and the `finalize` block are
essentially free; both are ruled out.

### What we added

Six new `mz_catalog_transact_phase_seconds{phase=...}` labels for
`Coordinator::catalog_transact_inner` (the wrapper layer):

* `coord_inner_total` — entire method (cross-check super-timer).
* `coord_pre_transact` — entry → just before `catalog.transact()`
  (op pre-walk, validate_resource_limits, get_local_write_ts,
  Arc::make_mut).
* `coord_arc_make_mut` — wraps just `Arc::make_mut(catalog)` to
  isolate the Catalog-clone-if-shared cost.
* `coord_post_transact` — just after `catalog.transact()` →
  method return (cluster_replica_statuses, builtin_table_execute,
  finalize, audit).
* `coord_builtin_table_execute` — wraps just
  `self.builtin_table_update().execute(builtin_table_updates).await`.
* `coord_finalize` — the bool-gated finalize block (config updates,
  webhook restarts, advance_timelines refresh).

### Headline timings (bogo backend, fresh-from-scratch)

`mz_catalog_transact_seconds{method="catalog_transact_with_ddl_transaction"}`:

| N       | mean (ms) | slope/+5k |
|---------|----------:|----------:|
| 5 000   |     36.45 |       —   |
| 10 000  |     44.72 |   +8.27   |
| 15 000  |     58.92 |  +14.20   |

create p50: 35.85 → 46.07 → 62.25 (tracks the same slope).

### Phase split — mean per single DDL

| phase                          | 5k    | 10k   | 15k   | Δ 5→10 | Δ 10→15 |
|--------------------------------|------:|------:|------:|-------:|--------:|
| coord_inner_total              | 17.32 | 25.11 | 34.06 |  +7.79 |   +8.95 |
|  ↳ coord_pre_transact          |  3.24 |  3.23 |  3.51 |  -0.01 |   +0.28 |
|  ↳ coord_arc_make_mut          |  0.00 |  0.00 |  0.00 |    0   |    0    |
|  ↳ Catalog::transact (sum)*    |  4.97 |  8.02 | 11.93 |  +3.05 |   +3.91 |
|  ↳ coord_post_transact         |  8.27 | 12.41 | 16.86 |  +4.14 |   +4.45 |
|     ↳ coord_builtin_table_exec |  8.26 | 12.40 | 16.85 |  +4.14 |   +4.45 |
|     ↳ coord_finalize           |  0.00 |  0.00 |  0.00 |    0   |    0    |
| apply_catalog_implications     | 11.39 | 11.16 | 13.51 |  -0.23 |   +2.35 |
| `catalog_transact_with_ddl`    | 36.45 | 44.72 | 58.92 |  +8.27 |  +14.20 |

(*) Catalog::transact = transact_inner + tx_commit + assign_state
(plus a small per-stage gap), per the previous phase split.

### What this tells us

1. **`builtin_table_update().execute()` is the single biggest
   slope component on the outside layer.** It contributes +4.14
   and +4.45 ms per +5k tables — essentially *half* of the entire
   per-DDL slope on its own. At N=15k it's 16.85 ms, ~29% of the
   58.92 ms total per-DDL latency.
2. **`Arc::make_mut(catalog)` is essentially zero** at all scales.
   The Catalog Arc is uniquely held while we're inside
   `catalog_transact_inner`, so the make_mut hot path doesn't
   trigger a clone. Original hypothesis ruled out.
3. **`coord_pre_transact` is flat** (~3.2 ms regardless of N).
   The op pre-walk + resource-limit validation + write-ts grab
   don't scale with N. Good — we can ignore these.
4. **`coord_finalize` is ≈ 0** for plain CREATE/DROP TABLE.
   The bool-gated config/tracing/etc. updates only fire for
   system-config ops. Not a suspect.
5. **`apply_catalog_implications` is mostly flat** — 11.4, 11.2,
   13.5 ms across scales. It's *big* (≈ 1/4 of the per-DDL total)
   but doesn't carry the slope.

So the slope budget at 10k→15k splits roughly:
* `coord_builtin_table_execute`: +4.45 ms
* `Catalog::transact` (tx_commit + transact_inner + assign_state): +3.91 ms
* `apply_catalog_implications`: +2.35 ms
* everything else (`coord_pre_transact` drift, gap): +3.49 ms

`coord_inner_total` minus its named children leaves a ~0.84 ms
(5k) → 1.45 ms (10k) → 1.76 ms (15k) gap — that's
cluster_replica_statuses updates + segment audit + setup overhead.
Cheap per-DDL but not flat. Probably not worth chasing yet.

### Inside `builtin_table_update().execute()`

Reading `src/adapter/src/coord/appends.rs::execute`, the call is:

```rust
self.coord.pending_writes.push(PendingWriteTxn::System { updates, ... });
let write_ts = self.coord.group_commit(None).await;
self.coord.advance_timelines_interval.reset();
```

So the time is **`Coordinator::group_commit(None).await`**. That's
where pending_writes get flushed to persist as table appends. The
size of `builtin_table_updates` per DDL is small (one or two rows
per builtin system table touched), so the growth has to be inside
`group_commit` itself — likely from iterating something that
scales with the number of tables (table advancement, upper bumps,
collection bookkeeping).

### Where we'd go next

1. **Instrument inside `group_commit`** — split the upper-advancement,
   table-append, and bookkeeping phases. We've already got a metric
   `mz_group_commit_table_advancement_seconds`; pair it with one
   for the per-DDL append cost.
2. **Look at `prepare_state` 15k hockey-stick** (0.04 → 0.19 →
   1.41 ms from the previous run). That's storage_controller side,
   not coord.
3. **Catalog::transact internal slope** (+3-4 ms/+5k) is still
   non-trivial. Mostly tx_commit and transact_inner growth from
   prior iterations — not on the critical path of "where does
   the headline slope live," but a real number.
