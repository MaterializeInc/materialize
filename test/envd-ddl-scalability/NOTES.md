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
