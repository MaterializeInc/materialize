# Catalog transactions in O(delta) instead of O(catalog size)

- Associated: branch `envd-ddl-scalability`; audit harness and findings
  in `test/envd-ddl-scalability/`.

## The Problem

DDL latency in Materialize grows with the number of catalog objects.
On the `envd-ddl-scalability` audit harness (warm local envd, post-fix
state), padding the catalog with 5000 trivial tables raises p50
single-statement DDL latency by 17-31 ms versus an empty catalog:

| op | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `CREATE TABLE` | 30.0 ms | 47.3 ms | +17.3 |
| `DROP TABLE` | 18.6 ms | 42.6 ms | +24.0 |
| `ALTER TABLE … ADD COLUMN` | 22.3 ms | 53.0 ms | +30.7 |
| `RENAME TABLE` | 17.4 ms | 43.6 ms | +26.2 |

The slope is ~3-6 μs/object and is shared by every DDL op — including
RENAME and CREATE VIEW, which do no controller-side work. That tells
us the cost is in the catalog/coordinator infrastructure paid by every
DDL, not in any op-specific path.

When padding objects share dependencies (e.g. many MVs reading the
same base table), the cost goes super-linear and dominates everything
else — see `test/envd-ddl-scalability/NOTES.md` "Storage-side blow-up"
for that workstream. This document does not address the storage-side
cost.

### Where the time goes

Trace ranking (post-fix envd, N=0 → N=5000, top growers):

| span | N=0 | N=5000 | Δ |
| --- | ---: | ---: | ---: |
| `snapshot` (catalog durable) | 0.5 ms | 4.1 ms | +3.6 |
| `transaction` (catalog durable) | 0.4 ms | 3.0 ms | +2.6 |
| `consolidate` (catalog durable) | 0.4 ms | 2.1 ms | +1.7 |
| `PersistTableWriteCmd::Append` (catalog shard) | 5.0 ms | 7.6 ms | +2.6 |
| `apply_catalog_implications_inner` | <0.5 ms | 1.2 ms | +1.2 |
| `apply_updates` (in-memory) | 0.5 ms | 1.4 ms | +0.9 |

The first three add ~8 ms per DDL at N=5000 ≈ **1.5 μs/object**, the
single largest source of the slope. Their cost is structural:

- `with_snapshot` (`src/catalog/src/durable/persist.rs:766`) walks the
  consolidated trace and constructs a fresh
  `Snapshot { databases: BTreeMap, schemas: BTreeMap, items: BTreeMap, … }`
  per transaction. For N items, that's N inserts into the items
  BTreeMap alone (plus all the other tables).
- `Transaction::new` (`src/catalog/src/durable/transaction.rs:128`)
  takes that snapshot and walks every row a second time, calling
  `TableTransaction::new(initial.into_iter().map(RustType::from_proto)
  .collect())` — a full O(N) proto→Rust deserialisation into owned
  BTreeMaps that the transaction then mutates.
- `consolidate` (`persist.rs:706`) re-consolidates the in-memory
  trace via differential dataflow when the catalog reaches its
  consolidation threshold. Trace size grows with N → cost grows
  with N.

So **every single-statement DDL materializes the full catalog state
twice**: once into `Snapshot`, once into `TableTransaction.initial`.
Adapter serialises DDL so these are paid sequentially — there is no
parallelism to hide them.

Three prior fixes on this branch removed named O(N) loops in
`validate_resource_limits` (`f69d91c977`), `allocate_oids`
(`9fca09ff8a`), and `TableTransaction::insert` (`293a243e6b`), but
the slope is essentially unchanged — those loops contributed below
measurement noise at N=5000 relative to the structural cost above.

## Success Criteria

1. Single-statement DDL latency is roughly flat as catalog size grows.
   Concretely, on the `envd-ddl-scalability` harness with tables
   padding, the Δ between N=0 and N=5000 for the four ops above
   should drop from +17-31 ms to ≤ +5 ms.
2. The fix is invariant under add-only growth — N=10 000 and beyond
   should remain flat to the same tolerance, modulo persist-shard
   append cost (which is out of scope here, see below).
3. Multi-statement DDL transactions stop re-doing O(prior_statements)
   work per added statement.
4. No regression on DDL throughput on a small catalog.

## Out of Scope

- **Persist-side scaling of the catalog shard.**
  `PersistTableWriteCmd::Append` and `compare_and_append` on the
  catalog shard grow with shard history. Smaller per-DDL batches from
  the durable-overlay change should reduce this somewhat, but the
  remainder belongs in a separate workstream owned by storage/persist.
- **Storage controller `open_data_handles` super-linearity.** The
  shared-dependency MV-padding regression is dominated by per-shard
  persist init in the storage controller; tracked separately in
  `NOTES.md` and not addressed here.
- **Schema/format changes to what is durably stored.** The fix is
  about how transactions read and write the catalog in memory; the
  protobuf state-update encoding on persist is unchanged.

## Solution Proposal

The full catalog already exists once as in-memory shared state in
`PersistCatalogState`. The fix is to **stop re-materialising it per
transaction**.

Replace the per-transaction `Snapshot` construction with shared
indexed catalog data, owned by `PersistCatalogState` and maintained
incrementally on every commit. Transactions hold an overlay over
that shared data: reads probe the overlay then fall through to the
base, writes only touch the overlay, and commits emit only the delta.

### Shared durable state

In `src/catalog/src/durable/persist.rs`, give `PersistCatalogState` a
new field:

```rust
struct DurableCatalogData {
    databases:                 Arc<imbl::OrdMap<DatabaseKey, DatabaseValue>>,
    schemas:                   Arc<imbl::OrdMap<SchemaKey, SchemaValue>>,
    roles:                     Arc<imbl::OrdMap<RoleKey, RoleValue>>,
    role_auth:                 Arc<imbl::OrdMap<RoleAuthKey, RoleAuthValue>>,
    items:                     Arc<imbl::OrdMap<ItemKey, ItemValue>>,
    comments:                  Arc<imbl::OrdMap<CommentKey, CommentValue>>,
    clusters:                  Arc<imbl::OrdMap<ClusterKey, ClusterValue>>,
    network_policies:          Arc<imbl::OrdMap<NetworkPolicyKey, NetworkPolicyValue>>,
    cluster_replicas:          Arc<imbl::OrdMap<ClusterReplicaKey, ClusterReplicaValue>>,
    introspection_sources:     Arc<imbl::OrdMap<ClusterIntrospectionSourceIndexKey, ClusterIntrospectionSourceIndexValue>>,
    id_allocator:              Arc<imbl::OrdMap<IdAllocKey, IdAllocValue>>,
    configs:                   Arc<imbl::OrdMap<ConfigKey, ConfigValue>>,
    settings:                  Arc<imbl::OrdMap<SettingKey, SettingValue>>,
    source_references:         Arc<imbl::OrdMap<SourceReferencesKey, SourceReferencesValue>>,
    system_object_mappings:    Arc<imbl::OrdMap<GidMappingKey, GidMappingValue>>,
    system_configurations:     Arc<imbl::OrdMap<ServerConfigurationKey, ServerConfigurationValue>>,
    default_privileges:        Arc<imbl::OrdMap<DefaultPrivilegesKey, DefaultPrivilegesValue>>,
    system_privileges:         Arc<imbl::OrdMap<SystemPrivilegesKey, SystemPrivilegesValue>>,
    storage_collection_metadata: Arc<imbl::OrdMap<StorageCollectionMetadataKey, StorageCollectionMetadataValue>>,
    unfinalized_shards:        Arc<imbl::OrdMap<UnfinalizedShardKey, ()>>,
    txn_wal_shard:             Arc<imbl::OrdMap<(), TxnWalShardValue>>,

    indexes:   DurableCatalogIndexes,
    counts:    CatalogResourceCounts,
    allocated_oids: Arc<imbl::OrdSet<u32>>,
}
```

`imbl::OrdMap` gives O(1) structural cloning, so passing it into a
transaction is essentially free. The `Arc` is belt-and-suspenders and
makes ownership explicit at the borrow level.

`DurableCatalogData` is maintained by `PersistHandle::sync` / the
trace-apply path: each `StateUpdate` becomes an insert or remove on
the relevant map, plus index/count updates. The data structure
replaces the per-transaction `Snapshot` entirely; the trace
(`self.snapshot: Vec<(StateUpdateKind, Timestamp, Diff)>`) can be
dropped in favour of consolidating directly into the maps as updates
arrive. We keep the persist read-path machinery
(`sync_to_current_upper`, `update_applier`) as-is — only the
materialised view of the state changes shape.

### Indexes maintained alongside

`DurableCatalogIndexes` holds the lookups that today require scans:

```rust
struct DurableCatalogIndexes {
    database_by_name:           imbl::OrdMap<String, DatabaseKey>,
    schema_by_parent_name:      imbl::OrdMap<(Option<DatabaseId>, String), SchemaKey>,
    item_by_namespace:          imbl::OrdMap<ItemNamespaceKey, ItemKey>,
    role_by_name:               imbl::OrdMap<String, RoleKey>,
    cluster_by_name:            imbl::OrdMap<String, ClusterKey>,
    replica_by_cluster_and_name: imbl::OrdMap<(ClusterId, String), ClusterReplicaKey>,
    oid_owner:                  imbl::OrdMap<u32, DurableObjectKey>,
}
```

These are updated in lock-step with the main maps in the trace-apply
path. They turn the uniqueness checks in `TableTransaction::insert`
and the OID allocator's full-scan-for-membership into O(log N) point
lookups.

### Per-transaction overlay

In `src/catalog/src/durable/transaction.rs`, change `TableTransaction`
from owning its own `BTreeMap` of initial values to overlaying:

```rust
struct TableTransaction<K, V> {
    base: Arc<imbl::OrdMap<K, V>>,
    pending: BTreeMap<K, Vec<TransactionUpdate<V>>>,
    uniqueness_violation: Option<fn(&V, &V) -> bool>,
}
```

Construction is `Arc::clone(&data.items)` — no walk, no
deserialisation. Reads check `pending` first (overlay semantics),
then `base`. Writes only touch `pending`. Commit emits a delta:
exactly the keys in `pending`.

`Transaction::new` becomes proportional to the number of `Arc::clone`s
(constant in the number of tables, not in N). The work `Transaction::
new` does today — building `initial_oids` by walking every OID-bearing
table's values — moves into the shared `DurableCatalogData`. The
allocator at the transaction level uses
`data.allocated_oids.clone()` plus a per-transaction
delta-applied overlay; both lookup and update remain O(log N).

### Adapter-side: durable overlay travels with the DDL transaction

In `src/adapter/src/coord/`, today every statement in a multi-
statement DDL transaction (`BEGIN; ALTER … ; CREATE … ; COMMIT`) ends
up calling `catalog.transact_op_in_session_transaction` which
**replays the accumulated ops** against a fresh durable state. With
the overlay design this is unnecessary.

Replace the in-flight DDL representation with an owned durable
overlay:

```rust
enum TransactionOps {
    None,
    Reads,
    Writes(…),
    DDL {
        durable_txn: DurableCatalogTransaction,
        state: CatalogState,
        side_effects: Vec<…>,
        revision: u64,
    },
    …
}
```

This requires `Transaction<'a>` to stop borrowing `&'a mut dyn
DurableCatalogState`. Today the borrow exists because the transaction
holds the `durable_catalog` reference for things like
`get_and_increment_id`. Most of those calls can become methods that
read from the shared data + overlay; the remaining ones (allocator
bumps) can be modeled as overlay-only mutations until commit. Once
the borrow is gone, the transaction can be moved into
`TransactionOps::DDL` and held across statements, with each
subsequent statement adding to the same overlay.

Result: a 50-statement DDL transaction does one durable read at
`BEGIN` instead of 50, and commits once at `COMMIT`.

### What about `consolidate`?

In the current implementation, the trace vector
`PersistHandle::snapshot` is periodically consolidated via
`differential_dataflow::consolidation::consolidate_updates`. With the
proposed design, the trace can be applied incrementally into the
shared `DurableCatalogData` maps — there is no consolidated trace
vector to maintain on the read path. The `Vec` can stay as a write-
side buffer of recent updates if useful, but the materialised state
is the `imbl::OrdMap` view.

That eliminates the `consolidate` span entirely from the per-DDL
path.

### `apply_catalog_implications_inner` and `apply_updates` audit

These two together added +2 ms at N=5000. The structural fix doesn't
touch them; they are a separate audit pass.

- `apply_updates` (`src/adapter/src/catalog/apply.rs:102`): iterates
  the per-DDL delta of `StateUpdate`s, so should be O(delta). The
  growth implies something inside the apply handlers reads catalog-
  wide state. Audit each `apply_*_update` branch.
- `apply_catalog_implications_inner`
  (`src/adapter/src/coord/catalog_implications.rs:182`): handlers
  per implication kind. Same shape; one of the handlers is reading
  catalog-wide state.

Fix as small, focused changes after the structural fix lands.

### Storage point APIs

`StorageTxn::get_collection_metadata() -> BTreeMap<…>` forces full
scans on callers. Add:

```rust
fn get_collection_shard(&self, id: GlobalId) -> Option<ShardId>;
fn collection_metadata_contains_shard(&self, shard: ShardId) -> bool;
```

so the catalog transaction's storage-side prep stays proportional to
the affected collections. Small, mechanical, ride along with the
durable-overlay change.

## Minimal Viable Prototype

Land in this order so each step is independently shippable and
measurable. Each step has a clear regression signal in the audit
harness (`test/envd-ddl-scalability/audit.py`).

1. **`DurableCatalogData` + overlay reads** — no ownership changes
   to `Transaction<'a>` yet. `with_snapshot` returns an
   `Arc::clone` of the shared maps; `Transaction::new` consumes
   them as `Arc<imbl::OrdMap>` rather than owned BTreeMaps.
   `TableTransaction` becomes overlay-based.

   Regression signal: `snapshot`, `transaction`, and `consolidate`
   span self-times drop to near-zero. Slope should drop by ~1.5
   μs/object.

2. **Indexes** (`database_by_name`, `item_by_namespace`, etc.)
   maintained on the trace-apply path. Wire callers in
   `TableTransaction::insert`, `verify_keys`, the OID allocator
   membership probe, and any name-lookup paths.

   Regression signal: the secondary growth contributors from the
   trace ranking flatten.

3. **Maintained resource counts** (already in `CatalogState`, branch
   commit `f69d91c977`). Re-validate that this still works after
   step 1.

4. **OID allocator backed by `Arc<imbl::OrdSet<u32>>` in shared
   state**, with per-transaction delta. Removes the per-txn O(N)
   `initial_oids` construction from `Transaction::new`. The existing
   `initial_oids` cache (`9fca09ff8a`) becomes obsolete and is
   removed.

   Regression signal: `Transaction::new` self-time at high N drops
   further.

5. **Split `Transaction` ownership.** Stop borrowing
   `&mut dyn DurableCatalogState`; allow the transaction to be
   owned. Add `TransactionOps::DDL` carrying the durable overlay
   across statements. Multi-statement DDL transactions stop
   replaying ops.

   Regression signal: latency of a 10-statement DDL transaction
   should be flat in catalog size and drop dramatically vs. today's
   replay cost.

6. **`apply_catalog_implications_inner` / `apply_updates` audit.**
   Identify and fix the remaining per-DDL O(N) loops.

7. **Storage point APIs.** Mechanical.

The first step alone should close roughly half the slope on the
audit harness. Each subsequent step is a smaller win.

## Alternatives

### Keep `Snapshot`, optimise `Snapshot` construction

Make `Snapshot` lazily backed by the trace rather than eagerly
materialised. This preserves the existing transaction API but is a
bigger change to the snapshot type and still pays the proto→Rust
conversion when the transaction reads any row. The proposed design
sidesteps both costs by sharing the materialised state.

### A purely incremental `Snapshot` cache in `PersistCatalogState`

Cache the most recent `Snapshot`, mutate it incrementally as updates
arrive, hand out clones to transactions. This is closer to the
proposed design but uses owned BTreeMaps, so handing a Snapshot to a
transaction still costs O(N) for the clone unless the BTreeMap is
swapped for a structurally-shared map. At which point we have the
proposed design.

### Use `Arc<BTreeMap>` instead of `imbl::OrdMap`

`Arc<BTreeMap>` plus a separate `pending: BTreeMap<K, Option<V>>`
overlay is workable. `imbl::OrdMap` gives the same overlay semantics
for free (the data structure is itself Cow-on-write with O(log N)
mutations) and matches what `CatalogState` already uses. Choosing
`imbl` reduces the amount of bespoke overlay code.

### Skip the durable overlay; only fix the in-memory side

The trace shows the durable side dominates. An in-memory-only fix
would leave most of the slope intact.

## Open questions

- Are there callers of `Snapshot` outside the transaction path that
  need a fully materialised owned snapshot (e.g. dumps, debug tools,
  upgrade paths)? If so, expose a helper that materialises one from
  the shared maps on demand — only those callers pay the O(N) cost.
- Audit-log updates today live on the `Transaction`. They are
  append-only and don't need the overlay treatment; confirm they
  stay correct under the new transaction shape.
- The `verify` / `verify_keys` walks in `TableTransaction` use
  uniqueness-violation predicates. For the cases handled by named
  indexes (name-based uniqueness), the index does the work. For any
  predicate that isn't reducible to a key, fall back to the existing
  walk over `pending` only (skip `base`), and require the caller to
  supply an explicit "this predicate is index-backed" capability.
- The `revision: u64` field on `TransactionOps::DDL` is a hook for
  optimistic concurrency between in-flight readers and the
  serialised DDL writer. Defer the policy decision (re-check at
  commit vs. fail-fast on conflict) until step 5; the field is
  cheap to carry.
