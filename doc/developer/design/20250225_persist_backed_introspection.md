# Persist-backed compute introspection

* Associated: `persist_introspection` branch

## The problem

Compute introspection data (dataflow operators, arrangement sizes, scheduling histograms, etc.) is ephemeral.
When a replica restarts, all introspection data is lost and must be rebuilt from scratch.
Introspection indexes are only accessible through subscribes scoped to the active cluster, which requires the replica to serve the subscribe.
When a replica is struggling (out of CPU, thrashing), it cannot serve subscribes, making introspection data inaccessible precisely when it is most needed.

Additionally, introspection data is only accessible through subscribes scoped to the active cluster.
There is no way to query historical introspection data from a different cluster, join it with catalog metadata, or build materialized views over it.

## Success criteria

* Introspection data survives replica restarts through persist-backed storage.
* Stale data from a previous replica incarnation is retracted via self-correction.
* Introspection sources appear as queryable catalog items scoped to a specific replica.
* A global kill switch disables persist writes at runtime without requiring catalog changes.
* The feature is opt-in per cluster via `PERSIST INTROSPECTION = true`.

## Out of scope

* **Cross-replica aggregation.**
  Combining introspection data from multiple replicas into a single view.
* **Retention policies.**
  Automatic compaction or time-based retention for persisted introspection data.

## Solution proposal

### Overview

Each compute replica with persist introspection enabled gets:

1. A **per-replica schema** in the `materialize` database that holds the replica's introspection sources and views.
2. A set of **persisted introspection sources** (one per log variant, 32 total) stored as `CatalogItem::Source` items with `DataSourceDesc::PersistedIntrospection`.
3. **Per-replica views** that mirror the existing `mz_introspection` views, generated via SQL-to-SQL rewriting.
4. **MV persist sinks** on the compute side that write introspection data to persist shards with self-correction enabled.

The catalog stores persisted introspection sources in a **dedicated durable collection** (separate from the general `items` collection), following the same pattern as `IntrospectionSourceIndex`.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Catalog                                                         │
│                                                                 │
│  ┌──────────────────────────────┐                               │
│  │ Durable collection:          │                               │
│  │ persisted_introspection_     │  (cluster_id, replica_id,     │
│  │ sources                      │   name) → (item_id,           │
│  │                              │            global_id, oid)    │
│  └──────────────────────────────┘                               │
│                                                                 │
│  Per-replica schema: materialize.mz_introspection_{id}_{id}    │
│    ├── mz_dataflow_operators_per_worker    (Source, persisted)  │
│    ├── mz_scheduling_elapsed_raw           (Source, persisted)  │
│    ├── mz_compute_error_counts_raw         (Source, persisted)  │
│    ├── ... (32 log variants total)                              │
│    ├── mz_dataflow_operators               (View, rewritten)   │
│    ├── mz_arrangement_sizes                (View, rewritten)   │
│    └── ... (views mirroring mz_introspection)                  │
│                                                                 │
│  System schema: mz_introspection                                │
│    ├── mz_dataflow_operators_{cluster_id}_primary_idx  (Index)  │
│    └── ... (existing IntrospectionSourceIndex items)            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│ Compute replica                                                 │
│                                                                 │
│  For each log variant with a registered persist shard:          │
│    introspection stream ──► MV persist sink (self-correcting)   │
│                                    │                            │
│                                    ▼                            │
│                              Persist shard                      │
└─────────────────────────────────────────────────────────────────┘
```

### Catalog representation

#### Dedicated durable collection

Persisted introspection sources use their own durable collection rather than the general `items` collection.
This avoids the sentinel `create_sql` values (`"<persisted introspection: name>"`) that previously required guards in six places: three migration functions, the consistency checker, the topological sort, and `item_type()`.

The collection follows the `IntrospectionSourceIndex` pattern exactly:

* **Key**: `(cluster_id, replica_id, name)` — uniquely identifies a source.
* **Value**: `(catalog_item_id, global_id, oid, schema_id)` — the catalog metadata including the per-replica schema.
* **StateUpdateKind variant**: `PersistedIntrospectionSource` at the proto, durable, and memory levels.
* **Apply logic**: `apply_persisted_introspection_source_update()` constructs `CatalogItem::Source` with `DataSourceDesc::PersistedIntrospection(log_variant)` and a generated `CREATE MATERIALIZED VIEW` SQL string (see below).

#### ID allocation

Both `CatalogItemId` and `GlobalId` are deterministically encoded in a single `u64`:

```
Bits 56-63: replica variant (3 = System, 4 = User)
Bits 8-55:  replica inner ID
Bits 0-7:   log variant (0-31)
```

This encoding avoids collisions with `IntrospectionSourceIndex` IDs, which use cluster variant values 1 (Managed) or 2 (Unmanaged) in the same bit positions.
The encoding is deterministic: given a replica ID and log variant, both the `CatalogItemId` and `GlobalId` can be reconstructed without consulting an allocator.

#### Per-replica schemas

Each replica gets a schema created in the `materialize` database, named `mz_introspection_{cluster_id}_{replica_id}`.
The `SchemaId` is stored directly on the `PersistedIntrospectionSource` durable object, eliminating any name-based lookup during catalog apply.

Schemas and their contents get `rbac::default_builtin_object_privilege` grants (USAGE for schemas, SELECT for sources and views) so that any user with the PUBLIC role can query them.

#### `create_sql` generation

Persisted introspection sources use `create_sql` containing a `CREATE MATERIALIZED VIEW` statement:

```sql
CREATE MATERIALIZED VIEW mz_introspection_u2_u2.mz_arrangement_batches_raw
  AS SELECT worker_id, operator_id, batches
  FROM mz_introspection.mz_arrangement_batches_raw
```

The column list is derived from the log variant's `RelationDesc`.
The body references the corresponding `mz_introspection` source by name.
This satisfies the catalog consistency checker, which parses `create_sql` for all items.

#### Catalog implications

When `PersistedIntrospectionSource` state updates flow through `parse_state_update`, they are converted into `ParsedStateUpdateKind::Item` by synthesizing a durable `Item`.
This reuses the existing catalog implications path, generating a `CatalogImplication::Source(Added(...))` that triggers `handle_create_source`.
The storage controller creates a collection with `DataSource::Other`, which is required for reads via `PeekTarget::Persist` — the adapter needs the in-memory `CollectionMetadata` (containing the shard ID) and read holds to serve peek queries.

### Per-replica views via SQL-to-SQL rewriting

The existing `mz_introspection` schema contains views (e.g., `mz_arrangement_sizes`, `mz_dataflow_operators`) that aggregate raw introspection data into user-friendly formats.
These views reference raw `BuiltinLog` sources via fully-qualified names like `mz_introspection.mz_arrangement_batches_raw`.

Per-replica schemas need equivalent views that reference the replica's persisted sources instead.
Rather than duplicating view definitions, we generate them by rewriting the existing `BuiltinView` SQL.

#### Rewrite approach

Each `BuiltinView` in `MZ_INTROSPECTION_SCHEMA` has a `sql` field containing a `SELECT` statement with `mz_introspection.`-qualified source references.
The rewrite produces a `CREATE VIEW` in the per-replica schema by:

1. Taking the `BuiltinView.sql` and constructing `CREATE VIEW {per_replica_schema}.{name} AS {sql}`.
2. Parsing the statement with the existing SQL parser.
3. Walking the AST with a `VisitMut` implementation that replaces `mz_introspection` schema qualifiers in `UnresolvedItemName` nodes with the per-replica schema name.
4. Unparsing back to SQL via `to_ast_string_stable()`.

This rewrite is selective: only `mz_introspection.*` references are replaced.
References to other schemas (`mz_catalog.*`, `mz_internal.*`) remain unchanged, since those system tables are shared and schema-independent.

#### Why this works

* The `BuiltinView` definitions are the single source of truth.
  When upstream view SQL changes, per-replica views automatically pick up the new definitions on next catalog open.
* The parse/transform/unparse pipeline is the same pattern used by catalog migrations (`ast_rewrite_*` functions in `migrate.rs`).
* All SQL constructs (CTEs, subqueries, joins) are handled uniformly by the AST visitor.
* The per-replica sources have the same names and schemas as the `BuiltinLog` sources they mirror, so the rewritten SQL is valid.

#### Lifecycle

Views are created and dropped alongside sources in the same transaction:

* **Creation:** After inserting persisted introspection sources, iterate over `BuiltinView` definitions in `MZ_INTROSPECTION_SCHEMA`, rewrite each, and insert into the catalog via `tx.insert_item()`.
* **Drop:** When dropping a replica's per-replica schema, all views in that schema are dropped along with the sources.

#### Which views to include

Only `BuiltinView` definitions where `schema == MZ_INTROSPECTION_SCHEMA` and whose SQL references only `mz_introspection.*` sources are candidates.
Views that reference system tables outside `mz_introspection` (e.g., joins with `mz_catalog.mz_clusters`) work without modification since those references are left untouched by the rewrite.

### Compute-side persistence

Each log variant's introspection stream is connected to an MV persist sink:

* **Self-correction enabled:** On replica restart, the sink diffs the current introspection state against persisted state and emits corrections.
* **Starts from `Timestamp::MIN`:** The as_of is the minimum timestamp, so the sink captures the full history.
* **No controller frontier tracking:** The compute controller skips frontier updates for `GlobalId::PersistedIntrospectionSource` IDs because these sinks are entirely managed by the replica.
* **Replica-level read-only signal:** Logging persist sinks are not tracked as compute collections and cannot receive per-collection `AllowWrites` commands.
  Instead, they use a replica-level `read_only_rx` signal on `ComputeState`, initialized from `InstanceConfig.read_only` (which propagates the controller's instance-level `read_only` flag).
  The signal flips to writable on the first `AllowWrites` command for any collection, providing a fallback for 0dt promotion.
  This ensures correct behavior in all cases: non-0dt clusters start writable, 0dt clusters start read-only, and new replicas joining an already-promoted cluster receive the correct state via command history replay.

The "Persist flatten" operator (converting arranged key-value pairs back to flat rows) is implemented once in `persist::render_arranged` and shared by all four logging dataflow files (`compute.rs`, `differential.rs`, `timely.rs`, `reachability.rs`).

The `ENABLE_PERSIST_INTROSPECTION` dyncfg (`enable_persist_introspection`, default `true`) acts as a global kill switch.
When disabled, compute replicas skip writing introspection data to persist even if the cluster has `PERSIST INTROSPECTION = true`.

### Lifecycle

**Cluster creation** (`PERSIST INTROSPECTION = true`):

1. For each log variant, allocate a deterministic persisted introspection source ID.
2. Register persist shards for each source in storage via `storage_collections_to_register`.
3. Create a per-replica schema in the `materialize` database with default privileges.
4. Insert `PersistedIntrospectionSource` durable objects (with `schema_id`) into the catalog.
5. Create per-replica views via SQL-to-SQL rewriting of `mz_introspection` builtin views.
6. On catalog apply, `parse_state_update` converts the source updates into `ParsedStateUpdateKind::Item`, triggering storage collection creation.

**Replica drop:**

1. Look up the per-replica schema by ID.
2. Drop all views and persisted introspection source items.
3. Mark persist shards for cleanup.
4. Drop the per-replica schema.

**ALTER CLUSTER SET (PERSIST INTROSPECTION = true/false):**
Toggling the option on an existing cluster creates or drops sources for all existing replicas.

### Files modified

| Area | Files |
|------|-------|
| Proto types | `src/catalog-protos/src/objects.rs` |
| Durable objects | `src/catalog/src/durable/objects.rs`, `serialization.rs` |
| Collection infra | `src/catalog/src/durable/debug.rs` |
| State updates | `src/catalog/src/durable/objects/state_update.rs`, `persist.rs` |
| Transactions | `src/catalog/src/durable/transaction.rs` |
| Memory objects | `src/catalog/src/memory/objects.rs` |
| Apply logic | `src/adapter/src/catalog/apply.rs` |
| Bootstrap | `src/adapter/src/catalog/open.rs` |
| Transact | `src/adapter/src/catalog/transact.rs` |
| Catalog implications | `src/adapter/src/coord/catalog_implications/parsed_state_updates.rs` |
| Migrations/consistency | `src/adapter/src/catalog/migrate.rs`, `consistency.rs` |
| Debug tooling | `src/catalog-debug/src/main.rs` |
| Re-exports | `src/catalog/src/durable.rs` |
| Compute persistence | `src/compute/src/logging/persist.rs`, `compute.rs`, `differential.rs`, `timely.rs`, `reachability.rs` |
| Compute state | `src/compute/src/compute_state.rs` |
| Test infra | `src/catalog/tests/debug.rs`, `open.rs` |
| Lint config | `misc/python/materialize/parallel_workload/action.py`, `mzcompose/__init__.py` |

## Minimal viable prototype

The prototype is implemented on the `persist_introspection` branch and validated by the `test/persisted-introspection/mzcompose.py` test suite, which covers:

* SQL syntax for `CREATE/ALTER CLUSTER ... PERSIST INTROSPECTION`
* Dyncfg kill switch behavior
* Cluster lifecycle (create, use, drop, recreate)
* Replica restart with self-correction (stale data retraction)
* Catalog changes (add/remove clusters)
* Multiple coexisting clusters with persist introspection
* Environmentd restart with data consistency

## Alternatives

### Store persisted introspection sources in the `items` collection

The initial implementation stored persisted introspection sources in the general `items` collection with a sentinel `create_sql` value of `"<persisted introspection: name>"`.
This required guards in six places to skip or special-case items with invalid SQL: three migration functions (`ast_rewrite_create_sql_*`), the consistency checker, the topological sort (empty dependencies), and `item_type()`.

Rejected because every new migration or consistency check must remember to add a guard, and the sentinel SQL values break any code that parses `create_sql`.

### Reuse `IntrospectionSourceIndex` collection

Rather than creating a new durable collection, persisted introspection sources could share the `ClusterIntrospectionSourceIndex` collection.
The key difference is scope: introspection source indexes are per-cluster, while persisted introspection sources are per-replica.
The key type would need a `replica_id` field, changing the existing collection's semantics.

Rejected because it conflates two distinct concepts and would require migrating existing introspection source index data.

### Generate valid SQL for `create_sql`

Instead of a dedicated collection, generate a valid `CREATE SOURCE` statement (similar to `index_sql()` for indexes) and continue using the `items` collection.
The generated SQL would have no item dependencies (e.g., `SELECT 1`), making topological sort work naturally.

Rejected because items in the `items` collection go through migration functions that rewrite SQL ASTs.
Persisted introspection sources don't need SQL migration, and the fake SQL would need to be maintained across AST changes.
The dedicated collection approach is cleaner.

### Use `create_sql: None`

The current implementation uses `create_sql: None` on the `CatalogItem::Source`, which is also used by other builtin sources.
This works for the in-memory representation but doesn't address the durable storage question — the items would still need to be stored somewhere with sentinel markers unless a separate collection is used.

### Use regular materialized views instead of custom persist sinks

Instead of rendering persist sinks inside the logging dataflows (via `logging/persist.rs`), create actual materialized views over the introspection sources.
The MV definitions would be identical to the current `create_sql` (e.g., `CREATE MATERIALIZED VIEW ... AS SELECT * FROM mz_introspection.mz_arrangement_batches_raw`), but they would be real catalog items rendered through the standard dataflow pipeline rather than custom operator graphs wired into logging initialization.

#### How it would work

The per-replica schema would contain materialized views instead of sources + views:

```
materialize.mz_introspection_u2_u2
  ├── mz_dataflow_operators_per_worker    (Materialized View)
  ├── mz_scheduling_elapsed_raw           (Materialized View)
  ├── ... (32 raw log variant MVs, each SELECT * FROM mz_introspection.<name>)
  ├── mz_dataflow_operators               (Materialized View over the raw MVs)
  └── mz_arrangement_sizes                (Materialized View over the raw MVs)
```

Each raw MV selects from the existing `mz_introspection` subscribe source (the `IntrospectionSourceIndex` that arranges logging data in memory).
The standard MV rendering pipeline handles persist sink creation, self-correction, frontier tracking, and read-only semantics.

Creation happens during `Op::CreateClusterReplica` by generating `CREATE MATERIALIZED VIEW` catalog ops.
Dropping happens automatically through the dependency system (already implemented via `cluster_replica_dependents` → `schema_dependents`).

#### Advantages

* **No compute changes.**
  Removes `logging/persist.rs` entirely.
  No custom persist sink rendering, no "Persist flatten" operator, no replica-level `read_only_rx` signal.
  Logging dataflows remain unchanged — they only produce arranged in-memory collections as before.
* **Runtime creation and removal.**
  MVs can be created and dropped at runtime via catalog ops without restarting the replica.
  This enables enabling/disabling persist introspection on existing clusters without recreating replicas.
* **Standard lifecycle management.**
  Per-collection frontier tracking, `AllowWrites` commands, controller visibility, and 0dt promotion all work out of the box.
  No special-casing needed for logging sinks.
* **No dedicated durable collection.**
  MVs are regular items in the `items` collection with valid `create_sql`.
  Removes the `PersistedIntrospectionSource` durable collection, the `CatalogItemId::PersistedIntrospectionSource` variant, deterministic ID encoding, dedicated apply logic, and the partition in the item drop path.

#### Challenges

* **Cluster binding.**
  Regular MVs are bound to a cluster — they need a cluster to render the dataflow on.
  The introspection MVs must be rendered on the same cluster and replica whose logging they capture.
  A `SELECT * FROM mz_introspection.mz_arrangement_batches_raw` on cluster X captures cluster X's data, so the MV must also run on cluster X.
  This is the natural behavior, since MVs are bound to the cluster they're created on.
  However, the MV dataflow runs on all replicas of the cluster, not just the target replica.
  On non-target replicas, the MV reads the local `mz_introspection` source (which contains that replica's data, not the target's), so it captures the wrong replica's data.
  This is a fundamental mismatch: per-replica introspection requires per-replica dataflows, but MVs render on all replicas.
* **`as_of` semantics.**
  The current implementation starts persist sinks from `Timestamp::MIN` to capture the full history.
  Regular MVs use the `as_of` from the dataflow, which is typically the current timestamp.
  Missing the initial data window means losing introspection data from before the MV was created.
  This may be acceptable since the MV is created immediately with the replica.
* **Source availability.**
  The `mz_introspection` sources (subscribe-backed `IntrospectionSourceIndex` items) must be readable as of the MV's `as_of`.
  These sources are arranged in memory and may not be ready at the exact moment the MV dataflow starts.
  The standard dataflow rendering already handles this via `StartSignal` / suspension tokens, but it needs to be verified that introspection indexes are treated as available inputs.
* **Arranged-to-flat conversion.**
  The current implementation explicitly converts arranged key-value pairs back to flat rows via `render_arranged`.
  With MVs, this conversion happens implicitly: the MV reads from the introspection source index (which is arranged), and the peek/subscribe path handles flattening.
  This should work transparently since the MV's `SELECT *` produces flat rows.

#### Assessment

The cluster binding challenge is the main blocker.
Regular MVs render on all replicas of a cluster, but per-replica introspection requires per-replica dataflows.
An MV created for replica R1 would also render on replica R2, where it captures R2's data instead of R1's — silently writing the wrong data to R1's persist shard.

Possible mitigations:
* **Replica-pinned MVs:** A new MV variant that renders only on a specific replica.
  This does not exist today and would require changes to the compute controller's dataflow scheduling.
* **Per-replica sources:** If the `mz_introspection` sources were per-replica (each replica has its own `GlobalId`), the MV could reference the correct source.
  This would require changes to introspection source index management.
* **Accept the limitation:** Only support single-replica clusters for persist introspection.
  This sidesteps the multi-replica issue but limits the feature's applicability.

Given the cluster binding issue, the custom persist sink approach avoids a significant gap in the MV infrastructure.
The custom approach hooks directly into the per-replica logging dataflow, guaranteeing it captures that replica's data.
If replica-pinned dataflows become available in the future, migrating to regular MVs would simplify the implementation.

## Open questions

### Querying from `mz_introspection`

The existing `mz_introspection` schema works within the context of a `SET CLUSTER` session variable.
Persisted introspection sources live in per-replica schemas, which means they are queryable by fully qualifying the schema name.
Whether persisted sources should also be accessible through the standard `mz_introspection` schema (perhaps with a `SET CLUSTER REPLICA` variable) is an open UX question.
