# Convert `mz_internal.mz_object_dependencies` to a BuiltinMaterializedView

## Context

Part of the SQL-150 series (multi-envd): builtin tables written by coordinated
`pack_*` updates from a single environmentd are being converted to builtin
materialized views over `mz_internal.mz_catalog_raw`. Precedents: mz_tables and
mz_views (94da6082c6), mz_indexes (d777d0f6da), mz_kafka/postgres_sources
(34813f0a3f), mz_roles (5b42d5e422), mz_audit_events (584bb9030c).

`mz_object_dependencies` is the hard one because its rows come from
`ResolvedIds` computed at plan time, and three edge classes are not directly
recoverable from stored `create_sql`:

- **Function references** print as plain qualified names (`print_id = false`
  for `Func` in `src/sql/src/names.rs:1521`), not `[id AS name]`.
  **Decision (user-confirmed): keep these edges** by emitting function names
  from the parser and joining `(schema_name, object_name)` against GidMapping
  rows (object_type `'8'`). All functions are builtins, so this is exact.
- **Array element_reference ids** were injected silently at resolution
  (`names.rs:1385`, `:1444`) and never printed.
  **Decision (user-confirmed): stop recording them in name resolution
  itself** (step 0), so `T[]` and `_T` both resolve to the array type alone.
  Recovering them per-consumer would leave user objects and builtins
  disagreeing, since only the builtin generator can reconstruct the pairing.
  Nothing user-visible consumes these edges (pg_depend filters to
  relations/indexes), and the fold-based resolver ends up agreeing with
  `DependencyVisitor` (`names.rs:2449`), which already ignores
  `element_reference` and is what source purification uses today.
- **Builtin objects' edges** (>200 rows with `object_id LIKE 's%'`): builtins
  have no `Item` rows in mz_catalog_raw.
  **Decision (user-confirmed): make_mz_indexes-style generator** that parses
  every builtin view/MV/index SQL at static-init, extracts referenced names,
  and inlines `(object_schema, object_name, object_type, ref_schema, ref_name,
  ref_kind)` VALUES joined to GidMapping at query time. Large VALUES list is
  accepted; inlining keeps the fingerprint-forces-migration property.

Other verified facts that shape the design:

- Today's packing: `pack_depends_update` at
  `src/adapter/src/catalog/builtin_table_updates.rs:106-117`, called from
  `pack_item_update` at `:212-218` over `entry.item().references().items()`,
  **skipping temporary items**.
- Builtin tables/sources/types/funcs/logs have empty `ResolvedIds`
  (`src/adapter/src/catalog/apply.rs:884`, `:983`, `:1028`) — only builtin
  views/MVs/indexes and introspection source indexes (`si* → s<log>`,
  apply.rs:2164) produce edges today.
- GidMapping rows carry `(schema_name, object_type, object_name) → catalog_id`
  for all builtins incl. funcs (`'8'`) and types (`'7'`)
  (`src/catalog-protos/src/objects_v91.rs:2240-2252`).
- `test_builtins_static_dependency_order` (`src/catalog/src/builtin.rs:1680`)
  already asserts builtin last-component names are globally unique across
  schemas, which makes 1-part-name → schema lookup in the generator sound. It
  also contains a `Visit<Raw>` `ItemNameCollector` precedent (:1660).
- `WithOptionValue::Secret/Item` route connection→secret, connection→connection,
  sink→connection references through `visit_item_name` generically.
- Workspace dev version: `26.38.0-dev.0` (`src/environmentd/Cargo.toml:4`).
- `migrate_builtin_tables_to_mvs` (`src/adapter/src/catalog/migrate.rs:848`)
  is generic — no change needed.
- Converted MVs keep their builtin indexes: `MZ_OBJECT_DEPENDENCIES_IND`
  (`mz_internal.rs:8734`) stays untouched.
- Constraint: the MV's SQL may only reference `mz_catalog_raw` and scalar
  functions (not `mz_schemas`/`mz_databases`, registered after its list slot).
  GidMapping rows carry `schema_name` directly, so this holds.

## Implementation steps

### 0. mz-sql: stop recording array element types

`NameResolver::resolve_data_type` records the element type of every `T[]` and,
for a named array type, its `element_reference`. Both go away: `T[]` and `_T`
resolve to the array type alone.

Threading a `record_ids` flag through `resolve_data_type_inner` is what makes
this precise. The element type still has to be *resolved* to find its paired
array type, but it must not be *recorded*, and a blanket removal afterwards
would also drop a legitimate independent mention of `T` in the same statement.

Lands as its own change ahead of the conversion: it is independently
reviewable, and it is what lets every later step treat arrays as ordinary
type references.

### 1. mz-sql-parser: shared reference collector

New module `src/sql-parser/src/ast/item_refs.rs` (exported from `ast`), used by
both the new sqlfunc and the builtin generator:

```rust
pub struct ItemReferences {
    pub ids: BTreeSet<String>,                 // RawItemName::Id anywhere (incl. inside RawDataType)
    pub named_relations: BTreeSet<UnresolvedItemName>, // RawItemName::Name outside function/type position, CTE-excluded
    pub named_funcs: BTreeSet<UnresolvedItemName>,     // Function<Raw>.name when RawItemName::Name
    pub named_types: BTreeSet<UnresolvedItemName>,     // RawDataType::Other names, written without []
    pub named_array_elements: BTreeSet<UnresolvedItemName>, // element names written T[]; reference _T, not T
}
pub fn collect_item_references(stmt: &Statement<Raw>) -> ItemReferences
```

Implemented as `impl Visit<'ast, Raw>` overriding `visit_function` (record the
name as func, manually visit args/filter/over so `visit_item_name` isn't re-hit
for the name), `visit_data_type` (manual recursion through
`RawDataType::{Array, List, Map, Other}`), `visit_item_name` (fallback =
relation), plus a pre-pass collecting CTE names (both `Cte` and WMR
`CteMutRec`) to exclude 1-part relation references. Generic walking means all
statement kinds (views, MVs, indexes, sources, tables incl. FROM SOURCE,
sinks, connections, webhook CHECK secrets, `FOR mv` replacement MVs,
`VERSION`ed refs) are covered by construction.

Unit tests: CTE exclusion incl. WMR, func-name classification, RawDataType
recursion, bracketed-id + VERSION handling.

### 2. mz-expr: `parse_catalog_item_references` sqlfunc

In `src/expr/src/scalar/func/impls/jsonb.rs`, following
`parse_catalog_create_sql` (:413):

```rust
/// Returns {"ids": ["u1","s470",...],
///          "funcs": [{"schema":"pg_catalog","name":"abs"},...],
///          "types": [...]}   // types-by-name: defensive, expected empty in stored SQL
#[sqlfunc]
fn parse_catalog_item_references<'a>(a: &'a str) -> Result<Jsonb, EvalError>
```

Unit tests in the style of commit b518faaec4 (parse_catalog_create_sql tests).

### 3. Registration and OIDs

- Register the func in `src/sql/src/func.rs` (MZ_INTERNAL_BUILTINS, near :5345).
- `src/pgrepr-consts/src/oid.rs`: **reuse the existing OID** (user-confirmed).
  Rename `TABLE_MZ_OBJECT_DEPENDENCIES_OID` (line ~423) to
  `MV_MZ_OBJECT_DEPENDENCIES_OID`, keeping the value 16698. Precedent:
  `MV_MZ_KAFKA_CONNECTIONS_OID: u32 = 16695` (oid.rs:420) kept its table OID.
  Append only `FUNC_PARSE_CATALOG_ITEM_REFERENCES_OID = 17120` at the tail
  (currently 17119).

### 4. mz-catalog: the generator and the MV

- Delete the `MZ_OBJECT_DEPENDENCIES` BuiltinTable static
  (`src/catalog/src/builtin/mz_internal.rs:397-454`).
- Add `make_mz_object_dependencies(builtins: impl Iterator<Item = &Builtin<NameReference>>) -> BuiltinMaterializedView`
  in mz_internal.rs:
  - For every `Builtin::View | MaterializedView | Index`, parse `create_sql()`
    and run the shared collector.
  - Name → (schema, name): ≥2 parts → last two; 1 part → schema from a
    name→schema map over the full builtin list (sound per the global-uniqueness
    assertion). Panic on lookup failure. `assert_safe_builtin_name` on
    everything inlined (hoist from `mz_catalog.rs:640` to `pub(super)`).
  - Emit deduped VALUES rows with `ref_kind ∈ ('rel','func','type')`; assert no
    self-edges. Omit mz_object_dependencies' own outgoing edges (can't inline
    its own rows without a fixpoint; today it has zero outgoing edges as a
    table anyway — precedent: `mz_builtin_views` omits itself,
    mz_catalog.rs:1454-1456).
  - Keep `desc` (two non-null text columns, no keys), `column_comments`,
    `ontology`, `access: PUBLIC_SELECT`, `is_retained_metrics_object: true`
    from the current table.
- In `src/catalog/src/builtin.rs`: remove `Builtin::Table(&MZ_OBJECT_DEPENDENCIES)`
  (:1113). Run the generator **after the final builtins vec is fully assembled**
  (after ontology views :1526 and `mz_builtin_*` prepends :1530-1537, since
  those have edges too), then insert at the old slot:
  `position(Builtin::Table(t) if t.name == "mz_iceberg_sinks")` — preserves
  fresh-install system-id order (e.g. `s483` in
  mz_catalog_server_index_accounting.slt).

MV SQL shape (generated):

```sql
IN CLUSTER mz_catalog_server
WITH (ASSERT NOT NULL object_id, ASSERT NOT NULL referenced_object_id) AS
WITH
  user_items AS (
    SELECT mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
           mz_internal.parse_catalog_item_references(
             data->'value'->'definition'->'V1'->>'create_sql') AS refs
    FROM mz_internal.mz_catalog_raw
    WHERE data->>'kind' = 'Item'
      AND data->'value'->>'ephemeral_owner_session' IS NULL   -- temp items excluded, as today
  ),
  gid_mappings AS (
    SELECT 's' || (data->'value'->>'catalog_id') AS id,
           data->'key'->>'schema_name' AS schema_name,
           data->'key'->>'object_name' AS object_name,
           data->'key'->>'object_type' AS object_type
    FROM mz_internal.mz_catalog_raw
    WHERE data->>'kind' = 'GidMapping'
  ),
  user_id_edges AS (
    SELECT u.id, r.ref FROM user_items u,
           jsonb_array_elements_text(u.refs->'ids') AS r(ref)
  ),
  user_func_edges AS (  -- identical defensive arm for refs->'types' joining object_type '7'
    SELECT u.id, gm.id FROM user_items u,
           jsonb_array_elements(u.refs->'funcs') AS f(func)
    JOIN gid_mappings gm ON gm.object_type = '8'
      AND gm.schema_name = f.func->>'schema' AND gm.object_name = f.func->>'name'
  ),
  builtin_edges AS (
    SELECT obj.id, ref.id
    FROM (VALUES {builtin_ref_values}) AS bv(...)
    JOIN gid_mappings obj ON (bv object side, object_type '4'/'5'/'6')
    JOIN gid_mappings ref ON (bv ref side; func→'8', type→'7', rel→IN ('1','2','4','5'))
  ),
  introspection_source_index_edges AS (
    -- si<id> → s<log id>, joining ClusterIntrospectionSourceIndex rows to the
    -- mz_introspection GidMapping by name (pattern: make_mz_indexes CTE)
  )
SELECT ... UNION ALL ...
```

### 5. mz-adapter: remove packing

- Delete `pack_depends_update` (`builtin_table_updates.rs:106-117`), the loop
  at `:212-218`, and the `MZ_OBJECT_DEPENDENCIES` import (:22). Leave a short
  comment in the established style pointing at the MV.

### 6. Migration step

Append to `MIGRATIONS` in
`src/adapter/src/catalog/open/builtin_schema_migration.rs`:

```rust
MigrationStep::replacement(
    "26.38.0-dev.0",
    CatalogItemType::MaterializedView,
    MZ_INTERNAL_SCHEMA,
    "mz_object_dependencies",
),
```

No older step names this table (verified).

### 7. Rust tests

Fingerprint tests in `builtin.rs` mirroring `:2169-2295`
(make_mz_sources/make_mz_indexes): stability (generator output matches the
BUILTINS_STATIC entry) and sensitivity (adding a builtin view with a new
reference changes the fingerprint).

### 8. Docs

`doc/user/content/reference/system-catalog/mz_internal.md:638-643`: "table" →
"materialized view" wording. RELATION_SPEC line unchanged (columns unchanged).
Do NOT touch `doc/developer/generated/` (read-only).

### 9. SQL tests

Regenerate slt goldens with `bin/sqllogictest -- --rewrite-results PATH`
(consult `mz-test` skill first, batched); testdrive files edited by hand.

- **New lockdown test** `test/sqllogictest/mz_object_dependencies.slt`
  (template: `mz_indexes.slt`/`mz_views.slt`, `reset-server` for stable ids):
  view→table edge; func edge (`abs`) cross-checked against `mz_functions`;
  cast/column type edge; connection→connection and connection→secret;
  table-FROM-SOURCE→source; temp-table exclusion; builtin spot-check
  (e.g. an edge from a converted builtin MV to `mz_catalog_raw`); si→log edge;
  no-duplicates assertion.
- **Must pass unchanged** (behavioral oracles): `test/testdrive/mz-depends.td`
  (>200 s% rows, no dupes, no 2-cycles, subsource edges),
  `test/sqllogictest/replacement-materialized-views.slt` (`FOR mv` edges).
- **Expected golden churn**: `oid.slt` (16698 keeps its name so its row likely
  stays, verify; new 17120 func row added),
  `information_schema_tables.slt` (:488 BASE TABLE → MATERIALIZED VIEW),
  `test/testdrive/catalog.td` (:611 moves from tables to materialized-views
  listing), `autogenerated/mz_internal.slt`,
  `mz_catalog_server_index_accounting.slt`, `catalog_server_explain.slt`,
  `show_create_system_objects.slt`, `system-cluster.slt` (:356-363 — plan
  should still be `ReadIndex ... mz_object_dependencies_ind`),
  `cockroach/srfs.slt`.

### 10. Generic old-vs-new builtin-relation diff harness (user-requested)

The strongest correctness check for any builtin-table→MV migration: dump the
relation's contents on a fresh environment running the baseline build, dump it
on a fresh environment running the new build with the same user-object corpus,
and diff. A fresh environment's ~500 builtin objects are a rich free test
corpus. Build this as a **generic, reusable composition** so future migrations
in the series (mz_columns, mz_sinks, mz_index_columns, ...) just add an entry.

New composition `test/builtin-relation-diff/mzcompose.py`:

**Configuration** — a module-level table drives everything:

```python
@dataclass
class RelationDiffConfig:
    ignore_columns: list[str] = ...       # e.g. mz_audit_events.occurred_at
    allow_old_only: Callable[[Row], bool] = ...  # expected disappearances
    allow_new_only: Callable[[Row], bool] = ...  # expected additions

RELATIONS: dict[str, RelationDiffConfig] = {
    "mz_internal.mz_object_dependencies": RelationDiffConfig(
        # element-ref edges dropped by step 0: old-only edge to type X where
        # the same object also has an edge to the paired array type _X.
        # Removable once the baseline itself omits the element edge.
        allow_old_only=is_dropped_element_ref_edge,
    ),
}
```

**Workflow** (`run`, with `--old-image` and repeatable `--relation` args
defaulting to all of `RELATIONS`):

1. Resolve the baseline image: `--old-image` flag; else
   `resolve_ancestor_image_tag` (`misc/python/materialize/version_list.py:173`,
   as used by `test/version-consistency/mzcompose.py`), overridable via
   `COMMON_ANCESTOR_OVERRIDE`. For an exact diff use
   `materialize/materialized:devel-$(git merge-base HEAD origin/main)`.
2. For each of [baseline image, locally built image]: start `Materialized`
   against a fresh data dir, apply the shared corpus SQL, dump each
   configured relation with `SELECT * FROM <rel>`, canonicalize, sort rows,
   write to a file. Tear down between runs.
3. Diff per relation, applying the config's ignore/allow hooks. Fail the
   workflow on any unexpected row, printing the offending rows.

**Generic id canonicalization** — the key trick that makes raw `SELECT *`
dumps comparable across builds. Fresh-install system-id assignment shifts
whenever a builtin is added (e.g. this PR's new func), so ids can't be diffed
literally. On each side, build an id→qualified-name map from `mz_objects` +
`mz_schemas` + `mz_databases` (and `mz_clusters`, `mz_cluster_replicas`,
`mz_roles` for their id spaces), then rewrite any cell value matching the id
shapes (`u\d+`, `s\d+`, `si\d+`, cluster/replica/role ids) to the qualified
name before diffing. This is relation-agnostic: no per-relation dump query
needed, and it works for id-bearing columns in any future migration.

**Shared corpus** (one SQL setup used for all relations), covering every edge
class relevant here and broadly useful later: tables with array-typed columns
(exercises the dropped element-ref edges), views with function calls and
casts, materialized views, indexes, a load-generator source with subsources
(auction), `CREATE TABLE ... FROM SOURCE`, secrets, a Kafka connection + sink
via a Redpanda service (connection→secret, sink→connection edges), and a
temporary table (must appear in neither dump).

**Manual procedure** (documented in the PR body for spot use): on main,
`bin/environmentd --reset`, apply the corpus, dump via psql `\copy`; repeat on
the branch; `diff`. Every difference must match the allowlist.

Check whether new compositions must be registered in a CI pipeline (there is
lint/CI machinery around composition discovery, consult the mz-test skill
during implementation). This composition is a migration-validation tool, run
manually via `bin/mzcompose --find builtin-relation-diff run run`; wiring into
nightly is optional and not part of this change.

### 11. Checks before reporting done

`bin/fmt` and `cargo check` (per CLAUDE.md), plus the new unit tests via the
mz-test skill conventions.

## Explicit semantic changes vs today (call out in PR body)

1. Array element_reference edges dropped (user-approved, step 0): `int4[]` and
   `_int4` both yield an edge to `_int4` only, for user objects and builtins
   alike. This is a name-resolution change, not a property of the conversion,
   so it applies uniformly and ships as its own change.
2. Builtin-arm name resolution relies on global builtin-name uniqueness
   (already asserted by `test_builtins_static_dependency_order`) instead of
   true search-path resolution. Add a generation-time assert for CTE-name
   shadowing.
3. mz_object_dependencies lists no outgoing edges for itself (matches today's
   zero-edge behavior, precedented by mz_builtin_views omitting itself).
4. Edge updates now flow through the mz_catalog_server MV dataflow rather than
   synchronous coordinator writes (accepted trade of the whole series).

## Verification

1. `cargo test -p mz-sql-parser` (collector), `cargo test -p mz-expr` (sqlfunc),
   `cargo test -p mz-catalog` (fingerprint tests).
2. `bin/sqllogictest` on the new lockdown file and the churned goldens
   (rewrite-results, then eyeball the diff).
3. Testdrive `mz-depends.td` unchanged and green — this is the primary
   correctness oracle for edge fidelity.
4. **Primary validation**: the old-vs-new diff harness (step 10) — run the
   mzcompose workflow and confirm the diff is empty modulo the allowlist. Also
   `SELECT count(*) ... WHERE object_id LIKE 's%'` > 200 on the new build.
5. Upgrade path: existing catalogs hit `MigrationStep::replacement`; the
   generic `migrate_builtin_tables_to_mvs` rewrites the object type. The
   platform-checks builtin_version_pin check may need the same treatment as
   in 34813f0a3f (`misc/python/materialize/checks/all_checks/builtin_version_pin.py`) — check during implementation.
