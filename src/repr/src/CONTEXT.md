# repr::src (mz-repr)

The lingua franca of Materialize: all layers of the stack share the types
defined here at their boundaries. No layer above this one is a dependency.

## Subdirectories

| Dir | What it owns |
|---|---|
| `adt/` | PostgreSQL-compatible ADT impls (array, char, date, datetime, interval, JSONB, ACL, numeric, range, regex, timestamp, varchar) — see [`adt/CONTEXT.md`](adt/CONTEXT.md) |
| `row/` | Arrow columnar encoding (`encode.rs`) and abstract row iteration traits (`iter.rs`) |
| `explain/` | Format-specific EXPLAIN renderers (text, JSON, DOT, tracing) |

## Files (LOC ≈ 19,187 in `src/repr/src/` excluding subdirs)

| File | What it owns |
|---|---|
| `scalar.rs` | `Datum<'a>` value enum; dual-type system: `SqlScalarType` (36 variants, SQL-level with modifiers) and `ReprScalarType` (29 variants, collapsed for compute/storage); `DatumKind` copy-tag; proptest strategies |
| `relation.rs` | `RelationDesc` (primary schema descriptor); dual-type split into `SqlColumnType`/`SqlRelationType` vs `ReprColumnType`/`ReprRelationType`; schema evolution (`RelationDescDiff`, `VersionedRelationDesc`) |
| `row.rs` | `Row` (compact byte sequence), `RowRef`, `RowPacker`, `RowArena`, `DatumList`, `DatumMap`, `SharedRow` |
| `strconv.rs` | String-to-value conversions matching PostgreSQL text format |
| `explain.rs` | `Explain` trait, `ExplainConfig`, `ExplainFormat`, `ExprHumanizer` trait; `ExplainConfig` threads through the optimizer pipeline |
| `stats.rs` | Persist pushdown statistics for complex types (numeric, timestamp, interval, JSONB) |
| `timestamp.rs` | `Timestamp` (system-wide `u64` ms type implementing Timely/DD traits) |
| `update.rs` | `UpdateCollection<T>` — columnar `(row, time, diff)` triples; `Rows`/`RowsBuilder`; `SharedSlice<T>` |
| `optimize.rs` | `OptimizerFeatures`, `OptimizerFeatureOverrides`, `OverrideFrom` trait |
| `refresh_schedule.rs` | REFRESH EVERY/AT schedule representation |
| `global_id.rs` / `catalog_item_id.rs` / `role_id.rs` / `network_policy_id.rs` | Identifier newtypes (16-byte `GlobalId` size-asserted) |
| `namespaces.rs` | Well-known schema name constants |
| `fixed_length.rs` | `ToDatumIter` abstraction |
| `datum_vec.rs` | Reusable `Datum` scratch buffer |
| `bytes.rs` | PostgreSQL-compatible `ByteSize` |
| `diff.rs` | `Diff` type alias (`Overflowing<i64>`) |
| `user.rs` | External user auth metadata |

## Key concepts

- **Dual-type system.** `SqlScalarType` (SQL layer, preserves modifiers like
  `VarChar(n)`, `Char(n)`, Oid subtypes) coexists with `ReprScalarType`
  (compute/storage layer, collapses those distinctions). Parallel `Sql*` /
  `Repr*` structs exist for both scalar and relation types. `From` impls
  convert downward; `backport_nullability` reconciles nullability back upward.
- **`Row` encoding.** Values are stored as a compact byte sequence decoded on
  demand. `RowArena` allocates borrowed `Datum<'a>` values into a scoped arena.
- **`Timestamp`.** A `u64` millisecond epoch that implements Timely `Timestamp`
  and differential-dataflow traits; the system-wide time coordinate.
- **`UpdateCollection<T>`.** Columnar storage for batches of
  `(row, time, diff)` triples — the fundamental update unit in DD pipelines.
- **`ExprHumanizer`.** A trait defined here but implemented upstream (in
  `mz-adapter`/`mz-catalog`); lets repr render plan nodes with catalog-resolved
  names without depending on those crates.
- **`OptimizerFeatures` in repr.** Query-time optimizer flags live here so they
  can be shared between `mz-adapter`, `mz-compute`, and catalog persistence
  without introducing cross-layer deps.

## Cross-references

- Downstream consumers: `mz-expr`, `mz-compute`, `mz-storage-*`, `mz-adapter`,
  `mz-catalog`, `mz-sql`, `mz-pgrepr`, `mz-interchange`, and more.
- `mz-sql-parser` is a dependency (for `NamedPlan` in `explain/tracing.rs`).
- Generated developer docs: `doc/developer/generated/repr/`.
