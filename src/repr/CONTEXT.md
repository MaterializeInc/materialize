# mz-repr

The lingua franca of Materialize: defines the core data types that all layers
of the stack agree on at their boundaries. No Materialize crate sits below it
(only `mz-ore`, `mz-proto`, `mz-pgtz`, `mz-persist-types`, `mz-sql-parser`).

## Structure

| Path | LOC | What it owns |
|---|---|---|
| `src/adt/` | 11,937 | PostgreSQL-compatible ADT implementations — see [`src/adt/CONTEXT.md`](src/adt/CONTEXT.md) |
| `src/` | 35,001 | All other repr modules — see [`src/CONTEXT.md`](src/CONTEXT.md) |

## Key types (bubble-up summary)

- **`Datum<'a>`** — core value enum; all SQL values pass through this type.
- **`Row` / `RowRef` / `RowPacker`** — compact byte-sequence encoding of a
  tuple of `Datum` values; the fundamental data unit in dataflow pipelines.
- **`RelationDesc`** — ordered named-column schema descriptor; used at every
  layer boundary (planning, storage, compute).
- **Dual-type system** — `SqlScalarType` (SQL-level, preserves modifiers) and
  `ReprScalarType` (compute/storage-level, collapsed variants); parallel split
  for relation types. The seam is intentional; `backport_nullability` is the
  reverse adapter.
- **`Timestamp`** — system-wide `u64` millisecond type implementing Timely and
  differential-dataflow traits.
- **`UpdateCollection<T>`** — columnar `(row, time, diff)` batch; the
  differential-dataflow update unit.
- **`ExprHumanizer`** — trait defined here, implemented upstream; decouples
  plan rendering from catalog knowledge.
- **`OptimizerFeatures`** — query-time optimizer flags; lives here for sharing
  across `mz-adapter`, `mz-compute`, and catalog persistence.

## Architecture notes (from ARCH_REVIEW)

1. **Dual-type seam** — correct design; `backport_nullability` is a narrow
   known cost. Monitor as new type modifiers are added.
2. **`OptimizerFeatures` placement** — optimizer policy in a data-repr crate
   is a mild layer violation; migration target is `mz-compute-types` or a
   thin `mz-repr-optimizer` crate.
3. **`mz-sql-parser` dep for one 6-variant enum** — `NamedPlan` should move
   into `repr::explain`; removes a heavy transitive build dep from the core
   data layer.

## What to bubble up to `src/CONTEXT.md`

- `mz-repr` is the stack's foundational data layer; every other crate in this
  review will import it.
- The dual-type system (`Sql*` vs `Repr*`) is the primary planning↔execution
  interface seam in the type system.
- `OptimizerFeatures` and `NamedPlan` are two cases where non-data concepts
  have leaked into the foundational data crate; both are migration candidates.

## Cross-references

- Generated developer docs: `doc/developer/generated/repr/`.
- See `src/ARCH_REVIEW.md` (not yet written) for cross-crate patterns.
