# expr::scalar

Defines `MirScalarExpr`, the central scalar expression type in MIR, and all
supporting machinery: the `EvalError` enum, type-inference and simplification
methods, and the complete scalar function library (via `func/`).

## Files (16,078 LOC total)

| File | What it owns |
|---|---|
| `func/` (12,392) | All scalar function enums and impls *(see `func/CONTEXT.md`)* |
| `scalar.rs` (3,426) | `MirScalarExpr` enum, `EvalError`, `eval`, type-inference, constant-folding/simplification |
| `like_pattern.rs` (≈260) | LIKE/ILIKE pattern compilation |

## Key concepts

- **`MirScalarExpr`** — recursive enum: `Column`, `Literal`, `CallUnmaterializable`,
  `CallUnary { func: UnaryFunc, expr }`, `CallBinary { func: BinaryFunc, exprs }`,
  `CallVariadic { func: VariadicFunc, exprs }`, `If { cond, then, els }`.
  Implements `VisitChildren` for the generic traversal infrastructure.
- **`EvalError`** — comprehensive runtime-error enum; includes variants for
  numeric overflow, parse failures, array shape violations, and
  `InvalidCatalogJson` for catalog deserialization errors.
- **Simplification** — `MirScalarExpr::reduce()` performs constant-folding,
  null-propagation, and limited algebraic rewrites. More aggressive rewrites
  live in `mz-transform`.
- **`TreatAsEqual<Option<Arc<str>>>`** on `Column` — carries an optional column
  name for diagnostic purposes; the `TreatAsEqual` wrapper ensures the name
  does not affect `Eq`/`Hash`/`Ord` so the optimizer cannot be confused by it.

## Bubbled in from func/

- `derive_unary!` / `derive_binary!` / `derive_variadic!` macros currently serve
  as a "temporary" replacement for `enum_dispatch`; see `func/ARCH_REVIEW.md`.
- Container-cast functions (`CastStringToList`, etc.) conservatively return
  `introduces_nulls = true` due to a crate-boundary constraint with
  `mz-sql::typeconv`; 6+ sites carry `TODO?` comments.

## Cross-references

- `MirRelationExpr` in `relation.rs` embeds `MirScalarExpr` in `Map`, `Filter`,
  `FlatMap`, `Reduce` predicates.
- `linear.rs` (`MapFilterProject`) composes scalar expressions into fused
  map-filter-project operators.
- `interpret.rs` abstract-interprets `MirScalarExpr` for filter-pushdown.
- `mz-transform` performs deeper scalar rewrites.
- Generated docs: `doc/developer/generated/expr/scalar/`.
