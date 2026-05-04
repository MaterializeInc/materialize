# expr::relation

Defines `MirRelationExpr`, the central relational expression type of MIR, and
all supporting relational types: aggregate/table/window functions, join
utilities, and expression normalization.

## Files (5,061 LOC total)

| File | What it owns |
|---|---|
| `func.rs` (3,918) | `AggregateFunc`, `TableFunc`, window frame types (`WindowFrame`, `WindowFrameBound`), `LagLead`, `FirstLastValue` |
| `join_input_mapper.rs` (580) | `JoinInputMapper` — index arithmetic for join column-offset remapping |
| `canonicalize.rs` (563) | `canonicalize_equivalences`, `canonicalize_equivalence_classes` — join equivalence normalization |

Plus `relation.rs` (4,470 LOC in parent dir) which is the primary `MirRelationExpr` definition file.

## Key concepts

- **`MirRelationExpr`** — large enum of relational operators: `Constant`, `Get`,
  `Let`, `LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `Join`, `Reduce`,
  `TopK`, `Negate`, `Threshold`, `Union`, `ArrangeBy`. Implements `VisitChildren`.
- **`MapFilterProject`** (defined in `relation.rs`, used by `linear.rs`) — fused
  operator combining `Map`, `Filter`, and `Project` into a single pass.
- **`RowSetFinishing`** — `ORDER BY` / `LIMIT` / `OFFSET` applied post-dataflow.
- **`RowComparator`** — reusable comparator that shares a `DatumVec` scratch
  buffer across calls to avoid re-allocation in tight sort loops.
- **`AggregateFunc`** — all aggregate functions (sum, count, min/max, array_agg,
  jsonb_agg, window rank/lag functions, etc.), evaluated over `Datum` iterators.
- **`TableFunc`** — set-returning functions (`generate_series`,
  `jsonb_array_elements`, `regexp_extract`, etc.).
- **`could_run_expensive_function`** — conservative predicate on
  `MirRelationExpr`; used by optimizer to guard costly transformations.
- **`RECURSION_LIMIT = 2048`** — set large to handle pathological `Let`-chain
  and `CallBinary` associative-operator patterns from SQL lowering.

## Cross-references

- `scalar/` provides `MirScalarExpr` embedded in `Map`, `Filter`, `FlatMap`,
  and `Reduce` predicates.
- `linear.rs` further fuses `MirRelationExpr` nodes into `MapFilterProject`.
- `mz-transform` optimizes `MirRelationExpr` trees.
- `mz-compute` executes `MirRelationExpr` as Timely dataflow.
- Generated docs: `doc/developer/generated/expr/relation/`.
