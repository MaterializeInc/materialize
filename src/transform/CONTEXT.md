# transform (mz-transform)

MIR optimizer crate. Provides semantics-preserving transformations of
`MirRelationExpr` for query planning and dataflow compilation.

## Subtree (≈ 24,378 LOC total)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 23,397 | All optimizer passes, framework, analyses |

See `src/CONTEXT.md` for the full module-level breakdown.

## Package identity

Crate name: `mz-transform`. No features; dev-only dependencies include
`mz-expr-test-util` and `datadriven` for the `.spec`-driven test harness.

## Purpose

Single-crate MIR optimizer. Everything from structural normalization through
join strategy selection is implemented here as `Transform` implementors sequenced
by `Optimizer` and driven by `optimize_dataflow`.

## Key interfaces (exported)

- `optimize_dataflow(dataflow, ctx, fast_path)` — whole-dataflow entry point
- `Transform` trait — `actually_perform_transform` / `transform` / `name`
- `Optimizer` — named pipeline factory (`physical_optimizer`, `logical_cleanup_pass`, `fast_path_optimizer`, `constant_optimizer`; `logical_optimizer` deprecated)
- `TransformCtx` — shared context struct (`local` / `global` constructors)
- `IndexOracle` / `StatisticsOracle` — pluggable oracle traits
- `DataflowMetainfo` (via `dataflow` module) — side-channel for index usage and notices
- `catch_unwind_optimize` — panic → `TransformError` boundary helper

## Downstream consumers

`mz-adapter` (all `optimize/` sub-pipelines) and `mz-compute` call
`optimize_dataflow` and `Optimizer::optimize` during planning.

## Dependencies

`mz-expr` (MIR types), `mz-repr` (scalar/type/optimizer features), `mz-compute-types`
(dataflow plan types), `mz-sql` (optimizer metrics).

## Bubbled findings for src/CONTEXT.md

- **`Transform` trait surface**: `actually_perform_transform` naming is counter-intuitive; dead `debug()` method.
- **Deprecated entry still live**: `Optimizer::logical_optimizer` is `#[deprecated]` but still called from `mz-adapter/src/optimize.rs:346`.
- **`ordering.rs` stub**: empty pub module occupying crate namespace; either fill or delete.
- **`Fixpoint` oscillation replay**: O(n²) worst-case replay on hash collision for large plans.
- **Pipeline ordering by convention only**: ordering constraints between passes (e.g., `LiteralConstraints` before `JoinImplementation`, `NormalizeLets` last before rendering) are comments, not structural guarantees.
