# transform (mz-transform)

MIR optimizer crate. Provides semantics-preserving transformations of
`MirRelationExpr` for query planning and dataflow compilation.

## Module surface (LOC ≈ 23,397)

| Module | Purpose |
|---|---|
| `lib.rs` (1,031) | Core traits (`Transform`, `IndexOracle`, `StatisticsOracle`), `TransformCtx`, `Optimizer`, `Fixpoint`, `FuseAndCollapse` |
| `dataflow.rs` (1,373) | `optimize_dataflow` entry point — inline views, run logical/physical passes cross-view, demand/filter/monotonic propagation |
| `typecheck.rs` (2,158) | Type-correctness verification pass; runs as a sentinel before/after pipelines |
| `analysis.rs` (1,988) | Bottom-up lattice analyses framework (`Analysis` + `Lattice` traits); ships `Arity`, `Cardinality`, `NonNegative`, `UniqueKeys`, `Equivalences`, `Monotonic`, `ColumnNames`, `SubtreeSize` |
| `join_implementation.rs` (1,362) | Join strategy selection (differential / delta / `IndexedFilter`); the costliest physical decision |
| `predicate_pushdown.rs` (1,220) | Filter pushdown toward sources |
| `normalize_lets.rs` (1,044) | `Let`/`LetRec` structural normalization and CTE inlining |
| `equivalence_propagation.rs` (803) | Bi-directional equivalence class propagation |
| `column_knowledge.rs` (875) | Per-column literal/nullability propagation |
| `redundant_join.rs` (859) | Removes redundant join inputs using key analysis |
| `literal_constraints.rs` (771) | Literal equality → `IndexedFilter` conversion |
| `literal_lifting.rs` (770) | Hoists literal scalars upward |
| `fold_constants.rs` (647) | Constant folding |
| `semijoin_idempotence.rs` (591) | Removes repeated semijoins |
| `case_literal.rs` (589) | Rewrites If-chains into O(log n) BTreeMap `CaseLiteral` lookup |
| `reduction_pushdown.rs` (493) | Pushes `Reduce` through `Join` |
| `notice.rs` (442) | `RawOptimizerNotice` / `OptimizerNoticeApi` trait; 4 notice types |
| `fusion/` | Operator fusion: Filter, Join, Map, Negate, Project, Reduce, TopK, Union |
| `movement/` | `ProjectionLifting`, `ProjectionPushdown` |
| `cse/` | ANF, `RelationCSE` — common-subexpression elimination |
| `canonicalization/` | `ReduceScalars`, `FlatMapElimination`, `ProjectionExtraction`, `TopKElision` |
| `compound/` | `UnionNegateFusion` (staging area for complex compound transforms) |
| `analysis/equivalences.rs` (1,237) | Full equivalences analysis |
| `analysis/monotonic.rs` | Monotonicity analysis |
| `ordering.rs` | Reserved stub (empty body) |

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
