# transform::src

MIR (Midlevel Intermediate Representation) optimizer. Consumes `MirRelationExpr`
from `mz-expr`, applies ~35 semantics-preserving transforms, and produces an
optimized MIR ready for rendering. Drives both per-relation (logical/physical)
and whole-dataflow optimization.

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

## Key interfaces

- **`Transform` trait** — `actually_perform_transform(&self, relation, ctx)` (implement) + `transform(...)` (call; adds timing/metrics); `name() -> &'static str`.
- **`TransformCtx`** — threads `IndexOracle`, `StatisticsOracle`, `OptimizerFeatures`, `SharedTypecheckingContext`, `DataflowMetainfo`, `OptimizerMetrics`, and per-run hash cache through all passes. Two constructors: `local()` (no index/stats) and `global()`.
- **`Optimizer`** — sequences `Vec<Box<dyn Transform>>`; four named pipelines: `logical_optimizer` (deprecated entry), `physical_optimizer`, `logical_cleanup_pass`, `fast_path_optimizer`, `constant_optimizer`.
- **`Fixpoint`** — iterates until plan hash is stable, with loop/oscillation detection and a soft-panic escape.
- **`optimize_dataflow`** — whole-dataflow entry point: view inlining → logical → filter/demand cross-view → logical cleanup → physical → monotonic; also `optimize_dataflow_snapshot` for subscribe fast-path.
- **`Analysis` + `Lattice` traits** — bottom-up lattice framework for reusable, composable plan analyses.

## Downstream consumers

`mz-adapter` (`optimize/peek.rs`, `optimize/index.rs`, `optimize/materialized_view.rs`, `optimize/subscribe.rs`, `optimize/copy_to.rs`) and `mz-compute` call `optimize_dataflow` and/or `Optimizer::optimize` during planning.

## Cross-references

- `mz-expr` — `MirRelationExpr`, `MirScalarExpr`, `JoinImplementation`, `MapFilterProject`
- `mz-repr` — scalar/type infrastructure, `GlobalId`, `OptimizerFeatures`
- `mz-compute-types` — `DataflowDesc`, reduction plan types
- `mz-sql` — `OptimizerMetrics`
- Generated docs: `doc/developer/generated/transform/`
