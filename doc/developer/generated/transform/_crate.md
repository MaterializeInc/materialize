---
source: src/transform/src/lib.rs
revision: de1872534e
---

# mz-transform

Provides semantics-preserving transformations of `MirRelationExpr` that improve query performance, organized as a collection of `Transform` implementors driven by an `Optimizer` sequencer.

## Module structure

* `analysis` — bottom-up lattice analyses (`Arity`, `Cardinality`, `NonNegative`, `UniqueKeys`, `Equivalences`, `Monotonic`, …) queried by other transforms.
* `canonicalization` — structural normalization: `ReduceScalars`, `FlatMapElimination`, `ProjectionExtraction`, `TopKElision`.
* `canonicalize_mfp` — MFP canonicalization and CSE.
* `case_literal` — rewrites If-chains matching a single expression against literals into `CaseLiteral` variadic functions with O(log n) BTreeMap lookup.
* `collect_notices` — collects optimizer notices that do not fit other transforms.
* `column_knowledge` — per-column literal/nullability propagation.
* `compound` — staging area for `UnionNegateFusion`.
* `cse` — common-subexpression elimination (`ANF`, `RelationCSE`).
* `dataflow` — whole-dataflow optimization (`optimize_dataflow`) and `DataflowMetainfo`.
* `demand` — column-demand propagation.
* `equivalence_propagation` — bi-directional equivalence class propagation.
* `fold_constants` — constant folding.
* `fusion` — operator fusion (Filter, Join, Map, Negate, Project, Reduce, TopK, Union).
* `join_implementation` — join strategy selection and `IndexedFilter` detection.
* `literal_constraints` — literal-equality to `IndexedFilter` conversion.
* `literal_lifting` — hoists literal scalars upward.
* `monotonic` — annotates `Reduce`/`TopK` with monotonicity flags.
* `movement` — `ProjectionLifting` and `ProjectionPushdown`.
* `non_null_requirements` — pushes non-null requirements toward sources.
* `normalize_lets` — normalizes `Let`/`LetRec` structure.
* `normalize_ops` — composite structural normalization pass.
* `notice` — optimizer notice types and `OptimizerNoticeApi` trait.
* `ordering` — reserved stub.
* `predicate_pushdown` — pushes filters toward sources.
* `reduce_elision` — removes redundant `Reduce` operators.
* `reduce_reduction` — splits multi-type `Reduce` into per-type reduces.
* `reduction_pushdown` — pushes `Reduce` through `Join`.
* `redundant_join` — removes redundant distinct-collection join inputs.
* `semijoin_idempotence` — removes repeated semijoins.
* `threshold_elision` — removes redundant `Threshold` operators.
* `typecheck` — type-correctness verification.
* `union_cancel` — cancels `A ∪ ¬A` branch pairs.
* `will_distinct` — propagates distinctness information downward.

## Key types

* `Transform` — the core trait all transforms implement.
* `TransformCtx` — shared context threading index/statistics oracles, optimizer features, dataflow metadata, and metrics through all transforms.
* `Optimizer` — sequences a list of `Transform`s; provides `logical_optimizer`, `physical_optimizer`, `logical_cleanup_pass`, `fast_path_optimizer`, and `constant_optimizer` factory methods.
* `Fixpoint` — iterates a list of transforms until the plan stabilizes or a limit is reached.
* `FuseAndCollapse` — a composite transform combining fusion and constant-folding steps.
* `TransformError` — error type for transform failures.
* `IndexOracle` / `StatisticsOracle` — traits for querying available indexes and cardinality estimates.

## Key dependencies

`mz-expr` (MIR types), `mz-repr` (scalar/type infrastructure), `mz-compute-types` (dataflow and reduction plan types), `mz-sql` (optimizer metrics).

## Downstream consumers

`mz-adapter` and `mz-compute` drive `optimize_dataflow` and the `Optimizer` pipeline during query planning and dataflow compilation.
