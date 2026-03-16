---
source: src/expr/src/interpret.rs
revision: c94586e255
---

# mz-expr::interpret

Implements an abstract interpreter for `MirScalarExpr` and `MapFilterProject` that propagates range/nullability specifications over datums.
The core types are `ResultSpec<'a>` (an overapproximation of the set of values an expression can produce, tracking nulls, errors, and a `Values` range) and `ColumnSpecs` (a per-column `ResultSpec` for a relation).
`Interpreter` and `MfpEval` provide the evaluation interface; `Trace` is a no-op interpreter used to determine which predicates are pushdownable (i.e., evaluable at the source).
This module drives filter-pushdown decisions in the explain and optimization pipelines.
