---
source: src/compute-types/src/plan/scalar.rs
revision: e926ec3a86
---

# compute-types::plan::scalar

Scalar expressions in a stable, serializable format for the LIR (Low-level IR) layer.

`LirScalarExpr` is the LIR counterpart of `MirScalarExpr`. It differs in two ways: it is a stable type written to durable storage, and it contains no unmaterializable functions (no `CallUnmaterializable`). Converting a `MirScalarExpr` that contains unmaterializable functions fails with a list of offending functions.

Key types:

* `LirScalarExpr` — enum with six variants: `Column(usize, TreatAsEqual<Option<Arc<str>>>)`, `Literal(Result<Row, EvalError>, ReprColumnType)`, `CallUnary`, `CallBinary`, `CallVariadic`, `If`. Implements `Eval`, `Columns`, `OptimizableExpr`, `VisitChildren`, `HumanizeDisplay`, and `ScalarOps`.

Key conversion functions:

* `mfp_mir_to_lir` — converts a `MapFilterProject<MirScalarExpr>` to `MapFilterProject<LirScalarExpr>`. Panics if any expression contains unmaterializable functions.
* `safe_mfp_mir_to_lir` — converts a `SafeMfpPlan<MirScalarExpr>` to `SafeMfpPlan<LirScalarExpr>`.
* `mfp_mir_to_lir_plan` — converts a `MapFilterProject<MirScalarExpr>` into an `MfpPlan<LirScalarExpr>`, extracting temporal bounds in the process.
* `mfp_plan_lir_to_mir` — converts an `MfpPlan<LirScalarExpr>` back to `MfpPlan<MirScalarExpr>` (always succeeds).
* `lses_from_mses` — bulk translation of a `&[MirScalarExpr]` to `Vec<LirScalarExpr>`. Panics on failure, which indicates a lowering bug.
