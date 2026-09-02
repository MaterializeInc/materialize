# mz-compute-types

## LIR operators

This crate defines the LIR operator set (`LirRelationNode` and the render-facing `Expr`, in `src/plan.rs` and `src/plan/render_plan.rs`).
The renderer in `mz-compute` lowers each operator against a shared inter-operator interface, `CollectionBundle` (`src/compute/src/render/context.rs`).
The rules below are aspirational, and the current operator set does not fully meet them.

LIR operators should be irreducible with respect to that interface.
An operator should express one thing that the render boundary cannot decompose further, not bundle several.

LIR operators should resist bools or variants that alter their behavior.
The line to hold: a flag describing an optimization property of the operator itself (for example monotonicity, or whether output must be consolidated) is defensible.
A flag that makes an operator absorb another operator's responsibility, or that changes behavior in surprising ways, is not.
Several current variants already carry such flags (for example `Union.consolidate_output`, the `ArrangementStrategy` threaded through `Reduce` / `TopK` / `Union` / `ArrangeBy`, and `GetPlan`'s modes).
Scrutinize before adding more.
