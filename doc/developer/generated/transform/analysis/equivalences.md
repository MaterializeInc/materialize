---
source: src/transform/src/analysis/equivalences.rs
revision: 52af3ba2a1
---

# mz-transform::analysis::equivalences

Provides the `Equivalences` analysis and the `EquivalenceClasses` type, which track sets of `MirScalarExpr` expressions that are guaranteed to evaluate to the same `Datum` for every record in a relation.
`EquivalenceClasses` are maintained as a lattice (with `None` representing a contradiction, as in an empty collection), and `Equivalences` implements `Analysis` so they can be derived bottom-up via the standard `DerivedBuilder` framework.
`ExpressionReducer` uses the equivalence information to simplify scalar expressions, and `EquivalenceClassesWithholdingErrors` is a variant that defers propagation of error literals.
