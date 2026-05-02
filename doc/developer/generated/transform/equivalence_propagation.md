---
source: src/transform/src/equivalence_propagation.rs
revision: 9eff685d95
---

# mz-transform::equivalence_propagation

Implements `EquivalencePropagation`, which uses the `Equivalences` bottom-up analysis to propagate expression equivalences both upward from leaves (enforced) and downward from roots (advised), simplifying scalar expressions throughout the plan.
Operators like `Filter`, `Map`, and `Join` contribute equivalences that are collected in `EquivalenceClasses`; in the downward pass these equivalences are used to substitute expressions with simpler equivalents where safe.
Care is taken not to introduce new rows by substitution during the downward pass, since the equivalences may themselves be caused by the structure of the expression being visited.
