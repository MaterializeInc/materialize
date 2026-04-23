---
source: src/transform/src/predicate_pushdown.rs
revision: 09f8729224
---

# mz-transform::predicate_pushdown

Implements `PredicatePushdown`, which pushes `Filter` predicates down through other operators toward sources to reduce data volume early.
Predicates are pushed through `Map`, `Project`, `Union`, `Join`, `Reduce`, and `Let`/`Get` pairs, with care taken not to push literal errors unless they will be unconditionally evaluated.
As a side effect, pushdown into `Join` operators sets the join's equivalence classes, which are used by `EquivalencePropagation` and `JoinImplementation`.
