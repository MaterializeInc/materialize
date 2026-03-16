---
source: src/transform/src/join_implementation.rs
revision: ddc1ff8d2d
---

# mz-transform::join_implementation

Implements `JoinImplementation`, which selects a concrete join execution strategy (differential, delta, or indexed-filter) for each `Join` operator and annotates it with the chosen `JoinImplementation` variant.
It uses `Cardinality` statistics and index availability (via `IndexOracle`) to order inputs, lift useful predicates, and convert filter+Get patterns into `IndexedFilter` semi-joins.
This transform is run in a tight fixpoint loop and must be preceded by `LiteralConstraints` and a `RelationCSE` with `inline_mfp = true`.
