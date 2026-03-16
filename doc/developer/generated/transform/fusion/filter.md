---
source: src/transform/src/fusion/filter.rs
revision: 52af3ba2a1
---

# mz-transform::fusion::filter

Implements `Filter` fusion: merges consecutive `Filter` operators into one, deduplicates predicates, removes literal-`true` predicates, and replaces filters with a literal-`false` or literal-error predicate with an empty constant.
Also calls `canonicalize_predicates` to bring the predicate list into a canonical order.
The `Filter::action` function is also called by `CanonicalizeMfp` when it rebuilds an MFP.
