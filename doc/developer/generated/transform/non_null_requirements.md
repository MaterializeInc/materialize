---
source: src/transform/src/non_null_requirements.rs
revision: cc1d7672ff
---

# mz-transform::non_null_requirements

Implements `NonNullRequirements`, which propagates non-null requirements derived from filter predicates downward toward sources, restricting constant collections to rows that satisfy the nullability constraints.
The primary effect is removing branches that introduce `NULL` rows (e.g., for outer-join decorrelation) when those rows would be filtered out by non-null-requiring predicates upstream.
