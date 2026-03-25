---
source: src/transform/src/redundant_join.rs
revision: cc1d7672ff
---

# mz-transform::redundant_join

Implements `RedundantJoin`, which removes join inputs that are collections of distinct elements when the join would only restrict results that are already guaranteed to appear in the other input.
This pattern arises frequently in decorrelated subqueries where a distinct collection is joined against query results and the join constraint is redundant.
The transform operates on a single redundant input per pass and is therefore placed inside a fixpoint loop.
