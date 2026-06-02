---
source: src/sql/src/plan/lowering/variadic_left.rs
revision: be45c7c514
---

# mz-sql::plan::lowering::variadic_left

Implements `attempt_left_join_magic`, an optimization that tries to lower a contiguous stack of uncorrelated left joins as inner joins against key-augmented right relations.
For each right branch it introduces the missing keys (with null placeholders) and an indicator column, then produces a standard inner join, avoiding the more expensive general decorrelation path for this common pattern.
The optimization requires that the join condition consists exclusively of equations that cross from the left relation to the right relation; conditions that equate two columns within the same side (left-left or right-right) cause the optimization to bail out, since correctly handling such mixed equation sets is not yet implemented.
