---
source: src/sql/src/plan/lowering/variadic_left.rs
revision: 52af3ba2a1
---

# mz-sql::plan::lowering::variadic_left

Implements `attempt_left_join_magic`, an optimization that tries to lower a contiguous stack of uncorrelated left joins as inner joins against key-augmented right relations.
For each right branch it introduces the missing keys (with null placeholders) and an indicator column, then produces a standard inner join, avoiding the more expensive general decorrelation path for this common pattern.
