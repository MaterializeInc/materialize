---
source: src/expr/src/relation/join_input_mapper.rs
revision: 703a0c27c8
---

# mz-expr::relation::join_input_mapper

Defines `JoinInputMapper`, a utility for translating column references between local (per-input) and global (post-join) coordinate systems.
Given the input arities of a join, it provides methods to map scalar expressions and column references between local and global contexts, locate which input a global column belongs to, and extract or negate the predicates that reference a subset of inputs.
Used extensively by join planning and optimization passes that need to reason about which inputs contribute which columns.
