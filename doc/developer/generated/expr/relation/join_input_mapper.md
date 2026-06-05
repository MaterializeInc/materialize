---
source: src/expr/src/relation/join_input_mapper.rs
revision: 5d046b3ab6
---

# mz-expr::relation::join_input_mapper

Defines `JoinInputMapper`, a utility for translating column references between local (per-input) and global (post-join) coordinate systems.
Given the input arities of a join, it provides methods to map scalar expressions and column references between local and global contexts, locate which input a global column belongs to, and extract or negate the predicates that reference a subset of inputs.
Several methods are now generic over `C: Columns` rather than taking concrete `MirScalarExpr`: `map_expr_to_local<C: Columns + Sized>`, `map_expr_to_global<C: Columns + Sized>`, `lookup_inputs<C: Columns>`, `single_input(&self, expr: &impl Columns)`, `is_localized(&self, expr: &impl Columns, index: usize)`, and `find_bound_expr<C: Columns + Clone + Eq>`.
Used extensively by join planning and optimization passes that need to reason about which inputs contribute which columns.
