# mz-transform

## Iterative tree traversal

Transforms walk `MirRelationExpr` and `MirScalarExpr` trees.
Deep trees can overflow the stack, so prefer iterative traversal over hand-written recursion.
This is aspirational: many transforms still recurse manually (for example `predicate_pushdown`, `join_implementation`, `equivalence_propagation`), and converting them is ongoing work.

Two mechanisms already exist and should be reached for before adding new recursion:

* The `Visit` trait in `mz-expr` (`src/expr/src/visit.rs`) is iterative, backed by an explicit heap work-stack.
  Its `visit_post` / `visit_mut_post` / `try_visit_mut_post` methods are the target shape for converting recursive traversals.
  Transforms like `fold_constants` and `normalize_lets` already use it.
* Where manual recursion stays, guard it.
  `mz_ore::stack` (`src/ore/src/stack.rs`) provides `maybe_grow` for stack growth and `RecursionGuard` / `CheckedRecursion` (`checked_recur`) for recursion limits.
  Most recursive transforms already wire these in.
  Unguarded recursion is a bug waiting to happen.
