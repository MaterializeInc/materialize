---
source: src/expr/src/visit.rs
revision: fc2aaf02e7
---

# mz-expr::visit

Defines the `VisitChildren` and `Visit` traits for iterative traversal of expression trees.
`VisitChildren<T>` is implemented by a type to expose its direct children via `children()` and `children_mut()` iterators (returning `DoubleEndedIterator`); `Visit` is then automatically derived for all such types and provides pre-order, post-order, combined pre/post, and context-accumulating traversal methods in both fallible/infallible and mutable/immutable variants.
All traversals are iterative and do not use a recursion guard or stack-growth.
The `visit_children`, `visit_mut_children`, `try_visit_children`, and `try_visit_mut_children` methods on `VisitChildren` have default implementations in terms of `children()` and `children_mut()`, so implementors only need to provide the two iterator methods.
Mutable post-order traversals use unsafe code internally; it is critical that `VisitChildren::children_mut` implementations be written using safe code (no aliasing of children or access to parents).
