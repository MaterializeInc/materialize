---
source: src/expr/src/visit.rs
revision: e757b4d11b
---

# mz-expr::visit

Defines the `VisitChildren` and `Visit` traits for stack-safe recursive traversal of expression trees.
`VisitChildren<T>` is implemented by a type to expose its direct children; `Visit` is then automatically derived for all such types and provides pre-order, post-order, combined pre/post, and context-accumulating traversal methods in both fallible/infallible and mutable/immutable variants.
All traversals are guarded by a `RecursionGuard` enforcing `RECURSION_LIMIT` and use `maybe_grow` to avoid stack overflows; deprecated `*_nolimit` variants exist for callers that predate the limit.
Also exposes `Visitor`, `VisitorMut`, `TryVisitor`, and `TryVisitorMut` object traits for callers that need to encapsulate traversal state.
