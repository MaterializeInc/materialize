---
source: src/walkabout/src/generated.rs
revision: e757b4d11b
---

# mz-walkabout::generated

Implements the three code-generation entry points exported by the crate: `gen_fold`, `gen_visit`, and `gen_visit_mut`.
Each function consumes an `Ir` and emits a Rust source string containing a trait (`Fold`/`Visit`/`VisitMut`), a node trait (`FoldNode`/`VisitNode`/`VisitMutNode`), per-type `impl` blocks, and free traversal functions.
The fold generator produces output type parameters with a `2`-suffix convention (e.g., `T` → `T2`) to express type-changing folds, while the visit generators borrow nodes by shared or mutable reference.
