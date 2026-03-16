---
source: src/expr/src/relation/canonicalize.rs
revision: 52af3ba2a1
---

# mz-expr::relation::canonicalize

Provides utility functions for transforming parts of a `MirRelationExpr` into a canonical form.
The primary entry point is `canonicalize_equivalences`, which simplifies join equivalence classes by reducing expressions to their minimal form (fewest non-literal nodes), applying constant folding, and deduplicating equivalence sets.
A secondary function `canonicalize_equivalence_classes` normalizes the structure of the equivalence class list itself (sorting and deduplicating).
