---
source: src/expr-parser/src/catalog.rs
revision: 52af3ba2a1
---

# mz-expr-parser::catalog

Implements `TestCatalog`, an in-memory registry of named relations used during expression parsing tests.
Each registered object maps a name to a `GlobalId`, a column name list, and a `SqlRelationType`; transient objects (registered with `transient: true`) can be bulk-removed via `remove_transient_objects`, enabling per-test scoping.
`TestCatalog` implements `ExprHumanizer` so parsed `MirRelationExpr`s can be pretty-printed with human-readable names.
