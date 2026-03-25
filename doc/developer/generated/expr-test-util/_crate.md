---
source: src/expr-test-util/src/lib.rs
revision: 52af3ba2a1
---

# mz-expr-test-util

Test utility crate providing helpers to construct `MirScalarExpr` and `MirRelationExpr` values from a compact text syntax via the `mz-lowertest` framework.
It exposes `build_scalar`, `build_rel`, and `json_to_spec` functions, plus a `TestCatalog` that maps source names to `GlobalId`s and type information for use in expression tests.
The deserialization contexts (`MirScalarExprDeserializeContext`, `MirRelationExprDeserializeContext`) extend the generic lowertest syntax with shorthand forms such as `#n` for column references and `(ok …)` / `(err …)` for literals.
