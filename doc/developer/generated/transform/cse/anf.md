---
source: src/transform/src/cse/anf.rs
revision: cc1d7672ff
---

# mz-transform::cse::anf

Implements the `ANF` (Administrative Normal Form) transform, which converts a `MirRelationExpr` into ANF by binding every distinct sub-expression to a `Let` binding and replacing each occurrence with a `Get`.
The `Bindings` struct performs a post-order traversal, maintaining a map from expressions to fresh `LocalId`s, and handles `LetRec` scopes carefully by distinguishing "before" and "after" identifiers for each recursively-bound variable.
The resulting expression has an excess of `Let` nodes and is expected to be followed by `NormalizeLets`.
