---
source: src/transform/src/cse.rs
revision: 82048ca795
---

# mz-transform::cse

Groups common-subexpression elimination (CSE) transforms.
Contains `anf` (ANF conversion, the algorithmic core) and `relation_cse` (the user-facing `RelationCSE` transform that combines ANF with `NormalizeLets`).
