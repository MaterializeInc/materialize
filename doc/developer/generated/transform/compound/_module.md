---
source: src/transform/src/compound.rs
revision: 82048ca795
---

# mz-transform::compound

A staging module for transforms that do not yet fit cleanly into `canonicalization`, `fusion`, `movement`, or `ordering`.
Currently contains a single child, `union`, which provides `UnionNegateFusion`.
