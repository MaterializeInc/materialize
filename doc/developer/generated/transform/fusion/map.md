---
source: src/transform/src/fusion/map.rs
revision: a5355b2e89
---

# mz-transform::fusion::map

Implements `Map` fusion: merges consecutive `Map` operators into one and removes empty `Map` operators.
The scalar expressions of later maps may reference columns produced by earlier maps, so they are adjusted during merging.
