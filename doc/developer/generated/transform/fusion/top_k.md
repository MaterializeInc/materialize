---
source: src/transform/src/fusion/top_k.rs
revision: 52af3ba2a1
---

# mz-transform::fusion::top_k

Implements `TopK` fusion: merges two consecutive `TopK` operators into one when they share the same grouping key and ordering key, by combining their limit and offset expressions.
