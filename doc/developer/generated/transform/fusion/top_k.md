---
source: src/transform/src/fusion/top_k.rs
revision: 8d4d6772ea
---

# mz-transform::fusion::top_k

Implements `TopK` fusion: merges two consecutive `TopK` operators into one when they share the same grouping key and ordering key, by combining their limit and offset expressions. When fusing, `expected_group_size` on the outer operator is updated to the maximum of the two operators' hints; the inner operator's hint is not modified.
