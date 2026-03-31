---
source: src/compute/src/extensions/reduce.rs
revision: c642b63c77
---

# mz-compute::extensions::reduce

Provides `MzReduce`, a wrapper around differential's `reduce_abelian` that ensures the output arrangement has `ArrangementSize` logging attached.
`ReduceExt` adds a `reduce_pair` combinator that applies two independent reduction logics to the same input arrangement and returns both output arrangements, used when a single operator needs to expose separate ok and error outputs.
