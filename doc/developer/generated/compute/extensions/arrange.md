---
source: src/compute/src/extensions/arrange.rs
revision: 0fe50ddc1b
---

# mz-compute::extensions::arrange

Provides `MzArrange` and `MzArrangeCore` extension traits that wrap differential dataflow's `arrange_core` and automatically attach an `ArrangementSize` logging operator to every arrangement.
The `ArrangementSize` trait and its implementations compute heap size, capacity, and allocation counts per batch; results are cached on first observation of each batch so that subsequent activations sum the cached values rather than re-walking each batch's backing regions. Log events (`ComputeEvent::ArrangementHeapSize*`) are emitted for introspection.
`KeyCollection` is a helper newtype for key-only (unit-value) collections, allowing them to flow through the same arrangement pipeline.
