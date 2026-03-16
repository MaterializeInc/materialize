---
source: src/adapter/src/optimize/copy_to.rs
revision: 52af3ba2a1
---

# adapter::optimize::copy_to

Implements the optimizer pipeline for `COPY ... TO` statements as a two-stage `Optimize` impl: the first stage lowers HIR to MIR and applies MIR transformations; the second stage (`resolve`) resolves the timestamp and builds a `DataflowDescription<Plan>` ready to ship to a compute instance.
The optimizer wraps a `DataflowBuilder` and `ComputeInstanceSnapshot` to resolve index imports during dataflow construction.
