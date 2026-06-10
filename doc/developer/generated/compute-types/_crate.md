---
source: src/compute-types/src/lib.rs
revision: 88c883f3be
---

# compute-types

`mz-compute-types` provides shared type definitions used across the compute layer: dataflow descriptors, the LIR plan representation, sink/source descriptors, replica configuration, and dynamic configuration flags.

## Module structure

* `dataflows` — `DataflowDescription<P, S, T>`, the generic dataflow descriptor.
* `plan` — `Plan<T>` (the LIR node tree), lowering from MIR, `RenderPlan`, and sub-plans for joins, reductions, top-k, threshold, plus abstract-interpretation and transformation passes.
* `sinks` — `ComputeSinkDesc` and `ComputeSinkConnection` variants.
* `sources` — `SourceInstanceDesc` and `SourceInstanceArguments`.
* `config` — `ComputeReplicaConfig` and `ComputeReplicaLogging`.
* `dyncfgs` — all `dyncfg::Config` constants for the compute layer.
* `explain` — `Explain` impl for `DataflowDescription<Plan>` (text and JSON).

## Key dependencies

Depends on `mz-expr` (MIR expressions), `mz-repr` (row/scalar types, `GlobalId`), `mz-storage-types` (storage instance IDs, time dependence), `mz-dyncfg`, `differential-dataflow`, and `timely`.
Consumed by `mz-compute-client` (the compute protocol), `mz-compute` (the rendering engine), `mz-adapter` (plan lowering), and tooling.
