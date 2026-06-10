---
source: src/adapter/src/optimize/index.rs
revision: 52af3ba2a1
---

# adapter::optimize::index

Implements the optimizer pipeline for `CREATE INDEX` as a single `Optimize` stage that lowers HIR to MIR, applies MIR transforms, and lowers to a `DataflowDescription<Plan>`.
Timestamp selection is intentionally excluded from this pipeline (indexes have no `until` frontier and are computed during bootstrapping before full timestamp information is available); callers set `as_of` on the returned plan separately.
