---
source: src/adapter/src/optimize/peek.rs
revision: 52af3ba2a1
---

# adapter::optimize::peek

Implements the optimizer pipeline for `SELECT` (peek) statements with three stages: local MIR optimization, global MIR optimization / fast-path detection, and LIR lowering.
After local optimization, the pipeline attempts to identify a `FastPathPlan` (direct arrangement read or constant result); if fast-path is not applicable it builds a full `DataflowDescription<Plan>` for the slow path.
`resolve` bridges the local and global stages by attaching the selected `TimestampContext`.
