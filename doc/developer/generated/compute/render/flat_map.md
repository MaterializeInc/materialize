---
source: src/compute/src/render/flat_map.rs
revision: b0fa98e931
---

# mz-compute::render::flat_map

Renders `MirRelationExpr::FlatMap` nodes, which expand each input row into zero or more output rows by calling a `TableFunc` (e.g., `generate_series`, `jsonb_array_elements`).
Uses a fuel-based operator that processes a bounded number of input rows per activation to avoid starving other operators, buffering unprocessed input in a `VecDeque`.
An `MfpPlan` is applied after table function expansion to filter and project the output rows.
