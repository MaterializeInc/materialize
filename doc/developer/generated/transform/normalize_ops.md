---
source: src/transform/src/normalize_ops.rs
revision: 69459d743b
---

# mz-transform::normalize_ops

Implements `NormalizeOps`, a composite transform that applies a fixed sequence of structural normalizations bottom-up in a single pass: `FlatMapElimination`, `TopKElision`, `Fusion`, `Join` fusion, and `ProjectionExtraction`.
It is used as the structural normalization half of the `normalize()` fixpoint alongside `NormalizeLets`.
