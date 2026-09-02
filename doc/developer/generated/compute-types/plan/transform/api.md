---
source: src/compute-types/src/plan/transform/api.rs
revision: e926ec3a86
---

# compute-types::plan::transform::api

Defines the `Transform` trait for mutating `LirRelationExpr` trees and `TransformConfig` (carrying a set of globally monotonic `GlobalId`s).
`Transform::transform` wraps `do_transform` with a tracing span for the optimizer; implementors override `do_transform`.
`BottomUpTransform` is a higher-level trait that drives a bottom-up traversal using `FoldMut` with an associated `Interpreter` and `action` callback; any `BottomUpTransform` automatically implements `Transform` via a blanket impl.
