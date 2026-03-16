---
source: src/compute-types/src/plan/transform/api.rs
revision: e757b4d11b
---

# compute-types::plan::transform::api

Defines the `Transform<T>` trait for mutating `Plan` trees and `TransformConfig` (currently carrying a set of globally monotonic `GlobalId`s).
`Transform::transform` wraps `do_transform` with optional tracing; implementors override `do_transform`.
`BottomUpTransform` is a blanket wrapper that drives a bottom-up traversal using `FoldMut` and an `Interpreter`, then applies a user-supplied mutation based on the inferred domain value.
