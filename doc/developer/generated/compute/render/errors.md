---
source: src/compute/src/render/errors.rs
revision: 930b71af79
---

# mz-compute::render::errors

Defines `DataflowErrorSer`, a serialized byte representation of `DataflowError` used on compute-internal dataflow edges instead of `DataflowError` directly.
`DataflowErrorSer` is backed by proto-encoded `ProtoDataflowError` bytes; because proto3 + prost with no map fields produces deterministic encoding, byte-equality implies semantic equality, enabling `Ord`, `Hash`, and other derived traits directly on the bytes.
`DataflowErrorSer` implements `From<DataflowError>` and `From<EvalError>` for construction, and provides a `deserialize` method to recover a `DataflowError` at the boundary with code that expects `DataflowError` directly (e.g., storage operators).
`DataflowErrorSer` derives the `Columnar` trait and provides a manual `Columnation` implementation backed by `DataflowErrorSerRegion`, which delegates to the `Vec<u8>` columnation region. This allows `DataflowErrorSer` values to be stored in columnar arrangements. It also implements `correction_v2::DataBytes`, reporting `size_of::<Self>()` plus the length of its inner `Vec<u8>`, so the correction buffer can size its staging area by bytes when `DataflowErrorSer` is the update datum type.

Also defines the `MaybeValidatingRow` trait, a thin abstraction that lets rendering operators be generic over whether they validate and capture errors (`Result<Row, DataflowErrorSer>`) or pass rows through without error capture (`Row`).
Operators parameterized by `MaybeValidatingRow` can be instantiated in either mode, avoiding code duplication between the error-capturing and non-capturing paths.
The module also defines `ErrorLogger`, a helper for rendering code that logs dataflow errors with a format optimized for Sentry triage: a static message string at ERROR level (for grouping) and dynamic details at WARN level (for breadcrumbs).
