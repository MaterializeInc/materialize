---
source: src/compute/src/render/errors.rs
revision: bf9d3f5f53
---

# mz-compute::render::errors

Defines `DataflowErrorSer`, a serialized byte representation of `DataflowError` used on compute-internal dataflow edges instead of `DataflowError` directly.
`DataflowErrorSer` is backed by proto-encoded `ProtoDataflowError` bytes; because proto3 + prost with no map fields produces deterministic encoding, byte-equality implies semantic equality, enabling `Ord`, `Hash`, and other derived traits directly on the bytes.
`DataflowErrorSer` implements `From<DataflowError>` and `From<EvalError>` for construction, and provides a `deserialize` method to recover a `DataflowError` at the boundary with code that expects `DataflowError` directly (e.g., storage operators).
`DataflowErrorSerRegion` provides a `Columnation` implementation for use in columnar arrangements.

Also defines the `MaybeValidatingRow` trait, a thin abstraction that lets rendering operators be generic over whether they validate and capture errors (`Result<Row, DataflowErrorSer>`) or pass rows through without error capture (`Row`).
Operators parameterized by `MaybeValidatingRow` can be instantiated in either mode, avoiding code duplication between the error-capturing and non-capturing paths.
