---
source: src/compute/src/render/errors.rs
revision: e1944c9392
---

# mz-compute::render::errors

Defines the `MaybeValidatingRow` trait, a thin abstraction that lets rendering operators be generic over whether they validate and capture errors (`Result<Row, DataflowError>`) or pass rows through without error capture (`Row`).
Operators parameterized by `MaybeValidatingRow` can be instantiated in either mode, avoiding code duplication between the error-capturing and non-capturing paths.
