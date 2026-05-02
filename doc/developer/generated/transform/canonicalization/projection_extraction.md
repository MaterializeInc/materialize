---
source: src/transform/src/canonicalization/projection_extraction.rs
revision: da91c9a7f1
---

# mz-transform::canonicalization::projection_extraction

Implements `ProjectionExtraction`, which converts column-reference scalars in a `Map` operator into an explicit `Project`, and deduplicates repeated group keys or aggregates in a `Reduce` operator by appending a `Project`.
This canonical form makes downstream transformations easier to reason about because projections and scalar computations are separated.
