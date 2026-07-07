---
source: src/timely-util/src/operator.rs
revision: 12181a5639
---

# timely-util::operator

Extends timely streams and differential collections with higher-level operator combinators.
`StreamExt` adds `unary_fallible`, `flat_map_fallible` (splitting output into ok/err streams), and `expire_stream_at` (blocking frontier progress at a fixed time).
`CollectionExt` adds `map_fallible`, `flat_map_fallible`, `explode_one`, `ensure_monotonic`, `expire_collection_at`, `consolidate_named`, and `consolidate_named_if` to differential `Collection`s, along with an `empty` constructor.
`ConcatenateFlatten` merges multiple input streams while transforming containers through a specified `ContainerBuilder`, and `consolidate_pact` provides a generalized batcher-driven consolidation operator that accepts a chunker type parameter (`Chu: ContainerBuilder`) to stage raw input into the chunks the batcher consumes, and a custom parallelization contract. The fixed AHash seed for worker assignment is delegated to `crate::hash::fixed_state`.
