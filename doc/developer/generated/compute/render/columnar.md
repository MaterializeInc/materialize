---
source: src/compute/src/render/columnar.rs
revision: d43cd78803
---

# mz-compute::render::columnar

Columnar dataflow edge support.

Defines `CollectionEdge`, a wrapper that lets dataflow edges between Plan nodes carry either row-based (`VecCollection`) or columnar (`ColumnarCollection`) batches of `(D, T, R)` updates.

`ColumnarCollection` mirrors differential's `VecCollection` with `Column<(D, T, R)>` as the container instead of `Vec<(D, T, R)>`.

`CollectionEdge` is an enum with two variants:
- `Vec` — row-formatted collection; the current default for all producers.
- `Columnar` — columnar collection; reserved for producers once the migration completes.

The migration is consumer-first: every Plan-node consumer learns to accept both variants before any producer emits the columnar variant. Consumers that have not yet learned the columnar form fall back to `CollectionEdge::into_vec`, which decodes through a named `ColumnarToVec` operator. These repack seams remain visible in dataflow introspection so they can be found and retired. Pure passthrough consumers (Negate, Union) round-trip the columnar variant without decoding.
