---
source: src/compute/src/render/columnar.rs
revision: 4e5eafb239
---

# mz-compute::render::columnar

Columnar dataflow edge support.

Defines `CollectionEdge`, the columnar batch representation that dataflow edges between Plan nodes carry.

`ColumnarCollection` mirrors differential's `VecCollection` with `Column<(D, T, R)>` as the container instead of `Vec<(D, T, R)>`.

`CollectionEdge` is a type alias for `ColumnarCollection<'scope, T, Row, Diff>`. Every producer emits this columnar representation. Within a Plan node, operators may freely work with row-based (`Vec`) collections, but only the columnar edge format is used at node boundaries. A node that produces a row-based collection re-encodes it to the columnar edge via `vec_to_columnar`. A node that must consume rows decodes at its input leaf via `columnar_to_vec`. Both are named operators (`VecToColumnar`, `ColumnarToVec`) so those conversion seams stay visible in dataflow introspection.

`flat_map_datums` is the canonical entry point for operators that decode `mz_repr::Datum`s from each row; it iterates the columnar batch directly without materializing owned `Row` values.
