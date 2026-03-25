---
source: src/repr/src/row.rs
revision: ccd84bf8e8
---

# mz-repr::row

Defines `Row`, the fundamental unit of data in Materialize: a compact byte sequence of `Datum`-encoded values with an efficient packing API (`RowPacker`) and read access via `RowRef`.
`RowArena` provides a scoped allocator for `Datum`s that borrow from temporary storage (needed because `Datum<'a>` carries a lifetime); `DatumList` and `DatumMap` represent nested list and map datums.
`SharedRow` is a thread-local re-usable row buffer; the `encode` submodule handles Arrow columnar encoding and the `iter` submodule provides abstract row iteration traits.
