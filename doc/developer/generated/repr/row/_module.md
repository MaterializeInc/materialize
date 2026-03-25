---
source: src/repr/src/row.rs
revision: fe91a762d1
---

# mz-repr::row

Defines `Row`, the fundamental unit of data in Materialize: a compact byte sequence of `Datum`-encoded values with an efficient packing API (`RowPacker`) and read access via `RowRef`.
`RowArena` provides a scoped allocator for `Datum`s that borrow from temporary storage (needed because `Datum<'a>` carries a lifetime).
`DatumList<'a, T>` is now generic over an element type parameter `T` (defaulting to `Datum<'a>`), enabling typed iteration through `DatumListTypedIter<'a, T>` while preserving backward compatibility for existing `DatumList<'a>` usage.
`DatumMap` represents nested map datums; `SharedRow` is a thread-local re-usable row buffer.
The `encode` submodule handles Arrow columnar encoding and the `iter` submodule provides abstract row iteration traits (`RowIterator`, `IntoRowIterator`).
