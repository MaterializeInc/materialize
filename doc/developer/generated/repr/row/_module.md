---
source: src/repr/src/row.rs
revision: 1553727d21
---

# mz-repr::row

Defines `Row`, the fundamental unit of data in Materialize: a compact byte sequence of `Datum`-encoded values with an efficient packing API (`RowPacker`) and read access via `RowRef`.
`RowArena` provides a scoped allocator for `Datum`s that borrow from temporary storage (needed because `Datum<'a>` carries a lifetime). It is backed by a stack of byte regions used as a bump allocator: `push_bytes` copies bytes into the active region and returns a reference valid for the arena's lifetime; when the active region lacks spare capacity a new, larger region (doubling) is allocated so existing references remain valid. `push_bytes` accepts anything that derefs to `[u8]` (e.g. `Vec<u8>`, `&[u8]`). `with_capacity` sizes the initial region in bytes; `reserve` ensures the active region can hold at least the requested number of additional bytes. `clear` retains only the single largest region (emptied) to right-size the arena for reuse.
`DatumList<'a, T>` is generic over an element type parameter `T` (defaulting to `Datum<'a>`), enabling typed iteration through `DatumListTypedIter<'a, T>` while preserving backward compatibility for existing `DatumList<'a>` usage.
`DatumMap` represents nested map datums; `SharedRow` is a thread-local re-usable row buffer.
The `encode` submodule handles Arrow columnar encoding and the `iter` submodule provides abstract row iteration traits (`RowIterator`, `IntoRowIterator`).
