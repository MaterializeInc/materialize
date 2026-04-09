---
source: src/repr/src/update.rs
revision: 2982634c0d
---

# repr::update

Columnar data structures for representing batches of row-time-diff updates.

**`SharedSlice<T>`** — an immutable, reference-counted slice backed by an `Arc<[T]>` with a `Range<usize>` window. Supports `split_at` without reallocation, and implements `Deref<Target = [T]>`, `Serialize`, `Deserialize`, `PartialEq`, and `Default`.

**`Rows`** / **`RowsBuilder`** — a packed representation of multiple `Row` values stored contiguously as raw bytes (`bytes::Bytes`) with a `SharedSlice<usize>` of run-end offsets. Supports `get(index)`, `iter()`, `split_at(mid)`, `len()`, and `byte_len()`. Built incrementally via `RowsBuilder::push` / `build`.

**`UpdateCollection<T>`** / **`UpdateCollectionBuilder<T>`** — a columnar collection of `(row, time, diff)` triples stored as parallel arrays: `Rows` for row data, `SharedSlice<T>` for timestamps, and `SharedSlice<Diff>` for diffs. Supports `get(index)`, `iter()`, `split_at`, `times()`, `len()`, and `byte_len()`. Implements `FromIterator<(&RowRef, &T, Diff)>`. The default timestamp type is `mz_repr::Timestamp`.
