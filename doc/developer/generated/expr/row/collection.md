---
source: src/expr/src/row/collection.rs
revision: 2982634c0d
---

# mz-expr::row::collection

Defines `RowCollection`, a compact representation of a sorted collection of `Row` values with per-row diff counts, and `RowCollectionIter`, its iterator.
`RowCollection` stores rows in a `Rows` buffer (from `mz-repr::update`) with a parallel `SharedSlice<NonZeroUsize>` for diffs; it supports incremental construction via `RowCollectionBuilder`, sorted construction with `new`, concatenation via `concat`, and multi-run merge-sort via `merge_sorted` (which uses `mz_ore::iter::merge_iters_by` and `RowComparator`).
`RowCollectionIter` implements the `RowIterator` protocol and supports lazy offset/limit application and column projection, making it suitable for serving `SELECT` result sets with `OFFSET`/`LIMIT`/`SELECT` column lists.
