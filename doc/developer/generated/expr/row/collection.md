---
source: src/expr/src/row/collection.rs
revision: eefc53999d
---

# mz-expr::row::collection

Defines `RowCollection`, a compact, contiguous-byte-blob representation of a sorted collection of `Row` values with per-row diff counts, and `RowCollectionIter`, its iterator.
`RowCollection` supports construction with a sort key (`ColumnOrder`), concatenation (`concat`), and multi-run merge-sort (`merge_sorted`); rows are stored as raw encoded bytes with an `Arc<[EncodedRowMetadata]>` index recording offsets and diffs.
`RowCollectionIter` implements the `RowIterator` protocol and supports lazy offset/limit application and column projection, making it suitable for serving `SELECT` result sets with `OFFSET`/`LIMIT`/`SELECT` column lists.
