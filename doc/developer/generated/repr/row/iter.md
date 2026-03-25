---
source: src/repr/src/row/iter.rs
revision: 4619e12b37
---

# mz-repr::row::iter

Defines `RowIterator` and `IntoRowIterator` traits for types that can produce streams of `&RowRef`, abstracting over both in-memory row collections and persist-backed row sources used by peek responses.
