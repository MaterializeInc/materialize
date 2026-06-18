---
source: src/row-spine/src/lib.rs
revision: 31e0aab020
---

# mz-row-spine

Packed-bytes differential dataflow spine layouts for `Row`-valued arrangements. Keys and values are stored as concatenated bytes in a contiguous backing region (via `mz_ore::region::Region`, which uses lgalloc when available) rather than as separately-allocated heap objects, giving cursor lookups block locality and allowing the OS to evict cold pages cleanly under memory pressure.

## Public types

* `DatumContainer` — packed-bytes container for `Row` keys or values; implements `columnar::Container` and serves as the storage type for `Row`-valued spine layouts. Also implements `PushInto<&RowRef>`, pushing the raw bytes of a `RowRef` directly into the backing byte container.
* `DatumSeq<'a>` — borrowing view of a packed byte sequence, decoded datum-by-datum as `Datum`s; implements `ExtendDatums` and `PartialEq<&RowRef>` (comparing the underlying byte slices).
* `OffsetOptimized` — offset list implementation wrapping `differential_dataflow`'s `OffsetList`, used in `OrdValBatch` and `OrdKeyBatch` layouts.

## Spine type aliases

* `RowRowSpine<T, R>` — spine with `Row` keys and `Row` values.
* `RowValSpine<V, T, R>` — spine with `Row` keys and arbitrary `V` values.
* `RowSpine<T, R>` — spine with `Row` keys and `()` values.
* `ValRowSpine<K, T, R>` — spine with arbitrary `K` keys and `Row` values.

## Batcher type aliases

All batchers use `MergeBatcher` from `differential_dataflow` with `ColumnationChunker` and `ColInternalMerger`:

* `RowRowBatcher<T, R>`, `RowValBatcher<V, T, R>`, `RowBatcher<T, R>`, `ValRowBatcher<K, T, R>`

## Builder type aliases

All builders use `RcBuilder` wrapping the appropriate `OrdValBuilder` or `OrdKeyBuilder` with a `ColumnationStack` input:

* `RowRowBuilder<T, R>`, `RowValBuilder<V, T, R>`, `RowBuilder<T, R>`, `ValRowBuilder<K, T, R>`

`RowRowColPagedBuilder<T, R>` is a `RowRowBuilder` variant that consumes `Column` chunks instead of `ColumnationStack` input. It pairs with `Col2ValPagedBatcher` for the spillable arrange path.
`ValRowColPagedBuilder<K, T, R>` is a `ValRowBuilder` variant that consumes `Column` chunks; pairs with `Col2ValPagedBatcher<K, Row, T, R>` for the spillable arrange path where keys are arbitrary `Columnar` values and values are packed `Row` bytes.

## Layout structs (internal)

`RowRowLayout`, `RowValLayout`, `RowLayout`, and `ValRowLayout` implement `differential_dataflow::trace::implementations::Layout`, parameterizing the `OrdVal/KeyBatch` and `OrdVal/KeyBuilder` types with `DatumContainer` as the key/value storage and `OffsetOptimized` as the offset list.
