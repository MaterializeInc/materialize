---
source: src/row-spine/src/lib.rs
revision: cc7f2656e3
---

# mz-row-spine

Packed-bytes differential dataflow spine layouts for `Row`-valued arrangements. Keys and values are stored as concatenated bytes in a contiguous backing region (via `mz_ore::region::Region`, which uses lgalloc when available) rather than as separately-allocated heap objects, giving cursor lookups block locality and allowing the OS to evict cold pages cleanly under memory pressure.

## Public types

* `DatumContainer` — packed-bytes container for `Row` keys or values; implements `columnar::Container` and serves as the storage type for `Row`-valued spine layouts.
* `DatumSeq<'a>` — borrowing view of a packed byte sequence, decoded datum-by-datum as `Datum`s; implements `ToDatumIter`.
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

## Layout structs (internal)

`RowRowLayout`, `RowValLayout`, `RowLayout`, and `ValRowLayout` implement `differential_dataflow::trace::implementations::Layout`, parameterizing the `OrdVal/KeyBatch` and `OrdVal/KeyBuilder` types with `DatumContainer` as the key/value storage and `OffsetOptimized` as the offset list.
