---
source: src/row-spine/src/lib.rs
revision: 98ea0cc1cc
---

# mz-row-spine

Packed-bytes differential dataflow spine layouts for `Row`-valued arrangements. Keys and values are stored as concatenated bytes in a contiguous backing region (via `mz_ore::region::Region`, which uses lgalloc when available) rather than as separately-allocated heap objects, giving cursor lookups block locality and allowing the OS to evict cold pages cleanly under memory pressure.

## Public types

* `ArcBatch<B>` — a newtype around `Arc<B>` that carries differential's batch traits (`Batch`, `BatchReader`, etc.) so that `Arc`-backed batches can be used in spines without requiring upstream blanket impls. Batches wrapped in `ArcBatch` are `Send + Sync`, enabling them to be read from threads other than the worker that maintains the trace.
* `ArcBuilder<Bld>` — builder type pairing with `ArcBatch`-backed spines; wraps an inner builder and seals each completed batch into an `Arc`.
* `DatumContainer` — packed-bytes container for `Row` keys or values; implements `columnar::Container` and serves as the storage type for `Row`-valued spine layouts. Also implements `PushInto<&RowRef>`, pushing the raw bytes of a `RowRef` directly into the backing byte container.
* `DatumSeq<'a>` — borrowing view of a packed byte sequence, decoded datum-by-datum as `Datum`s; implements `ExtendDatums` and `PartialEq<&RowRef>` (comparing the underlying byte slices).
* `OffsetOptimized` — offset list implementation wrapping `differential_dataflow`'s `OffsetList`, used in `OrdValBatch` and `OrdKeyBatch` layouts.

## Spine type aliases

* `RowRowSpine<T, R>` — spine with `Row` keys and `Row` values.
* `RowValSpine<V, T, R>` — spine with `Row` keys and arbitrary `V` values.
* `RowSpine<T, R>` — spine with `Row` keys and `()` values.
* `ValRowSpine<K, T, R>` — spine with arbitrary `K` keys and `Row` values.
* `ArcOrdValSpine<K, V, T, R>` — generic `ArcBatch`-backed key/value spine for callers outside `mz_compute` that need an arrangement over non-`Row`-specialized types.
* `ArcOrdKeySpine<K, T, R>` — generic `ArcBatch`-backed key-only spine.

All production spines use `ArcBatch`-wrapped batches so that batch handles are `Send + Sync` and can be shared across threads.

## Batcher type aliases

All batchers use `MergeBatcher` from `differential_dataflow` with `ColumnationChunker` and `ColInternalMerger`:

* `RowRowBatcher<T, R>`, `RowValBatcher<V, T, R>`, `RowBatcher<T, R>`, `ValRowBatcher<K, T, R>`

## Builder type aliases

All builders use `ArcBuilder` wrapping the appropriate `OrdValBuilder` or `OrdKeyBuilder` with a `ColumnationStack` input:

* `RowRowBuilder<T, R>`, `RowValBuilder<V, T, R>`, `RowBuilder<T, R>`, `ValRowBuilder<K, T, R>`
* `ArcOrdValBuilder<K, V, T, R>` — builder pairing with `ArcOrdValSpine`.
* `ArcOrdKeyBuilder<K, T, R>` — builder pairing with `ArcOrdKeySpine`.

`RowRowColPagedBuilder<T, R>` is a `RowRowBuilder` variant that consumes `Column` chunks instead of `ColumnationStack` input. It pairs with `Col2ValPagedBatcher` for the spillable arrange path and installs a dictionary codec on both the key and value containers at seal time, gathering statistics from the sealed `Column` chain.
`ValRowColPagedBuilder<K, T, R>` is a `ValRowBuilder` variant that consumes `Column` chunks; pairs with `Col2ValPagedBatcher<K, Row, T, R>` for the spillable arrange path where keys are arbitrary `Columnar` values and values are packed `Row` bytes. It installs a dictionary codec on the value container at seal time; keys are not `Row`-shaped and stay uncompressed.

## Layout structs (internal)

`RowRowLayout`, `RowValLayout`, `RowLayout`, and `ValRowLayout` implement `differential_dataflow::trace::implementations::Layout`, parameterizing the `OrdVal/KeyBatch` and `OrdVal/KeyBuilder` types with `DatumContainer` as the key/value storage and `OffsetOptimized` as the offset list.
