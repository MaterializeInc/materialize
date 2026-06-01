---
source: src/timely-util/src/column_pager.rs
revision: cc7f2656e3
---

# timely-util::column_pager

Column-aware pager that pages `Column<C>` instances out via `mz_ore::pager`, with optional lz4 frame-format compression.

The underlying pager (`mz_ore::pager`) deals in `Vec<u64>` blobs and two backends (Swap and File). This module adds three layers on top:

1. A `PagingPolicy` trait that decides _whether_ to page out, _which backend_, and _whether to compress_. Decisions live in the policy implementation rather than in a global atomic.
2. A `ColumnPager` that drains a `Column<C>` into a `PagedColumn<C>` and rehydrates it on demand.
3. Lz4 frame-format compression as an optional codec, streaming serialized bytes directly into the compressor with no intermediate uncompressed buffer.

The serialization uses the existing `ContainerBytes` protocol on `Column<C>`, so raw and compressed paths share a single byte layout.

## Key types

`Codec` is an enum with a single variant, `Lz4`, representing the lz4 frame format.

`PageHint` carries the uncompressed body size (matching `ContainerBytes::length_in_bytes`) and is passed to the policy when requesting a decision.

`PageDecision` is the policy's answer: `Skip` (keep resident) or `Page { backend, codec }` (spill using the given backend and optional codec).

`PageEvent` is a notification sent back to the policy after each operation — `PagedOut`, `PagedIn`, `Failed`, or `ResidentReleased` — allowing implementations to maintain metrics counters or adaptive accounting.

`PagingPolicy` is a `Send + Sync` trait with two methods: `decide(&self, PageHint) -> PageDecision` and `record(&self, PageEvent)`. Interior mutability is expected; a single policy instance can be shared across operator threads.

`PagedColumn<C>` is an enum with three variants:

- `Resident(Column<C>, ResidentTicket)` — body kept in memory; the ticket fires `PageEvent::ResidentReleased` on drop.
- `Paged { handle, meta }` — raw `ContainerBytes` payload stored via a `pager::Handle`.
- `Compressed { inner, meta }` — lz4-framed bytes held either in a resident `Vec<u8>` (`CompressedInner::Memory`) or in a pager handle (`CompressedInner::Paged`).

`ResidentTicket` is a drop guard that holds an `Arc<dyn PagingPolicy>` and the byte count charged at decide time. On drop it fires `PageEvent::ResidentReleased` so the policy can reclaim the budget it granted.

`ColumnPager` is cheap to clone (wraps an `Arc<dyn PagingPolicy>`). `ColumnPager::page` drains a `Column<C>` into a `PagedColumn<C>`, leaving the source as an empty typed default ready to be refilled. `ColumnPager::take` rehydrates a `PagedColumn<C>` back into a `Column<C>`, consuming the handle and reclaiming storage.

## Pageout paths

- **Uncompressed, `Column::Align`**: the inner `Vec<u64>` is moved directly into the pager handle with no copy.
- **Uncompressed, other variants**: the column is serialized via `ContainerBytes::into_bytes`, then the byte buffer is widened to `Vec<u64>` and handed to the pager.
- **Compressed**: serialized bytes stream through an lz4 `FrameEncoder` directly into the output buffer; no intermediate uncompressed allocation is materialized.

The `policy` submodule provides the concrete `TieredPolicy` implementation.
