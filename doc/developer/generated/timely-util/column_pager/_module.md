---
source: src/timely-util/src/column_pager.rs
revision: 7615b242e2
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

`ResidentTicket` is a drop guard that holds an `Arc<dyn PagingPolicy>` and the byte count charged at decide time. On drop it calls `metrics::observe_resident_released` and fires `PageEvent::ResidentReleased` so the policy can reclaim the budget it granted.

`ColumnPager` is cheap to clone (wraps an `Arc<dyn PagingPolicy>`). `ColumnPager::page` drains a `Column<C>` into a `PagedColumn<C>`, leaving the source as an empty typed default ready to be refilled. `ColumnPager::take` rehydrates a `PagedColumn<C>` back into a `Column<C>`, consuming the handle and reclaiming storage.

`ColumnPager::disabled` constructs a pager that never pages out: every `page` call returns a `PagedColumn::Resident` whose ticket discards release events. Useful as a placeholder before injecting a real policy. It is backed by `AlwaysResidentPolicy`, a private struct that always returns `PageDecision::Skip` and ignores events.

## Process-global pager

A `LazyLock<RwLock<ColumnPager>>` named `GLOBAL_PAGER` holds the process-wide active pager, defaulting to `ColumnPager::disabled` until `set_global_pager` is called.

`set_global_pager(pager)` installs a new process-wide pager. For the production path, `apply_tiered_config` is preferred: it reuses the singleton `TIERED_POLICY` so in-flight `ResidentTicket`s remain coherent with the running budget after reconfiguration.

`TIERED_POLICY` is a `LazyLock<Arc<policy::TieredPolicy>>` singleton initialized with zero budget. A singleton is required because every `ResidentTicket` holds an `Arc<dyn PagingPolicy>` pointing at the policy that granted residency; replacing the global policy would orphan in-flight tickets onto the old atomic, draining the new pool.

`tiered_policy()` returns a `&'static policy::TieredPolicy` reference to the singleton.

`apply_tiered_config(enabled, total_budget, backend, codec, swap_pageout)` calls `reconfigure` on the singleton policy, stores `swap_pageout` in the process-global `SWAP_PAGEOUT` atomic, and then either installs a `ColumnPager` backed by the policy (when `enabled`) or installs `ColumnPager::disabled`. When `SWAP_PAGEOUT` is set and the lz4 + swap path is active, `ColumnPager::page` calls `pager::advise_pageout` over the compressed bytes to proactively evict them from RSS.

`global_pager()` returns the current global pager by cloning the inner `Arc<dyn PagingPolicy>`.

## Pageout paths

- **Uncompressed, `Column::Align`**: the inner `Vec<u64>` is moved directly into the pager handle with no copy.
- **Uncompressed, other variants**: the column is serialized via `ContainerBytes::into_bytes`, then the byte buffer is widened to `Vec<u64>` and handed to the pager.
- **Compressed**: serialized bytes stream through an lz4 `FrameEncoder` directly into the output buffer; no intermediate uncompressed allocation is materialized.

The `metrics` submodule provides metric observation helpers called at each paging event. The `policy` submodule provides the concrete `TieredPolicy` implementation.
