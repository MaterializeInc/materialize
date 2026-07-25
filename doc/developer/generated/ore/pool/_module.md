---
source: src/ore/src/pool.rs
revision: f2082d0163
---

# mz-ore::pool

Prototype buffer pool for dataflow state. See `doc/developer/design/20260610_buffer_managed_state.md` for the design.

The pool is a cache of size-class anonymous virtual-memory regions whose slots hold resident chunks. Slots are scoped to residency: eviction returns a chunk's slot to the free list along with its physical pages, so slot demand tracks the resident set (bounded by the budget), not the potentially unbounded live backlog. Reads are copy-out: a resident slot is copied and an evicted extent decompressed straight into the caller's buffer, all under the chunk's state lock. No reference into pool memory escapes the pool. The backing store for evicted chunks is the swap-backed extent arena in `extent`.

## Memory tiers

Memory descends a ladder of tiers, each with its own ceiling:

- **Slots** (uncompressed, free reads) — bounded by the budget; crossing it compresses the oldest chunks into extents and releases their slots.
- **Warm free slots** (pages kept for fault-free reuse) — bounded by the warm cap.
- **Compressed-resident extents** (reads decompress, no device I/O) — bounded by the headroom the RSS target leaves above the first two tiers.
- **Swap device** — overflow; reads fault and decompress.

## Key types

- `Pool` — the pool itself; insert chunks, evict, and query stats.
- `ChunkHandle` — a handle to an inserted chunk; dropping it frees the chunk.
- `ChunkHints` — advisory placement hints (generational depth) supplied at insert.
- `ExtentCodec` — trait for the encode/decode transform between body bytes and stored (compressed) bytes.
- `PoolStats` — snapshot of pool counters.
- `max_stored_len` — computes the maximum stored-form length for a given body length.

Submodules: `extent` (swap-backed extent arena), `region` (size-class virtual-memory regions).
