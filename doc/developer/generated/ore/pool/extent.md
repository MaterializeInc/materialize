---
source: src/ore/src/pool/extent.rs
revision: f2082d0163
---

# mz-ore::pool::extent

Swap-backed extents: the backing store for the buffer pool on nodes whose whole disk is provisioned as swap.

An extent is a slot in the pool-owned `ExtentArena` holding the stored (compressed) bytes of one chunk. Writing encodes into the slot. The slot stays resident as the compressed-but-resident middle tier until the pool's RSS target forces `SwapExtent::pageout`, which pushes pages to the swap device with `MADV_PAGEOUT`. Reading issues `MADV_WILLNEED` ahead of decode. Freeing returns the slot to the arena with pages discarded, dropping any swapped copy.

The arena exists so that extent pages never belong to the global allocator. `MADV_PAGEOUT` over allocator-owned memory leaves swap-entry PTEs on freed ranges, which the allocator can recycle into unrelated allocations that then major-fault on dead compressed data. Arena regions are advised `MADV_NOHUGEPAGE` once at map time so reclaim never splits a large folio, and slot recycling never re-touches swap. A class whose region is exhausted degrades to a plain heap allocation rather than failing.

Pageout is observed, never assumed: `MADV_PAGEOUT` may decline any page and still return success, so the page table decides whether the extent left memory. An extent stays fully resident for accounting until its entire range is unmapped, observed via pagemap present bits rather than `mincore` (which would count clean swap-cache copies as resident).

Key types: `ExtentArena`, `SwapExtent`, `Scratch`.
