---
source: src/ore/src/pager/swap.rs
revision: 7615b242e2
---

# mz-ore::pager::swap

Swap backend for `mz_ore::pager`. Keeps paged-out data resident in memory while hinting to the kernel that the pages are cold and eligible for reclaim.

## SwapInner

`SwapInner` (crate-internal) holds the paged-out chunks and a prefix-sum array for efficient range indexing:

* `chunks: Vec<Vec<u64>>` — the moved-in chunk allocations; logical layout is their concatenation in order.
* `prefix: Vec<usize>` — cumulative element counts, where `prefix[0] == 0` and `prefix.last()` equals the total number of `u64`s. Used for O(log n) binary-search lookup of the starting chunk for a given offset.

## Operations

* `pageout_swap` — moves each chunk out of the caller's slice via `mem::take`, calls `madvise_cold` on the now-owned data, and wraps the chunks in a `SwapInner`.
* `read_at_swap` — binary-searches `prefix` to find the chunk containing a requested offset, then copies elements across chunk boundaries into `dst`.
* `take_swap` — single-chunk zero-copy fast path: if there is exactly one chunk and `dst` is empty, swaps the chunk directly into `dst`. Multi-chunk: concatenates all chunks into `dst`.

## MADV_COLD and MADV_PAGEOUT

On Linux, `madvise_cold` computes the page-aligned subrange of each chunk and calls `madvise(MADV_COLD)`, signaling to the kernel that these pages are low-priority for memory reclaim. On non-Linux platforms, `madvise_cold` is a no-op.

`advise_pageout(bytes)` issues `madvise(MADV_PAGEOUT)` over the page-aligned interior of the given byte slice, proactively swapping the pages out right now rather than waiting for kernel LRU pressure. Unlike `pageout_swap`, it does not transfer ownership: the allocation remains addressable in the caller's address space and re-faults on the next access. On non-Linux targets it is a no-op.

Both helpers share a private `madvise_aligned` helper that rounds the start address up and the end address down to page boundaries, skipping the call when no whole page is covered.
