---
source: src/ore/src/pool/region.rs
revision: f2082d0163
---

# mz-ore::pool::region

Size-class virtual-memory regions for the buffer pool.

One `Region` per size class, each a single anonymous `mmap` reservation. The reservation is virtual; physical memory materializes on first write to a slot. Slots are scoped to residency: eviction releases a slot's physical pages with `MADV_DONTNEED` and returns the slot index to the free list, so a chunk holds a slot only from insert until its eviction. The pool reads slots strictly copy-out under the owning chunk's state lock.

`SIZE_CLASSES` lists the supported chunk size classes in bytes, smallest first. The pool places each chunk in the smallest class that fits its payload. The top classes deliberately overshoot the batchers' nominal ~2 MiB chunk target to accommodate multimodal real chunk sizes. Slot internal fragmentation is virtual-only: slots populate lazily.

Two fault-amortization mechanisms soften the cost of cycling slots:

- Regions whose class is at least one huge page are aligned to the huge page and advised `MADV_HUGEPAGE`, so populating a large slot costs one fault instead of one per 4 KiB.
- The free list is split into a warm side (pages kept resident; reuse faults nothing) and a cold side (pages released). The pool decides which side a freed slot joins, bounding total warm bytes as a fraction of its budget.

All platform access goes through a `sys` seam (mapping, unmapping, paging advice, page size). Under Miri the seam swaps to a Rust-heap backing with advice as a contents-preserving no-op, enabling pool tests to run under the interpreter.
