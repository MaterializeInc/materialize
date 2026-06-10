---
source: src/ore/src/region.rs
revision: e757b4d11b
---

# mz-ore::region

Defines low-level, element-drop-skipping memory regions backed by either the heap or `lgalloc` file-mapped memory.
`Region<T>` is an enum with `Heap(Vec<T>)` and `MMap(MMapRegion<T>)` variants; it deref-slices to `[T]`, never drops its elements (callers must ensure `T: Copy` or manage drops externally), and forwards to `lgalloc::deallocate` on drop for the mmap case.
`LgAllocRegion<T>` layers `Region<T>` into a bump-style arena that stashes full allocations and doubles capacity on growth, providing `copy_iter`, `copy_slice`, and `reserve` for stable-address bulk copies.
The `ENABLE_LGALLOC_REGION` global flag gates whether `Region::new_auto` attempts an mmap allocation before falling back to heap.
