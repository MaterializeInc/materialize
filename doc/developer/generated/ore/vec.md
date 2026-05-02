---
source: src/ore/src/vec.rs
revision: b9f601d6bf
---

# mz-ore::vec

Provides vector utilities and extension traits.
`repurpose_allocation` reinterprets a `Vec<T1>` allocation as `Vec<T2>` without reallocating, asserting equal size and alignment; this avoids redundant heap allocations when the element types are layout-compatible.
`Vector<T>` is a trait abstracting over `push` and `extend_from_slice`, implemented for `Vec<T>`, and optionally for `SmallVec` and `CompactBytes` behind feature flags.
`VecExt` adds `is_sorted_by` (a stable-Rust substitute for `slice::is_sorted_by`) and `PartialOrdVecExt` adds `is_sorted` and `is_strictly_sorted` for `PartialOrd` elements.
`swap_remove_multiple` removes elements at multiple indices from a vector using `swap_remove` and returns them.
