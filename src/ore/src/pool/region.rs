// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Size-class virtual-memory regions for the buffer pool.
//!
//! One [`Region`] per size class, each a single anonymous `mmap` reservation.
//! The reservation is virtual; physical memory materializes on first write to
//! a slot. Slots are scoped to residency: eviction releases a slot's physical
//! pages with [`dontneed`] and returns the slot index to the free list, and a
//! fault-in allocates a fresh slot — possibly a different one — so a chunk's
//! address is stable only between a fault-in and the next eviction, and
//! pointers into a slot are valid only under the owning chunk's pin.

use std::io;
use std::sync::Mutex;

use crate::cast::CastFrom;

/// Chunk size classes in bytes, smallest first. The pool places each chunk in
/// the smallest class that fits its payload.
///
/// The top classes deliberately overshoot the batchers' nominal ~2 MiB chunk
/// target: the ship heuristic re-targets the next 2 MiB boundary whenever a
/// single push crosses one, so real chunk sizes are multimodal with bands
/// just under each boundary. A class that fits only the nominal target sends
/// the higher bands to the unpageable heap fallback. Slot internal
/// fragmentation is virtual-only — slots populate lazily, so a chunk costs
/// physical memory for its payload, not its class size.
pub(crate) const SIZE_CLASSES: [usize; 8] = [
    64 << 10,
    128 << 10,
    256 << 10,
    512 << 10,
    1 << 20,
    2 << 20,
    4 << 20,
    8 << 20,
];

/// One anonymous virtual-memory reservation serving fixed-size slots of a
/// single size class.
#[derive(Debug)]
pub(crate) struct Region {
    base: *mut u8,
    capacity: usize,
    class_size: usize,
    slots: Mutex<SlotAllocator>,
}

/// Free-list-plus-bump slot allocator. A slot index returns to the free list
/// whenever its chunk stops being resident — eviction and free alike.
#[derive(Debug)]
struct SlotAllocator {
    free: Vec<u32>,
    high_water: u32,
    max_slots: u32,
}

// SAFETY: `base` points at an anonymous mapping owned exclusively by this
// `Region` for its whole lifetime. Slot allocation is serialized by the
// `slots` mutex, and access to a slot's contents is serialized by the
// owning chunk's state mutex; the raw pointer itself carries no thread
// affinity.
unsafe impl Send for Region {}
// SAFETY: see the `Send` justification; all interior mutability is behind
// the `slots` mutex, and disjoint slots are written only by their owning
// chunks.
unsafe impl Sync for Region {}

impl Region {
    /// Reserves a region of `capacity_bytes` (rounded down to a whole number
    /// of slots) for slots of `class_size` bytes.
    pub(crate) fn new(class_size: usize, capacity_bytes: usize) -> io::Result<Region> {
        assert!(class_size > 0 && class_size % page_size() == 0);
        let capacity = capacity_bytes - capacity_bytes % class_size;
        if capacity == 0 {
            // A capacity below one slot yields an empty region: `alloc`
            // always answers `None` and the caller's exhaustion fallback
            // carries the class. No mapping exists; drop has nothing to do.
            return Ok(Region {
                base: std::ptr::NonNull::<u8>::dangling().as_ptr(),
                capacity: 0,
                class_size,
                slots: Mutex::new(SlotAllocator {
                    free: Vec::new(),
                    high_water: 0,
                    max_slots: 0,
                }),
            });
        }
        let max_slots = u32::try_from(capacity / class_size).expect("slot count fits u32");
        #[cfg(target_os = "linux")]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        // SAFETY: anonymous mapping with a null hint; the kernel picks a fresh
        // range that aliases no existing Rust object. `capacity` is positive
        // and page-aligned by construction.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Region {
            base: base.cast::<u8>(),
            capacity,
            class_size,
            slots: Mutex::new(SlotAllocator {
                free: Vec::new(),
                high_water: 0,
                max_slots,
            }),
        })
    }

    /// Size in bytes of every slot in this region.
    pub(crate) fn class_size(&self) -> usize {
        self.class_size
    }

    /// Allocates a slot index, or `None` if every slot of the class is in
    /// use. Slots are scoped to residency (eviction frees them), so demand
    /// scales with the *resident* set — bounded by the pool budget plus
    /// pinned and in-flight slack — and exhaustion means residency outgrew
    /// `class_capacity_bytes`; callers degrade rather than fail.
    pub(crate) fn alloc(&self) -> Option<u32> {
        let mut slots = self.slots.lock().expect("region allocator poisoned");
        if let Some(slot) = slots.free.pop() {
            return Some(slot);
        }
        if slots.high_water == slots.max_slots {
            return None;
        }
        let slot = slots.high_water;
        slots.high_water += 1;
        Some(slot)
    }

    /// Returns a slot to the free list. The caller must be freeing the chunk
    /// that owned the slot.
    pub(crate) fn free(&self, slot: u32) {
        let mut slots = self.slots.lock().expect("region allocator poisoned");
        debug_assert!(slot < slots.high_water);
        slots.free.push(slot);
    }

    /// Test hook: overwrites every free slot with `0xDE` so stale contents
    /// cannot masquerade as correct data when a slot is reused.
    pub(crate) fn poison_free_slots(&self) {
        let slots = self.slots.lock().expect("region allocator poisoned");
        for &slot in &slots.free {
            let offset = usize::cast_from(slot) * self.class_size;
            // SAFETY: the slot is on the free list and the allocator mutex is
            // held, so no chunk owns it and no allocation can race; the write
            // stays within the region's mapping.
            unsafe {
                std::ptr::write_bytes(self.base.add(offset), 0xDE, self.class_size);
            }
        }
    }

    /// The base address of a slot, fixed while its owning chunk is resident.
    pub(crate) fn slot_ptr(&self, slot: u32) -> *mut u8 {
        let offset = usize::cast_from(slot) * self.class_size;
        debug_assert!(offset + self.class_size <= self.capacity);
        // SAFETY: `slot` was handed out by `alloc`, so `offset + class_size`
        // lies within the single `capacity`-byte mapping that `base` points
        // to; the add stays in bounds of one allocated object.
        unsafe { self.base.add(offset) }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // Empty regions never created a mapping.
        if self.capacity == 0 {
            return;
        }
        // SAFETY: `base`/`capacity` describe exactly the mapping created in
        // `new`, and dropping the region means no chunk (and hence no
        // outstanding borrow) refers into it any longer.
        unsafe {
            libc::munmap(self.base.cast::<libc::c_void>(), self.capacity);
        }
    }
}

/// Releases the physical pages of the page-aligned subrange of
/// `[ptr, ptr + len)`, keeping the virtual range mapped.
///
/// # Safety
///
/// The range must lie within a live mapping exclusively owned by the caller,
/// with no outstanding references into it. After the call the range's contents
/// are undefined: Linux replaces them with zero pages, but other systems
/// (macOS in particular) may keep the old bytes resident, so callers must
/// fully overwrite the range before reading it again.
pub(crate) unsafe fn dontneed(ptr: *mut u8, len: usize) {
    madvise_aligned(ptr, len, libc::MADV_DONTNEED);
}

/// Hints the kernel to reclaim the page-aligned subrange of `[ptr, ptr + len)`
/// immediately, writing it to the swap device. Contents are preserved; this is
/// a non-destructive hint. No-op outside Linux.
#[cfg(target_os = "linux")]
pub(crate) fn pageout(ptr: *mut u8, len: usize) {
    madvise_aligned(ptr, len, libc::MADV_PAGEOUT);
}

/// See the Linux definition; reclaim hints have no portable equivalent.
#[cfg(not(target_os = "linux"))]
pub(crate) fn pageout(_ptr: *mut u8, _len: usize) {}

/// Hints the kernel to fault the page-aligned subrange of `[ptr, ptr + len)`
/// back in ahead of need: asynchronous swap-in, the swap-backed extent store's
/// readahead mechanism. Contents are preserved. No-op outside Linux.
#[cfg(target_os = "linux")]
pub(crate) fn willneed(ptr: *mut u8, len: usize) {
    madvise_aligned(ptr, len, libc::MADV_WILLNEED);
}

/// See the Linux definition; prefetch hints have no portable equivalent.
#[cfg(not(target_os = "linux"))]
pub(crate) fn willneed(_ptr: *mut u8, _len: usize) {}

/// Applies `advice` to the largest page-aligned subrange of `[ptr, ptr + len)`,
/// rounding the start up and the end down so the advice never spills onto
/// pages the range only partially covers.
fn madvise_aligned(ptr: *mut u8, len: usize, advice: libc::c_int) {
    if len == 0 {
        return;
    }
    let page = page_size();
    let base_addr = ptr.addr();
    let Some(start_unaligned) = base_addr.checked_add(page - 1) else {
        return;
    };
    let Some(end_unaligned) = base_addr.checked_add(len) else {
        return;
    };
    let aligned_start_addr = start_unaligned & !(page - 1);
    let aligned_end_addr = end_unaligned & !(page - 1);
    if aligned_end_addr <= aligned_start_addr {
        return;
    }
    let aligned_len = aligned_end_addr - aligned_start_addr;
    // SAFETY: `aligned_start_addr` lies in `[base_addr, base_addr + len]` by
    // construction (rounding the start up cannot exceed `end_unaligned`, and
    // the early return guarantees start < end), so `byte_add` stays within
    // the caller's range and preserves provenance.
    let aligned_ptr =
        unsafe { ptr.byte_add(aligned_start_addr - base_addr) }.cast::<libc::c_void>();
    // SAFETY: pointer and length describe a fully page-aligned subrange of the
    // caller's live mapping (justified above). Callers passing destructive
    // advice (`MADV_DONTNEED`) uphold the exclusivity contract documented on
    // `dontneed`; the remaining advice values are non-mutating hints.
    unsafe {
        libc::madvise(aligned_ptr, aligned_len, advice);
    }
}

pub(crate) fn page_size() -> usize {
    // SAFETY: `sysconf` with a valid argument is safe.
    let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(raw).expect("page size is positive and fits usize")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn alloc_free_reuses_slots() {
        let region = Region::new(64 << 10, 1 << 20).expect("mmap");
        let a = region.alloc().expect("slot");
        let b = region.alloc().expect("slot");
        assert_ne!(a, b);
        assert_ne!(region.slot_ptr(a), region.slot_ptr(b));
        let ptr_a = region.slot_ptr(a);
        region.free(a);
        let c = region.alloc().expect("slot");
        assert_eq!(c, a);
        assert_eq!(region.slot_ptr(c), ptr_a);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn exhaustion_returns_none() {
        let region = Region::new(64 << 10, 128 << 10).expect("mmap");
        assert!(region.alloc().is_some());
        assert!(region.alloc().is_some());
        assert!(region.alloc().is_none(), "third slot exceeds capacity");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn slots_are_writable_and_advice_is_accepted() {
        let region = Region::new(64 << 10, 1 << 20).expect("mmap");
        let slot = region.alloc().expect("slot");
        let ptr = region.slot_ptr(slot);
        // SAFETY: freshly allocated slot, exclusively owned by this test.
        unsafe {
            std::ptr::write_bytes(ptr, 0xAB, region.class_size());
        }
        pageout(ptr, region.class_size());
        willneed(ptr, region.class_size());
        // SAFETY: the slot is exclusively owned and is not read again before
        // being overwritten (it is not read again at all).
        unsafe {
            dontneed(ptr, region.class_size());
        }
    }
}
