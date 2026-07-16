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
//! pages with [`dontneed`] and returns the slot index to the free list, so a
//! chunk holds a slot only from insert until its eviction, and the pool reads
//! slots strictly copy-out under the owning chunk's state lock.
//!
//! Two fault-amortization mechanisms soften the cost of cycling slots:
//!
//! * Regions whose class is at least one huge page are aligned to the huge
//!   page and advised `MADV_HUGEPAGE`, so populating a large slot costs one
//!   fault instead of one per 4 KiB.
//! * The free list is split into a *warm* side (pages kept resident; reuse
//!   faults nothing and skips the kernel's page zeroing) and a *cold* side
//!   (pages released). The pool decides which side a freed slot joins,
//!   bounding total warm bytes as a fraction of its budget.
//!
//! TODO: consider `mlock`ing slot regions so kernel swap can never write
//! out pages the engine would discard or write better itself. Needs
//! `RLIMIT_MEMLOCK` tuning before it can be on by default.
//!
//! All platform access goes through the [`sys`] seam: mapping, unmapping,
//! paging advice, and the page size. Under Miri the seam swaps to a
//! Rust-heap backing with advice as a contents-preserving no-op, so the
//! pool's tests (including the unsafe slot borrows they exercise) run under
//! the interpreter.

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

/// The smallest size class that fits a payload of `len_bytes`, or `None`
/// when even the largest class is too small. A selected class always fits:
/// `SIZE_CLASSES[class] >= len_bytes`, the bound the pool turns into slice
/// lengths over slot memory (proved by the Kani harnesses).
pub(crate) fn size_class_for(len_bytes: usize) -> Option<usize> {
    SIZE_CLASSES.iter().position(|&c| c >= len_bytes)
}

/// One anonymous virtual-memory reservation serving fixed-size slots of a
/// single size class.
#[derive(Debug)]
pub(crate) struct Region {
    base: *mut u8,
    capacity: usize,
    /// Alignment of `base`, needed to return the mapping to [`sys::unmap`].
    align: usize,
    class_size: usize,
    slots: Mutex<SlotAllocator>,
}

/// Free-list-plus-bump slot allocator. A slot index returns to a free list
/// whenever its chunk stops being resident — eviction and free alike. Warm
/// slots keep their physical pages (reuse is fault-free); cold slots had
/// theirs released. Never-allocated slots beyond the high-water mark are
/// untouched virtual space and fault on first write like cold ones.
#[derive(Debug)]
struct SlotAllocator {
    free_warm: Vec<u32>,
    free_cold: Vec<u32>,
    high_water: u32,
    max_slots: u32,
}

impl SlotAllocator {
    fn new(max_slots: u32) -> SlotAllocator {
        SlotAllocator {
            free_warm: Vec::new(),
            free_cold: Vec::new(),
            high_water: 0,
            max_slots,
        }
    }

    /// Allocates a slot index, or `None` when every slot is in use; the flag
    /// reports whether the slot came from the warm list. Warm slots are
    /// preferred, then cold, then never-touched bump slots.
    fn alloc(&mut self) -> Option<(u32, bool)> {
        if let Some(slot) = self.free_warm.pop() {
            return Some((slot, true));
        }
        if let Some(slot) = self.free_cold.pop() {
            return Some((slot, false));
        }
        if self.high_water == self.max_slots {
            return None;
        }
        let slot = self.high_water;
        self.high_water += 1;
        Some((slot, false))
    }

    /// Returns a previously allocated slot to the warm or cold free list.
    fn free(&mut self, slot: u32, warm: bool) {
        debug_assert!(slot < self.high_water);
        if warm {
            self.free_warm.push(slot);
        } else {
            self.free_cold.push(slot);
        }
    }
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
    ///
    /// On Linux, regions whose class is at least [`HUGE_PAGE`] are aligned to
    /// the huge page and advised `MADV_HUGEPAGE`: their slots tile huge-page
    /// boundaries exactly, so populating a slot is one huge-page fault rather
    /// than one fault per 4 KiB, and a whole-slot [`dontneed`] frees whole
    /// huge pages without splitting any. Regions whose class is below the
    /// huge page are advised `MADV_NOHUGEPAGE` instead, so reclaim over a
    /// single slot stays page-exact.
    pub(crate) fn new(class_size: usize, capacity_bytes: usize) -> io::Result<Region> {
        Region::with_huge_pages(class_size, capacity_bytes, class_size >= HUGE_PAGE)
    }

    /// As [`Region::new`], but the region opts out of transparent huge pages
    /// regardless of class size. For regions whose slots are reclaimed with
    /// `MADV_PAGEOUT` under observed residency: reclaiming a range a large
    /// folio covers only partially needs a folio split, and a failed split
    /// silently leaves pages resident.
    pub(crate) fn new_nohuge(class_size: usize, capacity_bytes: usize) -> io::Result<Region> {
        Region::with_huge_pages(class_size, capacity_bytes, false)
    }

    fn with_huge_pages(class_size: usize, capacity_bytes: usize, huge: bool) -> io::Result<Region> {
        let page = sys::page_size();
        assert!(class_size > 0 && class_size % page == 0);
        let capacity = capacity_bytes - capacity_bytes % class_size;
        if capacity == 0 {
            // A capacity below one slot yields an empty region: `alloc`
            // always answers `None` and the caller's exhaustion fallback
            // carries the class. No mapping exists; drop has nothing to do.
            return Ok(Region {
                base: std::ptr::NonNull::<u8>::dangling().as_ptr(),
                capacity: 0,
                align: page,
                class_size,
                slots: Mutex::new(SlotAllocator::new(0)),
            });
        }
        let max_slots = u32::try_from(capacity / class_size).expect("slot count fits u32");
        let align = if cfg!(target_os = "linux") && huge {
            HUGE_PAGE
        } else {
            page
        };
        let base = sys::map(capacity, align)?;
        if !huge {
            // Opt the whole region out of transparent huge pages before any
            // slot is touched. Production hosts run
            // `transparent_hugepage=madvise`, where an unadvised region gets
            // no fault-time folios anyway, so this is defense in depth for
            // hosts running `always`: there, fault-time folios would straddle
            // sub-huge-page slot boundaries, and khugepaged (which tolerates
            // up to `max_ptes_none` empty entries) re-collapses ranges a
            // slot-granular [`dontneed`] just reclaimed, resurrecting the
            // released memory.
            nohugepage(base, capacity);
        }
        // Pool contents are recreatable spill data: keep the (potentially
        // huge) anonymous reservation out of core dumps.
        dontdump(base, capacity);
        Ok(Region {
            base,
            capacity,
            align,
            class_size,
            slots: Mutex::new(SlotAllocator::new(max_slots)),
        })
    }

    /// Size in bytes of every slot in this region.
    pub(crate) fn class_size(&self) -> usize {
        self.class_size
    }

    /// Allocates a slot index, or `None` if every slot of the class is in
    /// use; the flag reports whether the slot came from the warm list (its
    /// pages are resident; writing it faults nothing).
    ///
    /// Slots are scoped to residency (eviction frees them), so demand scales
    /// with the *resident* set — bounded by the pool budget plus in-flight
    /// slack — and exhaustion means residency outgrew the class reservation;
    /// callers degrade rather than fail.
    pub(crate) fn alloc(&self) -> Option<(u32, bool)> {
        self.slots
            .lock()
            .expect("region allocator poisoned")
            .alloc()
    }

    /// Returns a slot to the warm or cold free list. The caller must be
    /// freeing the chunk that owned the slot, must have released the slot's
    /// physical pages iff `warm` is false, and owns the warm-bytes accounting
    /// that bounds the warm side.
    pub(crate) fn free(&self, slot: u32, warm: bool) {
        self.slots
            .lock()
            .expect("region allocator poisoned")
            .free(slot, warm);
    }

    /// Moves warm free slots to the cold list, releasing their physical
    /// pages, until at least `want_bytes` have been cooled or no warm slot
    /// remains. Returns the bytes cooled. The caller owns the warm-bytes
    /// accounting that bounds the warm side.
    pub(crate) fn cool_warm_slots(&self, want_bytes: usize) -> usize {
        let mut slots = self.slots.lock().expect("region allocator poisoned");
        let mut cooled = 0;
        while cooled < want_bytes {
            let Some(slot) = slots.free_warm.pop() else {
                break;
            };
            let offset = usize::cast_from(slot) * self.class_size;
            // SAFETY: the slot is on a free list and the allocator mutex is
            // held, so no chunk owns it and no allocation can race; the
            // range stays within the region's mapping.
            unsafe {
                dontneed(self.base.add(offset), self.class_size);
            }
            slots.free_cold.push(slot);
            cooled += self.class_size;
        }
        cooled
    }

    /// Test hook: overwrites every free slot with `0xDE` so stale contents
    /// cannot masquerade as correct data when a slot is reused.
    #[cfg(test)]
    pub(crate) fn poison_free_slots(&self) {
        let slots = self.slots.lock().expect("region allocator poisoned");
        for &slot in slots.free_warm.iter().chain(slots.free_cold.iter()) {
            let offset = usize::cast_from(slot) * self.class_size;
            // SAFETY: the slot is on a free list and the allocator mutex is
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
        // SAFETY: `base`/`capacity`/`align` describe exactly the mapping
        // created in `new`, and dropping the region means no chunk (and
        // hence no outstanding borrow) refers into it any longer.
        unsafe {
            sys::unmap(self.base, self.capacity, self.align);
        }
    }
}

/// The transparent-huge-page size assumed for region alignment. Linux x86-64
/// and aarch64 (4 KiB base pages) both use 2 MiB; if a platform differs, the
/// alignment is merely unhelpful, never wrong.
pub(crate) const HUGE_PAGE: usize = 2 << 20;

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
    // SAFETY: forwarding this function's contract.
    unsafe { sys::advise(ptr, len, sys::Advice::DontNeed) }
}

/// Hints the kernel to reclaim the page-aligned subrange of `[ptr, ptr + len)`
/// immediately, writing it to the swap device. Contents are preserved; this is
/// a non-destructive hint. No-op outside Linux.
pub(crate) fn pageout(ptr: *mut u8, len: usize) {
    // SAFETY: the advice is a contents-preserving hint, so the only
    // obligation is that the range lies in a live mapping, which callers
    // guarantee by passing a live slot or extent allocation.
    unsafe { sys::advise(ptr, len, sys::Advice::PageOut) }
}

/// Hints the kernel to fault the page-aligned subrange of `[ptr, ptr + len)`
/// back in ahead of need: asynchronous swap-in, the swap-backed extent store's
/// readahead mechanism. Contents are preserved. No-op outside Linux.
pub(crate) fn willneed(ptr: *mut u8, len: usize) {
    // SAFETY: as in `pageout`, a contents-preserving hint over a live
    // mapping.
    unsafe { sys::advise(ptr, len, sys::Advice::WillNeed) }
}

/// Opts the page-aligned subrange of `[ptr, ptr + len)` out of transparent
/// huge pages, so reclaim advice over the range operates on base pages and
/// never needs a folio split. Contents are preserved. No-op outside Linux.
pub(crate) fn nohugepage(ptr: *mut u8, len: usize) {
    // SAFETY: as in `pageout`, a contents-preserving hint over a live
    // mapping.
    unsafe { sys::advise(ptr, len, sys::Advice::NoHugePage) }
}

/// Excludes the page-aligned subrange of `[ptr, ptr + len)` from core dumps.
/// Contents are preserved. No-op outside Linux.
pub(crate) fn dontdump(ptr: *mut u8, len: usize) {
    // SAFETY: as in `pageout`, a contents-preserving hint over a live
    // mapping.
    unsafe { sys::advise(ptr, len, sys::Advice::DontDump) }
}

/// The system page size.
pub(crate) fn page_size() -> usize {
    sys::page_size()
}

/// Whether every page of the page-aligned subrange of `[ptr, ptr + len)` is
/// out of memory, per `mincore(2)`: the observation the pageout ledger
/// trusts instead of the reclaim advice's return value. Errs toward `false`
/// (resident) when the observation is unavailable. In test builds the
/// answer comes from [`fake_residency`] instead of the platform.
pub(crate) fn nonresident(ptr: *mut u8, len: usize) -> bool {
    #[cfg(test)]
    {
        let _ = (ptr, len);
        fake_residency::observe()
    }
    #[cfg(not(test))]
    sys::nonresident(ptr, len)
}

/// Test seam over the pageout residency observation. The decline queue is
/// thread-local because observation runs on whichever thread enforces the
/// compressed cap, so tests drive enforcement inline on their own thread.
#[cfg(test)]
pub(crate) mod fake_residency {
    use std::cell::Cell;

    thread_local! {
        static DECLINES: Cell<u64> = const { Cell::new(0) };
    }

    /// Makes the next `n` observations on this thread report pages still
    /// resident, modeling a kernel that declined the reclaim advice.
    /// Replaces any previously queued declines.
    pub(crate) fn decline_next(n: u64) {
        DECLINES.with(|d| d.set(n));
    }

    /// One observation: consumes a queued decline (reporting the range
    /// still resident), or reports it fully nonresident.
    pub(super) fn observe() -> bool {
        DECLINES.with(|d| {
            let n = d.get();
            if n > 0 {
                d.set(n - 1);
                false
            } else {
                true
            }
        })
    }
}

/// The largest `page`-aligned subrange of `[addr, addr + len)`, as a
/// `(byte offset from addr, subrange length)` pair, or `None` when the range
/// covers no whole page (including on address-space overflow). `page` must
/// be a power of two.
///
/// Guarantees, relied on by [`sys::advise`] for pointer arithmetic and
/// proved by the Kani harnesses: `offset <= len`, `offset + sub_len <= len`,
/// and both `addr + offset` and `sub_len` are `page`-aligned.
#[cfg_attr(miri, allow(dead_code))]
fn aligned_subrange(addr: usize, len: usize, page: usize) -> Option<(usize, usize)> {
    debug_assert!(page.is_power_of_two());
    let start = addr.checked_add(page - 1)? & !(page - 1);
    let end = addr.checked_add(len)? & !(page - 1);
    (start < end).then(|| (start - addr, end - start))
}

/// Splits an over-mapped range of `map_len` bytes at `addr` into
/// `(head, tail)` trim amounts such that discarding `head` bytes from the
/// front and `tail` from the back leaves an `align`-aligned range of exactly
/// `len` bytes. `None` on address-space overflow or when the range cannot
/// fit an aligned `len` bytes.
///
/// When `addr` and `len` are page-aligned and `align` is a page-multiple
/// power of two, `head` and `tail` are page-aligned (so both trims are
/// unmappable) — proved by the Kani harnesses.
#[cfg_attr(miri, allow(dead_code))]
fn align_trim(addr: usize, map_len: usize, len: usize, align: usize) -> Option<(usize, usize)> {
    debug_assert!(align.is_power_of_two());
    let aligned = addr.checked_next_multiple_of(align)?;
    let head = aligned - addr;
    let tail = map_len.checked_sub(head.checked_add(len)?)?;
    Some((head, tail))
}

/// The platform seam: mapping, unmapping, paging advice, and the page size.
///
/// The `mmap` variant is production; the Miri variant backs regions with the
/// Rust heap and treats every advice as a contents-preserving no-op — the
/// weakest behavior the advice contracts allow — so the pool's tests run
/// under the interpreter with full provenance and data-race checking:
///
/// ```text
/// MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test -p mz-ore --features pool pool::
/// ```
///
/// (The isolation flag is for the test harness's wall-clock log timestamps,
/// not for anything the pool does.)
#[cfg(not(miri))]
mod sys {
    use std::io;

    use super::{align_trim, aligned_subrange};

    /// Paging advice, in the vocabulary the pool needs.
    pub(super) enum Advice {
        /// Release physical pages; contents become undefined.
        DontNeed,
        /// Reclaim to the swap device now; contents preserved.
        PageOut,
        /// Fault back in ahead of need; contents preserved.
        WillNeed,
        /// Exclude from transparent huge pages; contents preserved.
        NoHugePage,
        /// Exclude from core dumps; contents preserved.
        DontDump,
    }

    /// Maps `len` bytes of anonymous memory with the base aligned to
    /// `align`. `len` must be a whole number of pages and `align` a
    /// page-multiple power of two. Alignments beyond one page over-map and
    /// trim; huge-page alignments additionally advise `MADV_HUGEPAGE`
    /// (best-effort; the kernel falls back to base pages under
    /// fragmentation).
    pub(super) fn map(len: usize, align: usize) -> io::Result<*mut u8> {
        let page = page_size();
        debug_assert!(len % page == 0 && align % page == 0);
        #[cfg(target_os = "linux")]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;

        let map_len = if align > page { len + align } else { len };
        // SAFETY: anonymous mapping with a null hint; the kernel picks a
        // fresh range that aliases no existing Rust object. `map_len` is
        // positive and page-aligned by construction.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let raw = raw.cast::<u8>();
        if align <= page {
            return Ok(raw);
        }

        // Trim the over-mapped head and tail so the base is aligned and the
        // region owns exactly `len` bytes; `unmap` releases that range.
        let Some((head, tail)) = align_trim(raw.addr(), map_len, len, align) else {
            // Address-space arithmetic overflowed; treat the reservation as
            // failed rather than keep an unaligned mapping.
            // SAFETY: unmapping the mapping created above, in full.
            unsafe { libc::munmap(raw.cast::<libc::c_void>(), map_len) };
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        };
        // SAFETY: `head` and `tail` are page-aligned subranges of the
        // mapping just created (`align_trim`'s contract with page-aligned
        // inputs), disjoint from the `len` bytes the region keeps; nothing
        // references them.
        unsafe {
            if head > 0 {
                libc::munmap(raw.cast::<libc::c_void>(), head);
            }
            if tail > 0 {
                libc::munmap(raw.add(head + len).cast::<libc::c_void>(), tail);
            }
        }
        // SAFETY: `head` stays within the original mapping.
        let base = unsafe { raw.add(head) };

        #[cfg(target_os = "linux")]
        if align >= super::HUGE_PAGE {
            // SAFETY: `base`/`len` describe the live aligned mapping; the
            // advice is a non-destructive hint and failure is ignorable.
            unsafe {
                libc::madvise(base.cast::<libc::c_void>(), len, libc::MADV_HUGEPAGE);
            }
        }
        Ok(base)
    }

    /// Releases a mapping returned by [`map`].
    ///
    /// # Safety
    ///
    /// `ptr`, `len`, and `align` must describe exactly one prior [`map`]
    /// result, with no outstanding references into the range.
    pub(super) unsafe fn unmap(ptr: *mut u8, len: usize, _align: usize) {
        // SAFETY: per the function contract.
        unsafe {
            libc::munmap(ptr.cast::<libc::c_void>(), len);
        }
    }

    /// Applies `advice` to the largest page-aligned subrange of
    /// `[ptr, ptr + len)`, rounding the start up and the end down so the
    /// advice never spills onto pages the range only partially covers.
    ///
    /// # Safety
    ///
    /// The range must lie within a live mapping. For [`Advice::DontNeed`]
    /// the caller must additionally uphold the exclusivity contract
    /// documented on [`super::dontneed`]; the remaining advice values are
    /// non-mutating hints.
    pub(super) unsafe fn advise(ptr: *mut u8, len: usize, advice: Advice) {
        let advice = match advice {
            Advice::DontNeed => libc::MADV_DONTNEED,
            #[cfg(target_os = "linux")]
            Advice::PageOut => libc::MADV_PAGEOUT,
            #[cfg(target_os = "linux")]
            Advice::WillNeed => libc::MADV_WILLNEED,
            #[cfg(target_os = "linux")]
            Advice::NoHugePage => libc::MADV_NOHUGEPAGE,
            #[cfg(target_os = "linux")]
            Advice::DontDump => libc::MADV_DONTDUMP,
            // Reclaim, prefetch, THP, and dump hints have no portable
            // equivalent.
            #[cfg(not(target_os = "linux"))]
            Advice::PageOut | Advice::WillNeed | Advice::NoHugePage | Advice::DontDump => return,
        };
        let Some((offset, sub_len)) = aligned_subrange(ptr.addr(), len, page_size()) else {
            return;
        };
        // SAFETY: `offset <= len` (`aligned_subrange`'s contract), so the
        // add stays within the caller's range and preserves provenance.
        let aligned = unsafe { ptr.byte_add(offset) }.cast::<libc::c_void>();
        // SAFETY: pointer and length describe a fully page-aligned subrange
        // of the caller's live mapping; destructive advice is covered by the
        // function contract.
        unsafe {
            libc::madvise(aligned, sub_len, advice);
        }
    }

    /// Whether every page of the page-aligned subrange of `[ptr, ptr + len)`
    /// is nonresident, per `mincore(2)`. A failed observation reports
    /// `false`: the ledger keeps counting pages it cannot prove gone.
    ///
    /// Only Linux answers from the kernel. Elsewhere the reclaim advice is
    /// compiled out, so there is no reclaim to observe and the answer is
    /// `true`, keeping the compressed tier cycling on development platforms.
    #[cfg_attr(test, allow(dead_code))]
    pub(super) fn nonresident(ptr: *mut u8, len: usize) -> bool {
        #[cfg(target_os = "linux")]
        {
            let page = page_size();
            let Some((offset, sub_len)) = aligned_subrange(ptr.addr(), len, page) else {
                // No whole page to observe: vacuously nonresident.
                return true;
            };
            // SAFETY: `offset <= len` (`aligned_subrange`'s contract), so the
            // add stays within the caller's range and preserves provenance.
            let aligned = unsafe { ptr.byte_add(offset) }.cast::<libc::c_void>();
            // `sub_len` is a whole number of pages (`aligned_subrange`'s
            // contract), one status byte each.
            let mut status = vec![0u8; sub_len / page];
            // SAFETY: pointer and length describe a fully page-aligned
            // subrange of the caller's live mapping, and `status` holds one
            // byte per page of it.
            let rc = unsafe { libc::mincore(aligned, sub_len, status.as_mut_ptr().cast()) };
            if rc != 0 {
                return false;
            }
            status.iter().all(|&page_status| page_status & 1 == 0)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (ptr, len);
            true
        }
    }

    pub(super) fn page_size() -> usize {
        // SAFETY: `sysconf` with a valid argument is safe.
        let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page = usize::try_from(raw).expect("page size is positive and fits usize");
        // The alignment arithmetic (`aligned_subrange`, `align_trim`) masks
        // with `page - 1`; a non-power-of-two page would mis-round silently.
        assert!(page.is_power_of_two(), "page size is a power of two");
        page
    }
}

#[cfg(miri)]
mod sys {
    use std::alloc::Layout;
    use std::io;

    pub(super) enum Advice {
        DontNeed,
        PageOut,
        WillNeed,
        NoHugePage,
        DontDump,
    }

    pub(super) fn map(len: usize, align: usize) -> io::Result<*mut u8> {
        let layout = Layout::from_size_align(len, align).expect("valid region layout");
        // SAFETY: `len` is positive (empty regions never map). The memory is
        // deliberately left uninitialized, matching the slot contract that
        // contents are unspecified until fully overwritten; Miri enforces
        // that no path reads a byte before writing it.
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Ok(ptr)
    }

    pub(super) unsafe fn unmap(ptr: *mut u8, len: usize, align: usize) {
        let layout = Layout::from_size_align(len, align).expect("valid region layout");
        // SAFETY: `ptr` was returned by `map` with exactly this layout.
        unsafe { std::alloc::dealloc(ptr, layout) }
    }

    /// Advice is a contents-preserving no-op: the weakest behavior the
    /// contracts allow (`DontNeed` leaves contents undefined, and "old bytes
    /// kept" is one of the permitted outcomes, as on macOS).
    pub(super) unsafe fn advise(_ptr: *mut u8, _len: usize, _advice: Advice) {}

    /// The heap backing has no residency to observe, so reclaim advice is
    /// treated as fully effective, mirroring the non-Linux answer.
    #[cfg_attr(test, allow(dead_code))]
    pub(super) fn nonresident(_ptr: *mut u8, _len: usize) -> bool {
        true
    }

    pub(super) fn page_size() -> usize {
        4096
    }
}

/// Kani proof harnesses over the pure arithmetic and the slot allocator:
/// the sequential facts the pool's unsafe blocks rest on. Run
/// `cargo kani --features pool` from `src/ore`; each harness is exhaustive
/// over its symbolic inputs. Kani ships its own toolchain, so it needs a
/// release whose rustc is at or above the workspace `rust-version` (or the
/// manifest check bypassed for the run). Concurrency-justified claims (the
/// lock-protocol aliasing arguments in `pool.rs`) are outside Kani's
/// sequential model and are exercised under Miri instead.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// `aligned_subrange`'s contract: the subrange lies within the input
    /// range, is nonempty, and is page-aligned at both ends.
    #[kani::proof]
    fn aligned_subrange_stays_in_bounds() {
        let addr: usize = kani::any();
        let len: usize = kani::any();
        let shift: u32 = kani::any();
        kani::assume(shift < usize::BITS);
        let page = 1usize << shift;
        if let Some((offset, sub_len)) = aligned_subrange(addr, len, page) {
            assert!(offset <= len);
            assert!(sub_len <= len - offset);
            assert!(sub_len > 0);
            assert!((addr + offset) % page == 0);
            assert!(sub_len % page == 0);
        }
    }

    /// `align_trim`'s contract: head and tail partition the over-map
    /// exactly around an aligned range of the requested length, and with
    /// page-aligned inputs both trims are page-aligned, so each can be
    /// unmapped independently.
    #[kani::proof]
    fn align_trim_partitions_the_overmap() {
        let addr: usize = kani::any();
        let len: usize = kani::any();
        let page_shift: u32 = kani::any();
        let align_shift: u32 = kani::any();
        kani::assume(page_shift <= align_shift && align_shift < usize::BITS - 1);
        let page = 1usize << page_shift;
        let align = 1usize << align_shift;
        kani::assume(addr % page == 0);
        kani::assume(len % page == 0);
        let Some(map_len) = len.checked_add(align) else {
            return;
        };
        if let Some((head, tail)) = align_trim(addr, map_len, len, align) {
            assert_eq!(head + len + tail, map_len);
            assert!(head < align);
            assert!(head % page == 0);
            assert!(tail % page == 0);
            assert!((addr + head) % align == 0);
        }
    }

    /// The slot allocator, over every alloc/free sequence of a bounded
    /// length: an allocated slot is always in range and never aliases a
    /// live one. This is the disjointness fact `slot_ptr`'s callers turn
    /// into non-aliasing slices.
    #[kani::proof]
    #[kani::unwind(8)]
    fn slot_allocator_hands_out_disjoint_slots() {
        const MAX: u32 = 3;
        let mut slots = SlotAllocator::new(MAX);
        let mut live = [false; MAX as usize];
        for _ in 0..5 {
            if kani::any() {
                if let Some((slot, _warm)) = slots.alloc() {
                    let slot = usize::try_from(slot).expect("fits");
                    assert!(slot < live.len(), "slot out of range");
                    assert!(!live[slot], "live slot handed out twice");
                    live[slot] = true;
                }
            } else {
                let slot: usize = kani::any();
                kani::assume(slot < live.len());
                if live[slot] {
                    live[slot] = false;
                    slots.free(u32::try_from(slot).expect("fits"), kani::any());
                }
            }
        }
    }

    /// The class-selection contract the pool turns into slice bounds: a
    /// selected class always fits the payload.
    #[kani::proof]
    #[kani::unwind(10)]
    fn selected_class_fits_payload() {
        let len_bytes: usize = kani::any();
        if let Some(class) = size_class_for(len_bytes) {
            assert!(class < SIZE_CLASSES.len());
            assert!(SIZE_CLASSES[class] >= len_bytes);
        } else {
            assert!(len_bytes > SIZE_CLASSES[SIZE_CLASSES.len() - 1]);
        }
    }

    /// The offset arithmetic behind `slot_ptr`: for every size class and
    /// every slot index the allocator can hand out, the slot lies within
    /// the region's capacity.
    #[kani::proof]
    #[kani::unwind(9)]
    fn slot_offsets_lie_within_capacity() {
        for &class_size in &SIZE_CLASSES {
            let capacity_bytes: usize = kani::any();
            kani::assume(capacity_bytes <= 1 << 40);
            let capacity = capacity_bytes - capacity_bytes % class_size;
            let max_slots = capacity / class_size;
            let slot: usize = kani::any();
            kani::assume(slot < max_slots);
            let offset = slot * class_size;
            assert!(offset + class_size <= capacity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn alloc_free_reuses_slots() {
        let region = Region::new(64 << 10, 1 << 20).expect("mmap");
        let (a, warm_a) = region.alloc().expect("slot");
        let (b, _) = region.alloc().expect("slot");
        assert!(!warm_a, "bump slots are not warm");
        assert_ne!(a, b);
        assert_ne!(region.slot_ptr(a), region.slot_ptr(b));
        let ptr_a = region.slot_ptr(a);
        // A warm free is preferred by the next alloc and reported warm.
        region.free(a, true);
        let (c, warm_c) = region.alloc().expect("slot");
        assert_eq!(c, a);
        assert!(warm_c);
        assert_eq!(region.slot_ptr(c), ptr_a);
        // A cold free comes back, but not warm.
        region.free(c, false);
        let (d, warm_d) = region.alloc().expect("slot");
        assert_eq!(d, a);
        assert!(!warm_d);
    }

    /// Hugepage-class regions get a huge-page-aligned base, so slots tile
    /// huge-page boundaries exactly.
    #[mz_ore::test]
    fn hugepage_class_base_is_aligned() {
        let region = Region::new(2 << 20, 16 << 20).expect("mmap");
        let (slot, _) = region.alloc().expect("slot");
        if cfg!(target_os = "linux") {
            assert_eq!(
                region.slot_ptr(slot).addr() % HUGE_PAGE,
                0,
                "hugepage-class slots must be huge-page aligned",
            );
        }
    }

    #[mz_ore::test]
    fn exhaustion_returns_none() {
        let region = Region::new(64 << 10, 128 << 10).expect("mmap");
        assert!(region.alloc().is_some());
        assert!(region.alloc().is_some());
        assert!(region.alloc().is_none(), "third slot exceeds capacity");
    }

    #[mz_ore::test]
    fn slots_are_writable_and_advice_is_accepted() {
        let region = Region::new(64 << 10, 1 << 20).expect("mmap");
        let (slot, _) = region.alloc().expect("slot");
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

    #[mz_ore::test]
    fn aligned_subrange_agrees_with_examples() {
        // Fully aligned range: identity.
        assert_eq!(aligned_subrange(4096, 8192, 4096), Some((0, 8192)));
        // Unaligned start rounds up, unaligned end rounds down.
        assert_eq!(aligned_subrange(4097, 8192, 4096), Some((4095, 4096)));
        // Too short to cover a whole page.
        assert_eq!(aligned_subrange(4097, 4096, 4096), None);
        assert_eq!(aligned_subrange(0, 0, 4096), None);
    }

    #[mz_ore::test]
    fn align_trim_agrees_with_examples() {
        // Already aligned: no head, tail is the whole over-map.
        assert_eq!(
            align_trim(HUGE_PAGE, 3 * HUGE_PAGE, 2 * HUGE_PAGE, HUGE_PAGE),
            Some((0, HUGE_PAGE))
        );
        // Unaligned base: head consumes the misalignment.
        assert_eq!(
            align_trim(HUGE_PAGE + 4096, 3 * HUGE_PAGE, 2 * HUGE_PAGE, HUGE_PAGE),
            Some((HUGE_PAGE - 4096, 4096)),
        );
    }
}
