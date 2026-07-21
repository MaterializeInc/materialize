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

//! Swap-backed extents: the backing store for the buffer pool on nodes whose
//! whole disk is provisioned as swap.
//!
//! An extent is a slot in the pool-owned [`ExtentArena`] holding the
//! lz4-compressed bytes of one chunk. "Write" compresses into the slot. The
//! slot stays resident, forming the compressed-but-resident middle tier of
//! the pool's ladder, until the pool's RSS target forces
//! [`SwapExtent::pageout`], which pushes the pages to the swap device with
//! `MADV_PAGEOUT`. "Read" issues
//! `MADV_WILLNEED` ahead of the decompress (and makes the pages resident
//! again); "free" returns the slot to the arena with its pages discarded,
//! which also drops any swapped copy for free. Chunk slots never reach the
//! swap device: only these compressed extents are offered to it.
//!
//! The arena exists so that extent pages never belong to the global
//! allocator: `MADV_PAGEOUT` over allocator-owned memory leaves swap-entry
//! PTEs behind on freed ranges, which the allocator recycles into unrelated
//! allocations that then major-fault reading dead compressed data. Arena
//! regions are advised `MADV_NOHUGEPAGE` once at map time, so the reclaim
//! never needs to split a large folio, and slot recycling never re-touches
//! swap. A class whose region is exhausted degrades to a plain heap
//! allocation (counted, never paged out) rather than failing.
//!
//! Pageout is observed, never assumed: `MADV_PAGEOUT` may decline any page
//! and still return success (a kernel-internal pin fails isolation, the
//! swap device may be full or absent), so after the advice the page table
//! decides whether the extent left memory, and the extent stays fully
//! resident for accounting until its entire range is unmapped. The
//! observation reads pagemap present bits rather than `mincore`, which
//! would count the clean swap-cache copies of successfully reclaimed pages
//! as resident.

use std::alloc::Layout;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cast::CastFrom;
use crate::pool::region::{self, Region};

/// Length in bytes of the little-endian `u32` uncompressed-size prefix that
/// precedes the compressed bytes, matching the
/// `lz4_flex::block::compress_prepend_size` framing.
const SIZE_PREFIX: usize = 4;

/// Consecutive incomplete pageout passes after which an extent stops being
/// advised out until a read faults it back in. Transient declines are
/// kernel-internal page pins that defeat the reclaim's isolation step (a
/// folio sitting in another CPU's LRU batch, a momentary swap-slot
/// allocation failure); they clear as soon as the pin drains, so a retry
/// or two recovers them. Persistent declines (no swap device, an exhausted
/// or unswappable cgroup) never clear, and further advice is pure
/// page-table walking. Three passes covers the transient causes while
/// bounding the wasted advice at two extra passes per extent.
pub(crate) const PAGEOUT_RETRY_CAP: u8 = 3;

/// The extent size-class ladder for a `page`-byte page size: `page`, then
/// sizes of the form `2^k` and `3 * 2^(k-1)` bytes, up through the first
/// class that fits lz4's worst case over the largest chunk size class.
/// Every class is a multiple of `page` (the sub-page mid class between
/// `page` and `2 * page` is skipped), so slot-granular paging advice is
/// exact on any kernel page size. Consecutive classes above the smallest
/// are within 1.5x of each other, which bounds a compressed payload's
/// internal fragmentation below 1.5x (page-granular slack at the small
/// end).
fn extent_classes(page: usize) -> Vec<usize> {
    let max_comp = SIZE_PREFIX + lz4_flex::block::get_maximum_output_size(max_chunk_bytes());
    let mut classes = vec![page];
    let mut base = 2 * page;
    loop {
        classes.push(base);
        if base >= max_comp {
            break;
        }
        // `3 * 2^(k-1)`: a page multiple because `base` is at least two
        // pages.
        let mid = base + base / 2;
        classes.push(mid);
        if mid >= max_comp {
            break;
        }
        base *= 2;
    }
    classes
}

/// The largest chunk payload the pool can ask an extent to back.
fn max_chunk_bytes() -> usize {
    region::SIZE_CLASSES[region::SIZE_CLASSES.len() - 1]
}

/// Pool-owned arena of anonymous-memory regions backing extents, one region
/// per entry of the [`extent_classes`] ladder. Slots are allocated at write,
/// freed cold (pages and any swap copy discarded) at extent drop, and never
/// kept warm: a freed extent's compressed bytes are dead by definition.
#[derive(Debug)]
pub(crate) struct ExtentArena {
    /// Ladder of extent class sizes in bytes, ascending; same order as
    /// `regions`.
    classes: Vec<usize>,
    regions: Vec<Region>,
    /// Extent writes that degraded to the heap because their class had no
    /// free slot.
    fallbacks: AtomicU64,
}

impl ExtentArena {
    /// Reserves one region per extent class, `class_capacity_bytes` of
    /// virtual space each. The reservation knob mirrors the slot regions'
    /// so tests can exercise class exhaustion with small arenas.
    pub(crate) fn new(class_capacity_bytes: usize) -> io::Result<ExtentArena> {
        let classes = extent_classes(region::page_size());
        let regions = classes
            .iter()
            .map(|&class_size| Region::new_nohuge(class_size, class_capacity_bytes))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(ExtentArena {
            classes,
            regions,
            fallbacks: AtomicU64::new(0),
        })
    }

    /// Number of extent writes that degraded to the heap.
    pub(crate) fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Ordering::Relaxed)
    }

    /// Allocates a slot fitting a compressed payload of `comp_len` bytes,
    /// or `None` when the class is exhausted (the caller degrades to the
    /// heap).
    fn alloc(&self, comp_len: usize) -> Option<(usize, u32)> {
        let class = self.classes.iter().position(|&c| c >= comp_len)?;
        let (slot, _warm) = self.regions[class].alloc()?;
        Some((class, slot))
    }
}

/// One chunk's compressed backing copy.
#[derive(Debug)]
pub(crate) struct SwapExtent {
    ptr: *mut u8,
    /// Byte size of the backing allocation (the extent's class size, or the
    /// heap layout on the fallback path): the granule the resident
    /// accounting and the pageout operate on.
    alloc_size: usize,
    comp_len: usize,
    /// Whether the extent's pages are (engine-)resident: set at write and by
    /// [`SwapExtent::read_into`], cleared by a [`SwapExtent::pageout`] whose
    /// residency observation found the whole range gone. Drives the pool's
    /// `extent_resident_bytes` accounting; mutated only under the owning
    /// chunk's state mutex.
    resident: bool,
    /// Consecutive pageout passes whose observation found pages still
    /// resident. Reset by [`SwapExtent::read_into`]. At
    /// [`PAGEOUT_RETRY_CAP`] the extent stops being advised out.
    incomplete_passes: u8,
    backing: Backing,
}

/// Where an extent's bytes live.
#[derive(Debug)]
enum Backing {
    /// A slot in the pool's extent arena.
    Arena {
        arena: Arc<ExtentArena>,
        class: usize,
        slot: u32,
    },
    /// Global-allocator fallback for an exhausted class. Never paged out:
    /// `MADV_PAGEOUT` over allocator-owned pages leaves swap-entry PTEs on
    /// freed ranges for the allocator to recycle into unrelated
    /// allocations, which is the failure the arena exists to avoid.
    Heap { layout: Layout },
}

/// Retention policy for the thread-local compression scratch across
/// [`SwapExtent::write`] calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scratch {
    /// Keep the grown scratch for the next job: for spill threads, whose
    /// job stream reuses it immediately.
    Retain,
    /// Release the scratch after the job: for inline compression on worker
    /// threads, where a retained scratch would park the largest class's
    /// worst case (~8 MiB) per thread indefinitely, invisible to every
    /// gauge.
    Shrink,
}

// SAFETY: the extent exclusively owns its backing (an arena slot handed out
// by the region allocator, or a heap allocation); nothing else holds a
// pointer into it, so moving the owner across threads is sound. All access
// goes through the owning chunk's state mutex.
unsafe impl Send for SwapExtent {}

impl SwapExtent {
    /// Compresses `data` into a fresh extent, preferring an arena slot and
    /// degrading to the heap when the payload's class is exhausted. The
    /// pages stay resident; the pool's RSS-target enforcement decides when
    /// [`SwapExtent::pageout`] pushes them to the device.
    ///
    /// Compression goes through a reused thread-local scratch buffer so the
    /// extent slot can be chosen by the *actual* compressed payload rather
    /// than lz4's worst case. Worst-case sizing costs ~5.6× on compressible
    /// data, in swap capacity and in swap write bandwidth per eviction (the
    /// whole allocation is paged out), which at hydration eviction rates
    /// backs up device writeback and bloats the working set with swap-cache
    /// pages. `scratch` says whether the caller's thread keeps the grown
    /// scratch for its next job.
    pub(crate) fn write(arena: &Arc<ExtentArena>, data: &[u64], scratch: Scratch) -> SwapExtent {
        use std::cell::RefCell;
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        let bytes: &[u8] = bytemuck::cast_slice(data);
        SCRATCH.with(|cell| {
            let mut buf = cell.borrow_mut();
            let max_out = lz4_flex::block::get_maximum_output_size(bytes.len());
            buf.resize(SIZE_PREFIX + max_out, 0);
            let uncompressed_len =
                u32::try_from(bytes.len()).expect("chunk payloads are bounded by the size classes");
            buf[..SIZE_PREFIX].copy_from_slice(&uncompressed_len.to_le_bytes());
            let compressed = lz4_flex::block::compress_into(bytes, &mut buf[SIZE_PREFIX..])
                .expect("output sized by get_maximum_output_size");
            let comp_len = SIZE_PREFIX + compressed;

            let (ptr, alloc_size, backing) = match arena.alloc(comp_len) {
                Some((class, slot)) => (
                    arena.regions[class].slot_ptr(slot),
                    arena.classes[class],
                    Backing::Arena {
                        arena: Arc::clone(arena),
                        class,
                        slot,
                    },
                ),
                None => {
                    arena.fallbacks.fetch_add(1, Ordering::Relaxed);
                    let layout = heap_layout(comp_len);
                    // SAFETY: `layout` has nonzero size (`comp_len` includes
                    // the prefix).
                    let ptr = unsafe { std::alloc::alloc(layout) };
                    if ptr.is_null() {
                        std::alloc::handle_alloc_error(layout);
                    }
                    (ptr, layout.size(), Backing::Heap { layout })
                }
            };
            // The slack past `comp_len` is deliberately never touched: only
            // the first `comp_len` bytes are ever read back, and writing the
            // tail would fault pages the class's virtual slack is meant to
            // keep free.
            //
            // SAFETY: the destination is exclusively owned here (a freshly
            // allocated arena slot or heap allocation) and covers `comp_len`
            // bytes (the selected class fits the payload; the heap layout is
            // sized to it). The source is the scratch buffer, which cannot
            // alias a fresh allocation.
            unsafe {
                std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr, comp_len);
            }
            if scratch == Scratch::Shrink {
                buf.clear();
                // TODO: consider retaining some capacity here (shrinking to
                // the next power of two, say) rather than releasing all of
                // it; measure the realloc traffic before tuning.
                buf.shrink_to_fit();
            }
            SwapExtent {
                ptr,
                alloc_size,
                comp_len,
                resident: true,
                incomplete_passes: 0,
                backing,
            }
        })
    }

    /// The byte size of the extent's allocation: the granule the resident
    /// accounting and the pageout operate on.
    pub(crate) fn alloc_size(&self) -> usize {
        self.alloc_size
    }

    /// Whether the extent's pages are engine-resident (not pushed to the
    /// device since the last write or read).
    pub(crate) fn is_resident(&self) -> bool {
        self.resident
    }

    /// Whether the extent's pageout retry budget is exhausted: consecutive
    /// incomplete passes reached [`PAGEOUT_RETRY_CAP`], so callers stop
    /// calling [`SwapExtent::pageout`] until a read resets the budget.
    /// Heap-fallback extents are permanently capped: they are never advised
    /// out and stay counted resident until freed.
    pub(crate) fn pageout_capped(&self) -> bool {
        match self.backing {
            Backing::Heap { .. } => true,
            Backing::Arena { .. } => self.incomplete_passes >= PAGEOUT_RETRY_CAP,
        }
    }

    /// Hints the kernel to push the extent's pages to the swap device and
    /// observes the result: returns `true`, marking the extent non-resident,
    /// only when the observation finds the whole range unmapped (a page
    /// whose clean copy lingers in the swap cache counts as reclaimed). An
    /// incomplete pass leaves the extent fully resident for accounting and
    /// spends one unit of the retry budget: a single pinned page keeps the
    /// whole extent counted, which is the safe direction, and the retry
    /// budget exists exactly for such transient pins. Cheap: the
    /// compression is already paid, the madvise and page-table read are
    /// microseconds, and the device write happens on the kernel's
    /// asynchronous writeback path.
    ///
    /// Callers must not invoke this on a [`SwapExtent::pageout_capped`]
    /// extent.
    pub(crate) fn pageout(&mut self) -> bool {
        debug_assert!(!self.pageout_capped());
        region::pageout(self.ptr, self.alloc_size);
        if region::nonresident(self.ptr, self.alloc_size) {
            self.resident = false;
            self.incomplete_passes = 0;
            true
        } else {
            self.incomplete_passes += 1;
            false
        }
    }

    /// Compressed size in bytes, including the size prefix.
    pub(crate) fn comp_len(&self) -> usize {
        self.comp_len
    }

    /// Hints the kernel to swap the extent's pages back in ahead of a read.
    pub(crate) fn prefetch(&self) {
        region::willneed(self.ptr, self.alloc_size);
    }

    /// Decompresses the extent into `dst`, which must be exactly the chunk's
    /// uncompressed length. Reading faults the pages back in, so the extent
    /// is resident again afterwards; the caller owns the accounting for that
    /// transition (the pool re-counts and re-enqueues it for the RSS target).
    pub(crate) fn read_into(&mut self, dst: &mut [u8]) {
        let full = self.uncompressed_len();
        assert_eq!(
            full,
            dst.len(),
            "destination must match the extent's uncompressed length"
        );
        self.read_range_into(0, dst);
    }

    /// Decompresses the byte range `[offset, offset + dst.len())` of the
    /// extent's uncompressed body into `dst`. The range must lie within the
    /// body. Residency effects are those of [`SwapExtent::read_into`]
    /// regardless of the range: the stored form is a whole lz4 block, so any
    /// read faults and decompresses the entire extent, and a sub-range only
    /// narrows the final copy. A backend whose stored form is rangeable (file
    /// extents reading with `pread`, a sub-block-framed codec) can serve the
    /// range with proportional I/O behind this same signature.
    pub(crate) fn read_range_into(&mut self, offset: usize, dst: &mut [u8]) {
        self.resident = true;
        // The decompress faults every page back in, so prior incomplete
        // pageout passes no longer describe the mapping and the retry
        // budget starts over.
        self.incomplete_passes = 0;
        self.prefetch();
        // SAFETY: the extent exclusively owns its backing, and the first
        // `comp_len` bytes were initialized by `write`.
        let buf = unsafe { std::slice::from_raw_parts(self.ptr, self.comp_len) };
        let uncompressed_len = self.uncompressed_len();
        let end = offset
            .checked_add(dst.len())
            .expect("range end overflows usize");
        assert!(
            end <= uncompressed_len,
            "range end {end} exceeds the extent's uncompressed length {uncompressed_len}",
        );
        if offset == 0 && dst.len() == uncompressed_len {
            let written = lz4_flex::block::decompress_into(&buf[SIZE_PREFIX..], dst)
                .expect("extent holds a valid lz4 block");
            assert_eq!(written, dst.len(), "decompressed length mismatch");
            return;
        }
        // A sub-range still decompresses the whole block, into a reused
        // thread-local scratch, and copies the range out. Reads run on
        // worker threads, so the scratch mirrors the write side's `Shrink`
        // policy: capacity beyond the ~2 MiB chunk target is released after
        // the copy rather than parked per worker.
        use std::cell::RefCell;
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            scratch.resize(uncompressed_len, 0);
            let written = lz4_flex::block::decompress_into(&buf[SIZE_PREFIX..], &mut scratch)
                .expect("extent holds a valid lz4 block");
            assert_eq!(written, uncompressed_len, "decompressed length mismatch");
            dst.copy_from_slice(&scratch[offset..end]);
            if scratch.capacity() > 2 << 20 {
                scratch.clear();
                scratch.shrink_to_fit();
            }
        });
    }

    /// The uncompressed body length recorded in the extent's size prefix.
    fn uncompressed_len(&self) -> usize {
        // SAFETY: the extent exclusively owns its backing, and the prefix
        // was initialized by `write`.
        let buf = unsafe { std::slice::from_raw_parts(self.ptr, SIZE_PREFIX) };
        let prefix: [u8; SIZE_PREFIX] = buf.try_into().expect("prefix length");
        usize::cast_from(u32::from_le_bytes(prefix))
    }
}

/// The heap layout of a fallback extent for a compressed payload of
/// `comp_len` bytes.
fn heap_layout(comp_len: usize) -> Layout {
    Layout::array::<u8>(comp_len).expect("valid extent layout")
}

impl Drop for SwapExtent {
    fn drop(&mut self) {
        match &self.backing {
            Backing::Arena { arena, class, slot } => {
                // Discarding the pages also drops any copy on the swap
                // device (`MADV_DONTNEED` frees an anonymous range's swap
                // entries), so the slot returns to the free list with no
                // dead compressed data left to fault back in.
                //
                // SAFETY: the extent exclusively owns the slot and is being
                // dropped, so no reference into it exists.
                unsafe {
                    region::dontneed(self.ptr, self.alloc_size);
                }
                arena.regions[*class].free(*slot, false);
            }
            Backing::Heap { layout } => {
                // SAFETY: `ptr` was returned by `alloc` with exactly this
                // `layout` in `write` and is deallocated exactly once, here.
                unsafe {
                    std::alloc::dealloc(self.ptr, *layout);
                }
            }
        }
    }
}

/// Kani proof harnesses over the extent ladder arithmetic; see the sibling
/// module in `region.rs` for scope and run instructions.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// The extent ladder's contract for every plausible page size: every
    /// class is a page multiple, the top class fits lz4's worst case over
    /// the largest chunk class, a class is found for every payload up to
    /// that bound and fits it, and the selected class overshoots the payload
    /// by less than 1.5x (with page-granular slack at the smallest classes).
    #[kani::proof]
    #[kani::unwind(64)]
    fn extent_ladder_fits_payloads() {
        let page_shift: u32 = kani::any();
        kani::assume(page_shift >= 12 && page_shift <= 16);
        let page = 1usize << page_shift;
        let classes = extent_classes(page);
        let max_comp = SIZE_PREFIX + lz4_flex::block::get_maximum_output_size(max_chunk_bytes());
        for &class in &classes {
            assert!(class % page == 0);
        }
        assert!(classes[classes.len() - 1] >= max_comp);
        let comp_len: usize = kani::any();
        kani::assume(comp_len >= 1 && comp_len <= max_comp);
        let class = classes
            .iter()
            .position(|&c| c >= comp_len)
            .expect("ladder covers every payload up to the bound");
        assert!(classes[class] >= comp_len);
        if class <= 1 {
            assert!(classes[class] - comp_len < page);
        } else {
            // The previous class was too small and consecutive classes are
            // within 3/2 of each other, so the allocation is below 1.5x the
            // payload.
            assert!(classes[class - 1] < comp_len);
            assert!(classes[class] * 2 <= classes[class - 1] * 3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small arena for extent tests. Under Miri the region backing is real
    /// interpreter heap rather than lazy virtual memory, so shrink further.
    fn arena() -> Arc<ExtentArena> {
        let capacity = if cfg!(miri) { 1 << 20 } else { 64 << 20 };
        Arc::new(ExtentArena::new(capacity).expect("arena creation"))
    }

    #[mz_ore::test]
    fn round_trip() {
        let arena = arena();
        let data: Vec<u64> = (0..10_000).map(|i| i * 37).collect();
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert!(extent.comp_len() > SIZE_PREFIX);
        extent.prefetch();
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    #[mz_ore::test]
    fn compressible_data_shrinks() {
        let arena = arena();
        let data = vec![42u64; 100_000];
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert!(extent.comp_len() < data.len() * 8 / 4);
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    /// The slot is sized to the compressed payload, not lz4's worst case:
    /// extents must cost swap capacity and write bandwidth in proportion to
    /// what they store.
    #[mz_ore::test]
    fn allocation_is_sized_to_payload() {
        let arena = arena();
        let data = vec![7u64; 100_000];
        let extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        let page = region::page_size();
        assert!(extent.alloc_size() >= extent.comp_len());
        assert_eq!(extent.alloc_size() % page, 0, "class is a page multiple");
        assert!(
            extent.alloc_size() < data.len() * 8 / 8,
            "compressible data must not be stored at worst-case size",
        );
        assert!(
            extent.alloc_size() <= (extent.comp_len() * 3 / 2).max(2 * page),
            "the ladder bounds internal fragmentation",
        );
    }

    /// The ladder is page-granular, strictly ascending, covers lz4's worst
    /// case over the largest chunk class, and steps by at most 1.5x above
    /// the smallest class.
    #[mz_ore::test]
    fn ladder_shape() {
        for page in [4096usize, 16384, 65536] {
            let classes = extent_classes(page);
            let max_comp =
                SIZE_PREFIX + lz4_flex::block::get_maximum_output_size(max_chunk_bytes());
            assert!(classes.windows(2).all(|w| w[0] < w[1]), "ascending");
            assert!(classes.iter().all(|&c| c % page == 0), "page multiples");
            assert!(classes[classes.len() - 1] >= max_comp, "covers worst case");
            for w in classes.windows(2).skip(1) {
                assert!(w[1] * 2 <= w[0] * 3, "steps at most 1.5x above the base");
            }
        }
    }

    /// An exhausted extent class degrades to a heap-backed extent that still
    /// round-trips, is never advised out, and is counted; freeing an arena
    /// extent lets the next write reuse its slot.
    #[mz_ore::test]
    fn exhaustion_falls_back_to_heap() {
        // One page of capacity: the smallest class holds one slot, every
        // larger class is empty.
        let arena = Arc::new(ExtentArena::new(region::page_size()).expect("arena creation"));
        let data = vec![3u64; 64];
        let a = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert_eq!(arena.fallbacks(), 0);
        assert!(!a.pageout_capped(), "arena extents start with retry budget");
        let mut b = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert_eq!(arena.fallbacks(), 1, "second same-class write degrades");
        assert!(b.pageout_capped(), "heap extents are never advised out");
        assert!(b.is_resident());
        let mut out = vec![0u64; data.len()];
        b.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
        // Freeing the arena extent frees its slot for the next write.
        drop(a);
        let c = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert_eq!(arena.fallbacks(), 1, "freed slot is reused, no fallback");
        drop(c);
        drop(b);
    }

    /// A ranged read returns exactly the corresponding slice of a full
    /// read, at aligned and unaligned offsets, across page boundaries, and
    /// at the body's edges.
    #[mz_ore::test]
    fn ranged_read_matches_full_read_slice() {
        let arena = arena();
        let data: Vec<u64> = (0..10_000u64).map(|i| i.wrapping_mul(0x9E37)).collect();
        let bytes: &[u8] = bytemuck::cast_slice(&data);
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        let mut full = vec![0u8; bytes.len()];
        extent.read_into(&mut full);
        assert_eq!(full, bytes);
        let page = region::page_size();
        let ranges = [
            (0, 8),
            (8, 16),
            (page - 3, page + 7),
            (bytes.len() - 24, 24),
            (0, bytes.len()),
        ];
        for (offset, len) in ranges {
            let mut out = vec![0u8; len];
            extent.read_range_into(offset, &mut out);
            assert_eq!(out, &full[offset..offset + len], "range ({offset}, {len})");
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "range end")]
    fn ranged_read_out_of_bounds_panics() {
        let arena = arena();
        let data = vec![5u64; 64];
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        let mut out = vec![0u8; 16];
        extent.read_range_into(64 * 8 - 8, &mut out);
    }

    #[mz_ore::test]
    #[should_panic(expected = "destination must match")]
    fn wrong_destination_length_panics() {
        let arena = arena();
        let data = vec![1u64; 16];
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        let mut out = vec![0u64; 8];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
    }

    /// Residency follows the observation, not the advice: a declined pass
    /// leaves the extent resident, an accepted one marks it gone.
    #[mz_ore::test]
    fn pageout_is_observed_not_trusted() {
        let arena = arena();
        let data = vec![9u64; 10_000];
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        assert!(extent.is_resident());
        region::fake_residency::decline_next(1);
        assert!(!extent.pageout(), "declined pass reports incomplete");
        assert!(extent.is_resident(), "declined pass leaves it resident");
        assert!(!extent.pageout_capped());
        assert!(extent.pageout(), "accepted pass reports reclaimed");
        assert!(!extent.is_resident());
    }

    /// Consecutive declined passes exhaust the retry budget. A read faults
    /// the pages back in and restores it.
    #[mz_ore::test]
    fn pageout_retry_cap_and_read_reset() {
        let arena = arena();
        let data: Vec<u64> = (0..10_000).collect();
        let mut extent = SwapExtent::write(&arena, &data, Scratch::Shrink);
        region::fake_residency::decline_next(u64::MAX);
        let mut passes = 0u8;
        while !extent.pageout_capped() {
            assert!(!extent.pageout());
            passes += 1;
        }
        assert_eq!(passes, PAGEOUT_RETRY_CAP, "capped after exactly the cap");
        assert!(extent.is_resident(), "capped extent stays resident");
        region::fake_residency::decline_next(0);
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
        assert!(!extent.pageout_capped(), "a read resets the retry budget");
        assert!(extent.pageout());
    }
}
