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
//! An extent is a page-aligned anonymous allocation holding the lz4-compressed
//! bytes of one chunk. "Write" compresses into the allocation, which stays
//! resident — the compressed-but-resident middle tier of the pool's ladder —
//! until the pool's RSS target forces [`SwapExtent::pageout`], which pushes
//! the pages to the swap device with `MADV_PAGEOUT`. "Read" issues
//! `MADV_WILLNEED` ahead of the decompress (and makes the pages resident
//! again); "free" is a plain deallocation, with any swapped copy discarded
//! for free. Nothing is ever both uncompressed and on the device.
//!
//! Pageout is observed, never assumed: `MADV_PAGEOUT` may decline any page
//! and still return success (a transient extra reference fails isolation,
//! the swap device may be full or absent), so after the advice `mincore`
//! decides whether the extent left memory, and the extent stays fully
//! resident for accounting until its entire range is nonresident. Extents
//! are advised `MADV_NOHUGEPAGE` at creation so the reclaim never needs to
//! split a large folio the extent covers only partially.

use std::alloc::Layout;

use crate::cast::CastFrom;
use crate::pool::region;

/// Alignment and size granule of extent allocations.
const EXTENT_ALIGN: usize = 4096;

/// Length in bytes of the little-endian `u32` uncompressed-size prefix that
/// precedes the compressed bytes, matching the
/// `lz4_flex::block::compress_prepend_size` framing.
const SIZE_PREFIX: usize = 4;

/// Consecutive incomplete pageout passes after which an extent stops being
/// advised out until a read faults it back in. Transient declines (a racing
/// read holding an extra page reference, a momentary swap-slot allocation
/// failure) clear as soon as the racing operation finishes, so a retry or
/// two recovers them. Persistent declines (no swap device, an exhausted or
/// unswappable cgroup) never clear, and further advice is pure page-table
/// walking. Three passes covers the transient causes while bounding the
/// wasted advice at two extra passes per extent.
pub(crate) const PAGEOUT_RETRY_CAP: u8 = 3;

/// One chunk's compressed backing copy.
#[derive(Debug)]
pub(crate) struct SwapExtent {
    ptr: *mut u8,
    layout: Layout,
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
}

// SAFETY: the extent exclusively owns its allocation; nothing else holds a
// pointer into it, so moving the owner across threads is sound. All access
// goes through the owning chunk's state mutex.
unsafe impl Send for SwapExtent {}

impl SwapExtent {
    /// Compresses `data` into a fresh extent. The pages stay resident; the
    /// pool's RSS-target enforcement decides when [`SwapExtent::pageout`]
    /// pushes them to the device.
    ///
    /// Compression goes through a reused thread-local scratch buffer so the
    /// extent allocation can be sized to the *actual* compressed payload
    /// (rounded to the page granule) rather than lz4's worst case. Worst-case
    /// sizing costs ~5.6× on compressible data — in swap capacity, in swap
    /// write bandwidth per eviction (the whole allocation is paged out), and
    /// in `alloc_zeroed` memset traffic on recycled allocations — which at
    /// hydration eviction rates backs up device writeback and bloats the
    /// working set with swap-cache pages.
    pub(crate) fn write(data: &[u64]) -> SwapExtent {
        use std::cell::RefCell;
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        let bytes: &[u8] = bytemuck::cast_slice(data);
        SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            let max_out = lz4_flex::block::get_maximum_output_size(bytes.len());
            scratch.resize(SIZE_PREFIX + max_out, 0);
            let uncompressed_len =
                u32::try_from(bytes.len()).expect("chunk payloads are bounded by the size classes");
            scratch[..SIZE_PREFIX].copy_from_slice(&uncompressed_len.to_le_bytes());
            let compressed = lz4_flex::block::compress_into(bytes, &mut scratch[SIZE_PREFIX..])
                .expect("output sized by get_maximum_output_size");
            let comp_len = SIZE_PREFIX + compressed;

            let layout = extent_layout(comp_len);
            // SAFETY: `layout` has nonzero size (at least one granule).
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Opt out of transparent huge pages before first touch:
            // `MADV_PAGEOUT` must split any large folio the advised range
            // covers only partially, and a failed split silently leaves
            // those pages resident. Page-rounded extents routinely cover
            // multi-size THP folios partially, so base pages keep the
            // reclaim page-exact.
            region::nohugepage(ptr, layout.size());
            // SAFETY: `ptr` is a fresh allocation of at least `comp_len`
            // bytes (`extent_layout`'s contract) exclusively owned here; the
            // copy plus the tail zeroing below initialize every byte, so
            // later borrows of the allocation (in `read_into`) see
            // initialized memory. The source is the scratch buffer, which
            // cannot alias the fresh allocation.
            unsafe {
                std::ptr::copy_nonoverlapping(scratch.as_ptr(), ptr, comp_len);
                std::ptr::write_bytes(ptr.add(comp_len), 0, layout.size() - comp_len);
            }
            SwapExtent {
                ptr,
                layout,
                comp_len,
                resident: true,
                incomplete_passes: 0,
            }
        })
    }

    /// The byte size of the extent's allocation: the granule the resident
    /// accounting and the pageout operate on.
    pub(crate) fn alloc_size(&self) -> usize {
        self.layout.size()
    }

    /// Whether the extent's pages are engine-resident (not pushed to the
    /// device since the last write or read).
    pub(crate) fn is_resident(&self) -> bool {
        self.resident
    }

    /// Whether the extent's pageout retry budget is exhausted: consecutive
    /// incomplete passes reached [`PAGEOUT_RETRY_CAP`], so callers stop
    /// calling [`SwapExtent::pageout`] until a read resets the budget.
    pub(crate) fn pageout_capped(&self) -> bool {
        self.incomplete_passes >= PAGEOUT_RETRY_CAP
    }

    /// Hints the kernel to push the extent's pages to the swap device and
    /// observes the result: returns `true`, marking the extent non-resident,
    /// only when the observation finds the whole range out of memory. An
    /// incomplete pass leaves the extent fully resident for accounting and
    /// spends one unit of the retry budget. Cheap: the compression is
    /// already paid, the madvise and mincore are microseconds, and the
    /// device write happens on the kernel's asynchronous writeback path.
    ///
    /// Callers must not invoke this on a [`SwapExtent::pageout_capped`]
    /// extent.
    pub(crate) fn pageout(&mut self) -> bool {
        debug_assert!(!self.pageout_capped());
        region::pageout(self.ptr, self.layout.size());
        if region::nonresident(self.ptr, self.layout.size()) {
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
        region::willneed(self.ptr, self.layout.size());
    }

    /// Decompresses the extent into `dst`, which must be exactly the chunk's
    /// uncompressed length. Reading faults the pages back in, so the extent
    /// is resident again afterwards; the caller owns the accounting for that
    /// transition (the pool re-counts and re-enqueues it for the RSS target).
    pub(crate) fn read_into(&mut self, dst: &mut [u8]) {
        self.resident = true;
        // The decompress faults every page back in, so prior incomplete
        // pageout passes no longer describe the mapping and the retry
        // budget starts over.
        self.incomplete_passes = 0;
        self.prefetch();
        // SAFETY: the extent exclusively owns `[ptr, ptr + layout.size())`
        // and `comp_len <= layout.size()` by construction in `write`.
        let buf = unsafe { std::slice::from_raw_parts(self.ptr, self.comp_len) };
        let prefix: [u8; SIZE_PREFIX] = buf[..SIZE_PREFIX].try_into().expect("prefix length");
        let uncompressed_len = usize::cast_from(u32::from_le_bytes(prefix));
        assert_eq!(
            uncompressed_len,
            dst.len(),
            "destination must match the extent's uncompressed length"
        );
        let written = lz4_flex::block::decompress_into(&buf[SIZE_PREFIX..], dst)
            .expect("extent holds a valid lz4 block");
        assert_eq!(written, dst.len(), "decompressed length mismatch");
    }
}

/// The allocation layout for a compressed payload of `comp_len` bytes: the
/// size rounded up to the extent granule, so the allocation always covers
/// the payload with less than one granule of slack.
fn extent_layout(comp_len: usize) -> Layout {
    let size = comp_len.next_multiple_of(EXTENT_ALIGN);
    Layout::from_size_align(size, EXTENT_ALIGN).expect("valid extent layout")
}

impl Drop for SwapExtent {
    fn drop(&mut self) {
        // SAFETY: `ptr` was returned by `alloc` with exactly this `layout`
        // in `write` and is deallocated exactly once, here.
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

/// Kani proof harnesses over the extent layout arithmetic; see the sibling
/// module in `region.rs` for scope and run instructions.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// `extent_layout` covers every payload the size classes can produce,
    /// with less than one granule of slack, at the granule alignment. The
    /// bound is lz4's worst case over the largest class plus the size
    /// prefix, which is also what justifies the unchecked `next_multiple_of`
    /// (no overflow within the bound).
    #[kani::proof]
    fn extent_layout_covers_payload() {
        let max_comp = SIZE_PREFIX
            + lz4_flex::block::get_maximum_output_size(
                region::SIZE_CLASSES[region::SIZE_CLASSES.len() - 1],
            );
        let comp_len: usize = kani::any();
        kani::assume(comp_len >= 1 && comp_len <= max_comp);
        let layout = extent_layout(comp_len);
        assert!(layout.size() >= comp_len);
        assert!(layout.size() - comp_len < EXTENT_ALIGN);
        assert!(layout.size() % EXTENT_ALIGN == 0);
        assert_eq!(layout.align(), EXTENT_ALIGN);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn round_trip() {
        let data: Vec<u64> = (0..10_000).map(|i| i * 37).collect();
        let mut extent = SwapExtent::write(&data);
        assert!(extent.comp_len() > SIZE_PREFIX);
        extent.prefetch();
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    #[mz_ore::test]
    fn compressible_data_shrinks() {
        let data = vec![42u64; 100_000];
        let mut extent = SwapExtent::write(&data);
        assert!(extent.comp_len() < data.len() * 8 / 4);
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    /// The allocation is sized to the compressed payload, not lz4's worst
    /// case: extents must cost swap capacity and write bandwidth in
    /// proportion to what they store.
    #[mz_ore::test]
    fn allocation_is_sized_to_payload() {
        let data = vec![7u64; 100_000];
        let extent = SwapExtent::write(&data);
        assert_eq!(
            extent.alloc_size(),
            extent.comp_len().next_multiple_of(EXTENT_ALIGN),
        );
        assert!(
            extent.alloc_size() < data.len() * 8 / 8,
            "compressible data must not be stored at worst-case size",
        );
    }

    #[mz_ore::test]
    #[should_panic(expected = "destination must match")]
    fn wrong_destination_length_panics() {
        let data = vec![1u64; 16];
        let mut extent = SwapExtent::write(&data);
        let mut out = vec![0u64; 8];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
    }

    /// Residency follows the observation, not the advice: a declined pass
    /// leaves the extent resident, an accepted one marks it gone.
    #[mz_ore::test]
    fn pageout_is_observed_not_trusted() {
        let data = vec![9u64; 10_000];
        let mut extent = SwapExtent::write(&data);
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
        let data: Vec<u64> = (0..10_000).collect();
        let mut extent = SwapExtent::write(&data);
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
