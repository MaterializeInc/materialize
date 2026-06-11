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
//! bytes of one chunk. "Write" compresses into the allocation and issues
//! `MADV_PAGEOUT`, pushing the pages to the swap device; "read" issues
//! `MADV_WILLNEED` ahead of the decompress; "free" is a plain deallocation,
//! with any swapped copy discarded for free.

use std::alloc::Layout;

use crate::cast::CastFrom;
use crate::pool::region;

/// Alignment and size granule of extent allocations.
const EXTENT_ALIGN: usize = 4096;

/// Length in bytes of the little-endian `u32` uncompressed-size prefix that
/// precedes the compressed bytes, matching the
/// `lz4_flex::block::compress_prepend_size` framing.
const SIZE_PREFIX: usize = 4;

/// One chunk's compressed backing copy.
#[derive(Debug)]
pub(crate) struct SwapExtent {
    ptr: *mut u8,
    layout: Layout,
    comp_len: usize,
}

// SAFETY: the extent exclusively owns its allocation; nothing else holds a
// pointer into it, so moving the owner across threads is sound. All access
// goes through the owning chunk's state mutex.
unsafe impl Send for SwapExtent {}

impl SwapExtent {
    /// Compresses `data` into a fresh extent and hints the kernel to push the
    /// extent's pages to the swap device.
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

            let size = comp_len.next_multiple_of(EXTENT_ALIGN);
            let layout = Layout::from_size_align(size, EXTENT_ALIGN).expect("valid extent layout");
            // SAFETY: `layout` has nonzero size (`size` is at least one granule).
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // SAFETY: `ptr` is a fresh allocation of `size >= comp_len` bytes
            // exclusively owned here; the copy plus the tail zeroing below
            // initialize every byte, so later borrows of the allocation (in
            // `read_into`) see initialized memory. The source is the scratch
            // buffer, which cannot alias the fresh allocation.
            unsafe {
                std::ptr::copy_nonoverlapping(scratch.as_ptr(), ptr, comp_len);
                std::ptr::write_bytes(ptr.add(comp_len), 0, size - comp_len);
            }
            region::pageout(ptr, size);
            SwapExtent {
                ptr,
                layout,
                comp_len,
            }
        })
    }

    /// Test-only: the byte size of the extent's allocation.
    #[cfg(test)]
    pub(crate) fn alloc_size(&self) -> usize {
        self.layout.size()
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
    /// uncompressed length.
    pub(crate) fn read_into(&self, dst: &mut [u8]) {
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

impl Drop for SwapExtent {
    fn drop(&mut self) {
        // SAFETY: `ptr` was returned by `alloc` with exactly this `layout`
        // in `write` and is deallocated exactly once, here.
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // madvise is a foreign call
    fn round_trip() {
        let data: Vec<u64> = (0..10_000).map(|i| i * 37).collect();
        let extent = SwapExtent::write(&data);
        assert!(extent.comp_len() > SIZE_PREFIX);
        extent.prefetch();
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // madvise is a foreign call
    fn compressible_data_shrinks() {
        let data = vec![42u64; 100_000];
        let extent = SwapExtent::write(&data);
        assert!(extent.comp_len() < data.len() * 8 / 4);
        let mut out = vec![0u64; data.len()];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    /// The allocation is sized to the compressed payload, not lz4's worst
    /// case: extents must cost swap capacity and write bandwidth in
    /// proportion to what they store.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // madvise is a foreign call
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
    #[cfg_attr(miri, ignore)] // madvise is a foreign call
    #[should_panic(expected = "destination must match")]
    fn wrong_destination_length_panics() {
        let data = vec![1u64; 16];
        let extent = SwapExtent::write(&data);
        let mut out = vec![0u64; 8];
        extent.read_into(bytemuck::cast_slice_mut(&mut out));
    }
}
