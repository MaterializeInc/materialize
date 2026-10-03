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

//! The extent type and the arena that holds extents in memory, the first
//! home of every extent whatever the pool's backend.
//!
//! An extent is a slot in the pool-owned [`ExtentArena`] holding the stored
//! bytes of one chunk, produced by the chunk's [`ExtentCodec`] (lz4 in
//! practice). "Write" encodes into the slot. The slot stays resident,
//! forming the compressed-but-resident middle tier of the pool's ladder,
//! until the pool's RSS target forces [`Extent::pageout`], which pushes
//! the pages to the swap device with `MADV_PAGEOUT`. "Read" issues
//! `MADV_WILLNEED` ahead of the decode (and makes the pages resident
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
//!
//! On a pool with a file store, enforcement demotes an arena extent to a
//! file slot instead of advising it out: the extent's home moves from
//! `Arena` through `Demoting` (written without the chunk lock) to `File`,
//! which is terminal, and reads go to the file.

use std::alloc::Layout;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pool::file::{self, AlignedBuf, FileSlot, FileStore, WriteError};
use crate::pool::region::{self, Region};
use crate::pool::{ExtentCodec, max_stored_len};

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
/// class that fits [`max_stored_len`], the codec contract's worst case,
/// over the largest chunk size class. Every class is a multiple of `page`
/// (the sub-page mid class between `page` and `2 * page` is skipped), so
/// slot-granular paging advice is exact on any kernel page size.
/// Consecutive classes above the smallest are within 1.5x of each other,
/// which bounds a stored payload's internal fragmentation below 1.5x
/// (page-granular slack at the small end).
pub(crate) fn extent_classes(page: usize) -> Vec<usize> {
    let max_comp = max_stored_len(max_chunk_bytes());
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

    /// Test hook: the number of slots currently allocated across classes.
    #[cfg(test)]
    pub(crate) fn slots_in_use(&self) -> usize {
        self.regions.iter().map(Region::slots_in_use).sum()
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
pub(crate) struct Extent {
    /// Byte size of the backing allocation (the extent's class size, or the
    /// heap layout on the fallback path): the granule the resident
    /// accounting and the pageout operate on.
    alloc_size: usize,
    comp_len: usize,
    /// Whether the extent's pages are (engine-)resident: set at write and by
    /// [`Extent::read_into`], cleared by a [`Extent::pageout`] whose
    /// residency observation found the whole range gone. Meaningful only in
    /// the `Arena` and `Heap` homes, see [`Extent::is_resident`]. Mutated
    /// only under the owning chunk's state mutex.
    resident: bool,
    /// Consecutive pageout passes whose observation found pages still
    /// resident. Reset by [`Extent::read_into`]. At
    /// [`PAGEOUT_RETRY_CAP`] the extent stops being advised out.
    incomplete_passes: u8,
    /// Checksum of the first `comp_len` stored bytes, set when a demotion
    /// commits and verified by every file read.
    crc: u32,
    /// Whether the extent was read from its file since its demotion.
    read_since_demotion: bool,
    home: Home,
}

/// Where an extent's bytes live.
#[derive(Debug)]
enum Home {
    /// A slot in the pool's extent arena.
    Arena {
        arena: Arc<ExtentArena>,
        class: usize,
        slot: u32,
        ptr: *mut u8,
    },
    /// Global-allocator fallback for an exhausted class. Never paged out:
    /// `MADV_PAGEOUT` over allocator-owned pages leaves swap-entry PTEs on
    /// freed ranges for the allocator to recycle into unrelated
    /// allocations, which is the failure the arena exists to avoid.
    Heap { ptr: *mut u8, layout: Layout },
    /// File mode, transient: the arena slot still holds the bytes, which
    /// are immutable and readable while a demoter writes them to
    /// `file_slot` without the chunk lock. The demoter owns the transition
    /// out of this home, so nothing else may drop the extent.
    Demoting {
        arena: Arc<ExtentArena>,
        class: usize,
        slot: u32,
        ptr: *mut u8,
        file_slot: FileSlot,
    },
    /// File mode: the bytes live only in `slot` of `store`. Terminal until
    /// the extent is dropped, so a reader may use the location without the
    /// chunk lock.
    File {
        store: Arc<FileStore>,
        slot: FileSlot,
    },
}

impl Home {
    /// The in-memory copy of the stored bytes, `None` for a file home.
    fn ptr(&self) -> Option<*mut u8> {
        match self {
            Home::Arena { ptr, .. } | Home::Heap { ptr, .. } | Home::Demoting { ptr, .. } => {
                Some(*ptr)
            }
            Home::File { .. } => None,
        }
    }
}

/// Where a `File`-home extent's bytes live, captured under the chunk lock
/// so the read can run without it.
#[derive(Debug)]
pub(crate) struct FileLocation {
    store: Arc<FileStore>,
    slot: FileSlot,
    comp_len: usize,
    crc: u32,
}

/// The arena bytes of a `Demoting` extent, captured under the chunk lock
/// for the demoter's unlocked write.
#[derive(Debug)]
pub(crate) struct DemotionSource {
    ptr: *const u8,
    comp_len: usize,
    alloc_size: usize,
    file_slot: FileSlot,
}

impl DemotionSource {
    /// Checksums the stored bytes and writes them to the file slot,
    /// returning the checksum and the write's result.
    ///
    /// # Safety
    ///
    /// The extent this was captured from must stay in its `Demoting` home
    /// for the duration of the call: not dropped, and not moved to another
    /// home.
    pub(crate) unsafe fn write(&self, store: &FileStore) -> (u32, Result<(), WriteError>) {
        // The write covers whole pages. Extent classes are page multiples,
        // so the arena slot spans at least the rounded length.
        let len = self.comp_len.next_multiple_of(region::page_size());
        assert!(len <= self.alloc_size, "write overruns the arena slot");
        // SAFETY: the arena slot is `alloc_size >= len` bytes of mapped
        // memory owned by the extent, which the caller keeps `Demoting`, so
        // the slot is neither freed nor reused. Its bytes are immutable in
        // that home: nothing writes an extent's slot after `Extent::write`,
        // and concurrent readers only read. Bytes past `comp_len` within the
        // last page are initialized memory of the mapping with unspecified
        // contents. Reads consume only `comp_len` bytes and the checksum
        // covers only those, so the tail's contents do not matter.
        let bytes = unsafe { std::slice::from_raw_parts(self.ptr, len) };
        let crc = file::crc(&bytes[..self.comp_len]);
        (crc, store.write(self.file_slot, bytes, self.comp_len))
    }
}

/// Retention policy for the thread-local compression scratch across
/// [`Extent::write`] calls.
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

// SAFETY: the extent owns its backing (an arena slot handed out by the
// region allocator, a heap allocation, or a file slot), so moving the owner
// across threads is sound. Access goes through the owning chunk's state
// mutex, except for a `Demoting` extent's arena slot, which the demoter
// reads through its `DemotionSource` without the mutex. Those bytes are
// immutable while `Demoting`, the demoter is the only reader outside the
// mutex, and the slot is freed only after the demoter's write finishes.
unsafe impl Send for Extent {}

/// lz4 codec for the pool's own tests, mirroring the production codec that
/// lives with the chunk implementation: a little-endian `u32` body-length
/// prefix followed by one lz4 block.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestLz4Codec;

#[cfg(test)]
pub(crate) static TEST_CODEC: TestLz4Codec = TestLz4Codec;

#[cfg(test)]
impl ExtentCodec for TestLz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("chunk payloads fit u32");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..])
            .expect("output sized to the maximum");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix length");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits usize");
        assert_eq!(
            len,
            body.len(),
            "destination must match the encoded body length"
        );
        let written = lz4_flex::block::decompress_into(&stored[4..], body)
            .expect("stored bytes hold a valid lz4 block");
        assert_eq!(written, body.len(), "decoded length mismatch");
    }
}

impl Extent {
    /// Encodes `data` through `codec` into a fresh extent, preferring an
    /// arena slot and degrading to the heap when the payload's class is
    /// exhausted. The pages stay resident; the pool's RSS-target
    /// enforcement decides when [`Extent::pageout`] pushes them to the
    /// device.
    ///
    /// Encoding goes through a reused thread-local scratch buffer so the
    /// extent slot can be chosen by the *actual* stored payload rather
    /// than the codec's worst case. Worst-case sizing costs ~5.6× on
    /// compressible data, in swap capacity and in swap write bandwidth per
    /// eviction (the whole allocation is paged out), which at hydration
    /// eviction rates backs up device writeback and bloats the working set
    /// with swap-cache pages. `scratch` says whether the caller's thread
    /// keeps the grown scratch for its next job.
    pub(crate) fn write(
        arena: &Arc<ExtentArena>,
        data: &[u64],
        codec: &dyn ExtentCodec,
        scratch: Scratch,
    ) -> Extent {
        use std::cell::RefCell;
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        let bytes: &[u8] = bytemuck::cast_slice(data);
        SCRATCH.with(|cell| {
            let mut buf = cell.borrow_mut();
            codec.encode(bytes, &mut buf);
            let comp_len = buf.len();
            crate::soft_assert_no_log!(
                comp_len <= max_stored_len(bytes.len()),
                "codec output exceeds the extent-store bound",
            );

            let (home, alloc_size) = match arena.alloc(comp_len) {
                Some((class, slot)) => (
                    Home::Arena {
                        arena: Arc::clone(arena),
                        class,
                        slot,
                        ptr: arena.regions[class].slot_ptr(slot),
                    },
                    arena.classes[class],
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
                    (Home::Heap { ptr, layout }, layout.size())
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
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    home.ptr().expect("fresh extent is in memory"),
                    comp_len,
                );
            }
            if scratch == Scratch::Shrink {
                buf.clear();
                // TODO: consider retaining some capacity here (shrinking to
                // the next power of two, say) rather than releasing all of
                // it; measure the realloc traffic before tuning.
                buf.shrink_to_fit();
            }
            Extent {
                alloc_size,
                comp_len,
                resident: true,
                incomplete_passes: 0,
                crc: 0,
                read_since_demotion: false,
                home,
            }
        })
    }

    /// The in-memory copy of the stored bytes. Panics for a `File` extent,
    /// which has none.
    fn ptr(&self) -> *mut u8 {
        self.home.ptr().expect("extent has an in-memory copy")
    }

    /// The byte size of the extent's allocation: the granule the resident
    /// accounting and the pageout operate on.
    pub(crate) fn alloc_size(&self) -> usize {
        self.alloc_size
    }

    /// Whether the extent's pages are engine-resident (not pushed to the
    /// device since the last write or read). A `Demoting` extent is
    /// resident until its demotion commits, and a `File` extent never is.
    pub(crate) fn is_resident(&self) -> bool {
        match self.home {
            Home::Arena { .. } | Home::Heap { .. } => self.resident,
            Home::Demoting { .. } => true,
            Home::File { .. } => false,
        }
    }

    /// Whether the extent sits in an arena slot, resident and not
    /// retry-capped: the state RSS-target enforcement acts on, and the one
    /// in which freeing the extent avoids a device write.
    pub(crate) fn is_reclaimable_arena(&self) -> bool {
        matches!(self.home, Home::Arena { .. }) && self.is_resident() && !self.pageout_capped()
    }

    /// Whether a demoter currently owns the extent.
    pub(crate) fn is_demoting(&self) -> bool {
        matches!(self.home, Home::Demoting { .. })
    }

    /// The file slot's size for a `File` extent, `None` otherwise.
    pub(crate) fn file_bytes(&self) -> Option<usize> {
        match &self.home {
            Home::File { store, slot } => Some(store.class_size(slot.class)),
            Home::Arena { .. } | Home::Heap { .. } | Home::Demoting { .. } => None,
        }
    }

    /// The file location of a `File` extent, `None` otherwise.
    pub(crate) fn file_location(&self) -> Option<FileLocation> {
        match &self.home {
            Home::File { store, slot } => Some(FileLocation {
                store: Arc::clone(store),
                slot: *slot,
                comp_len: self.comp_len,
                crc: self.crc,
            }),
            Home::Arena { .. } | Home::Heap { .. } | Home::Demoting { .. } => None,
        }
    }

    /// The store and slot of a `File` extent, `None` otherwise.
    #[cfg(test)]
    pub(crate) fn file_slot(&self) -> Option<(Arc<FileStore>, FileSlot)> {
        self.file_location()
            .map(|location| (location.store, location.slot))
    }

    /// Records a read of the extent's file, returning whether it was already
    /// read since its demotion.
    pub(crate) fn note_file_read(&mut self) -> bool {
        std::mem::replace(&mut self.read_since_demotion, true)
    }

    /// Moves an `Arena` extent to `Demoting` toward `file_slot` and returns
    /// what the demoter's unlocked write needs. Panics for any other home.
    pub(crate) fn begin_demotion(&mut self, file_slot: FileSlot) -> DemotionSource {
        let Home::Arena {
            arena,
            class,
            slot,
            ptr,
        } = &self.home
        else {
            panic!("only arena extents demote");
        };
        let source = DemotionSource {
            ptr: ptr.cast_const(),
            comp_len: self.comp_len,
            alloc_size: self.alloc_size,
            file_slot,
        };
        self.home = Home::Demoting {
            arena: Arc::clone(arena),
            class: *class,
            slot: *slot,
            ptr: *ptr,
            file_slot,
        };
        source
    }

    /// Commits a demotion whose write succeeded: the extent moves to its
    /// `File` home with checksum `crc`, and its arena slot is freed with its
    /// pages discarded. Panics unless the extent is `Demoting`. The caller
    /// holds the chunk's state lock, so no reader borrows the arena slot.
    pub(crate) fn commit_demotion(&mut self, store: Arc<FileStore>, crc: u32) {
        let Home::Demoting {
            arena,
            class,
            slot,
            ptr,
            file_slot,
        } = &self.home
        else {
            panic!("only demoting extents commit");
        };
        let (arena, class, slot, ptr) = (Arc::clone(arena), *class, *slot, *ptr);
        self.home = Home::File {
            store,
            slot: *file_slot,
        };
        self.crc = crc;
        self.read_since_demotion = false;
        // SAFETY: the extent owned the slot until the home change above, and
        // readers borrow it only under the chunk lock the caller holds.
        unsafe { free_arena_slot(&arena, class, slot, ptr, self.alloc_size) };
    }

    /// Returns a `Demoting` extent to its `Arena` home and hands back the
    /// file slot the demotion reserved, for the caller to return to the
    /// store. Panics unless the extent is `Demoting`.
    pub(crate) fn abort_demotion(&mut self) -> FileSlot {
        let Home::Demoting {
            arena,
            class,
            slot,
            ptr,
            file_slot,
        } = &self.home
        else {
            panic!("only demoting extents abort");
        };
        let file_slot = *file_slot;
        self.home = Home::Arena {
            arena: Arc::clone(arena),
            class: *class,
            slot: *slot,
            ptr: *ptr,
        };
        file_slot
    }

    /// Whether the extent's pageout retry budget is exhausted: consecutive
    /// incomplete passes reached [`PAGEOUT_RETRY_CAP`], so callers stop
    /// calling [`Extent::pageout`] until a read resets the budget.
    /// Heap-fallback extents are permanently capped: they are never advised
    /// out and stay counted resident until freed.
    pub(crate) fn pageout_capped(&self) -> bool {
        match self.home {
            Home::Heap { .. } => true,
            Home::Arena { .. } => self.incomplete_passes >= PAGEOUT_RETRY_CAP,
            // File mode demotes instead of advising pages out.
            Home::Demoting { .. } | Home::File { .. } => false,
        }
    }

    /// Hints the kernel to push the extent's pages to the swap device and
    /// observes the result: returns `true`, marking the extent non-resident,
    /// only when the observation finds the whole range unmapped (a page
    /// whose clean copy lingers in the swap cache counts as reclaimed). An
    /// incomplete pass leaves the extent fully resident for accounting and
    /// spends one unit of the retry budget: a single pinned page keeps the
    /// whole extent counted, which is the safe direction, and the retry
    /// budget exists exactly for such transient pins.
    ///
    /// Callers must not invoke this on a [`Extent::pageout_capped`]
    /// extent.
    pub(crate) fn pageout(&mut self) -> bool {
        crate::soft_assert_no_log!(!self.pageout_capped());
        region::pageout(self.ptr(), self.alloc_size);
        if region::nonresident(self.ptr(), self.alloc_size) {
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
    /// A no-op for `Demoting` and `File` extents: the first is resident, and
    /// the second would need an asynchronous read into a buffer a later read
    /// can claim.
    pub(crate) fn prefetch(&self) {
        match self.home {
            Home::Arena { ptr, .. } | Home::Heap { ptr, .. } => {
                region::willneed(ptr, self.alloc_size)
            }
            Home::Demoting { .. } | Home::File { .. } => {}
        }
    }

    /// Decodes the extent through `codec` into `dst`, which must be exactly
    /// the chunk's body length. Reading an in-memory extent faults its pages
    /// back in, so it is resident again afterwards; the caller owns the
    /// accounting for that transition (the pool re-counts and re-enqueues it
    /// for the RSS target). A `File` extent is read per
    /// [`FileLocation::read_range_into`] and stays on file.
    pub(crate) fn read_into(&mut self, codec: &dyn ExtentCodec, dst: &mut [u8]) {
        self.read_range_into(codec, dst.len(), 0, dst);
    }

    /// Decodes the byte range `[offset, offset + dst.len())` of the
    /// extent's `body_len`-byte body into `dst`. The range must lie within
    /// the body, and `body_len` must be the body's exact length (the codec
    /// validates it against the stored form). Residency effects are those
    /// of [`Extent::read_into`] regardless of the range: the stored
    /// form is one whole codec block, so any read faults (or reads from
    /// file) and decodes the entire extent, and a sub-range only narrows
    /// the final copy.
    pub(crate) fn read_range_into(
        &mut self,
        codec: &dyn ExtentCodec,
        body_len: usize,
        offset: usize,
        dst: &mut [u8],
    ) {
        if let Some(location) = self.file_location() {
            location.read_range_into(codec, body_len, offset, dst);
            return;
        }
        self.resident = true;
        // The decode faults every page back in, so prior incomplete
        // pageout passes no longer describe the mapping and the retry
        // budget starts over.
        self.incomplete_passes = 0;
        self.prefetch();
        // SAFETY: the extent exclusively owns its in-memory copy, and the
        // first `comp_len` bytes were initialized by `write`. A `Demoting`
        // extent's arena bytes are immutable while the demoter writes them
        // out, so this read may overlap that write.
        let buf = unsafe { std::slice::from_raw_parts(self.ptr(), self.comp_len) };
        decode_range(buf, codec, body_len, offset, dst);
    }
}

impl FileLocation {
    /// Reads the stored bytes from file, verifies their checksum, and
    /// decodes the byte range `[offset, offset + dst.len())` of the
    /// `body_len`-byte body into `dst`, per [`Extent::read_range_into`].
    /// Panics on an I/O error, a short read, or a checksum mismatch.
    pub(crate) fn read_range_into(
        &self,
        codec: &dyn ExtentCodec,
        body_len: usize,
        offset: usize,
        dst: &mut [u8],
    ) {
        use std::cell::RefCell;
        thread_local! {
            static READ_BUF: RefCell<AlignedBuf> = const { RefCell::new(AlignedBuf::new()) };
        }
        READ_BUF.with(|cell| {
            let mut buf = cell.borrow_mut();
            self.store
                .read(self.slot, self.comp_len, self.crc, &mut buf);
            decode_range(buf.as_slice(), codec, body_len, offset, dst);
            // The decode scratch's policy: reads run on worker threads, so
            // capacity beyond the ~2 MiB chunk target is released.
            buf.shrink_above(2 << 20);
        });
    }
}

/// Decodes the byte range `[offset, offset + dst.len())` of the
/// `body_len`-byte body whose stored form is `stored` into `dst`.
fn decode_range(
    stored: &[u8],
    codec: &dyn ExtentCodec,
    body_len: usize,
    offset: usize,
    dst: &mut [u8],
) {
    let end = offset
        .checked_add(dst.len())
        .expect("range end overflows usize");
    assert!(
        end <= body_len,
        "range end {end} exceeds the extent's body length {body_len}",
    );
    if offset == 0 && dst.len() == body_len {
        codec.decode(stored, dst);
        return;
    }
    // A sub-range still decodes the whole block, into a reused
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
        scratch.resize(body_len, 0);
        codec.decode(stored, &mut scratch);
        dst.copy_from_slice(&scratch[offset..end]);
        if scratch.capacity() > 2 << 20 {
            scratch.clear();
            scratch.shrink_to_fit();
        }
    });
}

/// Returns an arena slot to its region with its pages discarded.
/// Discarding the pages also drops any copy on the swap device
/// (`MADV_DONTNEED` frees an anonymous range's swap entries), so the slot
/// returns to the free list with no dead compressed data left to fault back
/// in.
///
/// # Safety
///
/// The caller owned the slot, and no reference into it exists.
unsafe fn free_arena_slot(
    arena: &ExtentArena,
    class: usize,
    slot: u32,
    ptr: *mut u8,
    alloc_size: usize,
) {
    // SAFETY: per the function contract.
    unsafe {
        region::dontneed(ptr, alloc_size);
    }
    arena.regions[class].free(slot, false);
}

/// The heap layout of a fallback extent for a compressed payload of
/// `comp_len` bytes.
fn heap_layout(comp_len: usize) -> Layout {
    Layout::array::<u8>(comp_len).expect("valid extent layout")
}

impl Drop for Extent {
    fn drop(&mut self) {
        match &self.home {
            Home::Arena {
                arena,
                class,
                slot,
                ptr,
            } => {
                // SAFETY: the extent exclusively owns the slot and is being
                // dropped, so no reference into it exists.
                unsafe { free_arena_slot(arena, *class, *slot, *ptr, self.alloc_size) };
            }
            Home::Heap { ptr, layout } => {
                // SAFETY: `ptr` was returned by `alloc` with exactly this
                // `layout` in `write` and is deallocated exactly once, here.
                unsafe {
                    std::alloc::dealloc(*ptr, *layout);
                }
            }
            Home::File { store, slot } => store.free(*slot),
            Home::Demoting { .. } => {
                // The demoter reads the arena slot without the chunk lock
                // and writes the file slot. Freeing either here would hand
                // it to a new owner mid-I/O, so both leak instead.
                crate::soft_panic_or_log!("dropped an extent mid-demotion");
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
    /// class is a page multiple, the top class fits the codec contract's
    /// worst case over the largest chunk class, a class is found for every
    /// payload up to that bound and fits it, and the selected class
    /// overshoots the payload by less than 1.5x (with page-granular slack
    /// at the smallest classes).
    #[kani::proof]
    #[kani::unwind(64)]
    fn extent_ladder_fits_payloads() {
        let page_shift: u32 = kani::any();
        kani::assume(page_shift >= 12 && page_shift <= 16);
        let page = 1usize << page_shift;
        let classes = extent_classes(page);
        let max_comp = max_stored_len(max_chunk_bytes());
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
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        assert!(extent.comp_len() > 4);
        extent.prefetch();
        let mut out = vec![0u64; data.len()];
        extent.read_into(&TEST_CODEC, bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    #[mz_ore::test]
    fn compressible_data_shrinks() {
        let arena = arena();
        let data = vec![42u64; 100_000];
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        assert!(extent.comp_len() < data.len() * 8 / 4);
        let mut out = vec![0u64; data.len()];
        extent.read_into(&TEST_CODEC, bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
    }

    /// The slot is sized to the compressed payload, not lz4's worst case:
    /// extents must cost swap capacity and write bandwidth in proportion to
    /// what they store.
    #[mz_ore::test]
    fn allocation_is_sized_to_payload() {
        let arena = arena();
        let data = vec![7u64; 100_000];
        let extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
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

    #[mz_ore::test]
    fn ladder_shape() {
        for page in [4096usize, 16384, 65536] {
            let classes = extent_classes(page);
            let max_comp = max_stored_len(max_chunk_bytes());
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
        let a = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        assert_eq!(arena.fallbacks(), 0);
        assert!(!a.pageout_capped(), "arena extents start with retry budget");
        let mut b = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        assert_eq!(arena.fallbacks(), 1, "second same-class write degrades");
        assert!(b.pageout_capped(), "heap extents are never advised out");
        assert!(b.is_resident());
        let mut out = vec![0u64; data.len()];
        b.read_into(&TEST_CODEC, bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
        // Freeing the arena extent frees its slot for the next write.
        drop(a);
        let c = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
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
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        let mut full = vec![0u8; bytes.len()];
        extent.read_into(&TEST_CODEC, &mut full);
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
            extent.read_range_into(&TEST_CODEC, bytes.len(), offset, &mut out);
            assert_eq!(out, &full[offset..offset + len], "range ({offset}, {len})");
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "range end")]
    fn ranged_read_out_of_bounds_panics() {
        let arena = arena();
        let data = vec![5u64; 64];
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        let mut out = vec![0u8; 16];
        extent.read_range_into(&TEST_CODEC, 64 * 8, 64 * 8 - 8, &mut out);
    }

    #[mz_ore::test]
    #[should_panic(expected = "destination must match")]
    fn wrong_destination_length_panics() {
        let arena = arena();
        let data = vec![1u64; 16];
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        let mut out = vec![0u64; 8];
        extent.read_into(&TEST_CODEC, bytemuck::cast_slice_mut(&mut out));
    }

    #[mz_ore::test]
    fn pageout_is_observed_not_trusted() {
        let arena = arena();
        let data = vec![9u64; 10_000];
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
        assert!(extent.is_resident());
        region::fake_residency::decline_next(1);
        assert!(!extent.pageout(), "declined pass reports incomplete");
        assert!(extent.is_resident(), "declined pass leaves it resident");
        assert!(!extent.pageout_capped());
        assert!(extent.pageout(), "accepted pass reports reclaimed");
        assert!(!extent.is_resident());
    }

    #[mz_ore::test]
    fn pageout_retry_cap_and_read_reset() {
        let arena = arena();
        let data: Vec<u64> = (0..10_000).collect();
        let mut extent = Extent::write(&arena, &data, &TEST_CODEC, Scratch::Shrink);
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
        extent.read_into(&TEST_CODEC, bytemuck::cast_slice_mut(&mut out));
        assert_eq!(out, data);
        assert!(!extent.pageout_capped(), "a read resets the retry budget");
        assert!(extent.pageout());
    }
}
