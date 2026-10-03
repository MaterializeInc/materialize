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

//! The file-backed extent store: a userspace extent allocator over one
//! anonymous file per extent class in a scratch directory.
//!
//! **Files.** Each class of the [`extent_classes`] ladder gets one file,
//! opened `O_TMPFILE` so it has no name and the kernel frees its blocks when
//! the process exits, crash included. Where the filesystem refuses
//! `O_TMPFILE`, the store creates a uniquely named file and unlinks it
//! immediately. A slot `(class, index)` lives at byte offset
//! `index * class_size` of its class's file.
//!
//! **Block ownership.** Each class allocates slot indices with a
//! [`SlotAllocator`] behind its own mutex. Its warm side holds free slots
//! whose blocks are still allocated, its cold side free slots whose blocks
//! were punched, and slots above its high-water mark were never touched.
//! Freeing a slot pushes it warm with no filesystem call, so slot reuse
//! performs no metadata operation. A slot taken cold or from the high-water
//! mark is `fallocate`d before it is handed out, which turns `ENOSPC` into
//! an allocation failure before any data is in flight.
//!
//! **Capacity.** Every slot with allocated blocks, in use or warm, is
//! charged at its class size against the store's capacity. When an
//! allocation does not fit, the store punches holes over warm slots of the
//! other class holding the most warm bytes, moving them cold, until it
//! fits. This returns space stranded by a shift in the class mix exactly
//! when it blocks an allocation, and never otherwise.
//!
//! **Alignment.** Class sizes are page multiples, and every transfer covers
//! whole pages from a page-aligned buffer, which satisfies `O_DIRECT` on
//! devices with 512-byte and 4 KiB logical blocks. `open` probes one
//! page-aligned write and read. A probe failing with `EINVAL` means the
//! device needs a larger alignment or the filesystem lacks direct I/O, and
//! the store reopens its files buffered: each write is then followed by a
//! synchronous writeback and a page-cache drop over its range. Memory-backed
//! filesystems are rejected by type, since tmpfs accepts the probe and would
//! hold extents in RAM.
//!
//! **Errors.** Running out of capacity, or `ENOSPC` from `fallocate`, fails
//! the allocation with [`AllocError::Full`]. An `ENOSPC` from a write fails
//! it with [`WriteError::Full`] after the store punches and takes back the
//! slot. Either `ENOSPC` additionally lowers the capacity to the bytes
//! allocated at that moment, permanently. Any other write or `fallocate`
//! error disables writes for the store's lifetime. Data already written
//! stays readable. A read error, short read, or checksum mismatch panics:
//! there is no correct value to return, and the pool's contents are
//! recreatable.

use std::alloc::Layout;
use std::fs::File;
use std::io;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use itertools::Itertools;

use crate::cast::CastFrom;
use crate::pool::extent::extent_classes;
use crate::pool::region::{SlotAllocator, page_size};

/// Where the store's I/O goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoMode {
    /// `O_DIRECT`: transfers bypass the page cache.
    Direct,
    /// Page-cache I/O, written back and dropped after each write.
    Buffered,
}

/// A slot in the store: `index` in the file of extent class `class`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileSlot {
    pub(crate) class: usize,
    pub(crate) index: u32,
}

/// Why an allocation failed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AllocError {
    /// The store's capacity, or the filesystem, has no room for the slot.
    Full,
    /// An earlier I/O error disabled writes for the store's lifetime.
    WritesDisabled,
}

/// Why a write failed.
#[derive(Debug)]
pub(crate) enum WriteError {
    /// The filesystem had no room for the slot's data. The store took the
    /// slot back, so the caller must not free it.
    Full,
    /// Writes are disabled for the store's lifetime, by this write's I/O
    /// error or an earlier one.
    Disabled,
}

/// Number of read-latency buckets. Bucket `i < READ_LATENCY_BUCKETS - 1`
/// counts reads that took less than `32 µs << i` and, for `i > 0`, at least
/// `16 µs << i`. The last bucket counts reads of 65.536 ms and more.
pub const READ_LATENCY_BUCKETS: usize = 13;

/// A snapshot of the store's counters.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FileStoreStats {
    pub(crate) writes: u64,
    pub(crate) reads: u64,
    /// Bytes transferred by reads, page-rounded.
    pub(crate) read_bytes: u64,
    pub(crate) read_latency_buckets: [u64; READ_LATENCY_BUCKETS],
    /// I/O errors that disabled writes.
    pub(crate) write_errors: u64,
    pub(crate) holes_punched_bytes: u64,
}

/// The checksum the store verifies on every read.
pub(crate) fn crc(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// The bucket of [`FileStoreStats::read_latency_buckets`] for a read that
/// took `micros` microseconds.
fn read_latency_bucket(micros: u64) -> usize {
    // `floor(log2(micros / 16))`, with everything below 32 µs in bucket 0.
    let log2 = (micros / 16).checked_ilog2().unwrap_or(0);
    usize::cast_from(log2).min(READ_LATENCY_BUCKETS - 1)
}

/// One extent class: its file and slot allocator.
#[derive(Debug)]
struct Class {
    size: usize,
    file: File,
    slots: Mutex<SlotAllocator>,
    /// Bytes on the warm side of `slots`, updated under its mutex and read
    /// without it.
    warm_bytes: AtomicU64,
}

impl Class {
    fn slots(&self) -> std::sync::MutexGuard<'_, SlotAllocator> {
        self.slots.lock().expect("file store allocator poisoned")
    }
}

#[derive(Debug, Default)]
struct Counters {
    writes: AtomicU64,
    reads: AtomicU64,
    read_bytes: AtomicU64,
    read_latency_buckets: [AtomicU64; READ_LATENCY_BUCKETS],
    write_errors: AtomicU64,
    holes_punched_bytes: AtomicU64,
}

/// The file-backed extent store. See the module documentation.
#[derive(Debug)]
pub(crate) struct FileStore {
    classes: Vec<Class>,
    io_mode: IoMode,
    page: usize,
    capacity: AtomicU64,
    /// Bytes of slots with allocated blocks, in use or warm. Never exceeds
    /// `capacity` except transiently after `ENOSPC` lowers it.
    allocated: AtomicU64,
    writes_disabled: AtomicBool,
    enospc_seen: AtomicBool,
    counters: Counters,
}

impl FileStore {
    /// Opens one anonymous file per extent class in `dir`. `capacity_bytes`
    /// `None` derives capacity from the volume: the bytes available to
    /// unprivileged writers, minus the larger of 1 GiB and 2% of them.
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] when `dir` is on a
    /// memory-backed filesystem or the platform is not Linux.
    pub(crate) fn open(dir: &Path, capacity_bytes: Option<u64>) -> io::Result<FileStore> {
        if sys::is_memory_backed(dir)? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "scratch directory is memory-backed",
            ));
        }
        let capacity = match capacity_bytes {
            Some(capacity) => capacity,
            None => {
                let available = sys::available_bytes(dir)?;
                // Headroom for filesystem metadata of the preallocated
                // extents and for incidental writers sharing the volume.
                available.saturating_sub(std::cmp::max(1 << 30, available / 50))
            }
        };
        let page = page_size();
        let sizes = extent_classes(page);
        let (files, io_mode) = match open_files(dir, sizes.len(), page, true) {
            Ok(files) => (files, IoMode::Direct),
            Err(err) if err.raw_os_error() == Some(libc::EINVAL) => {
                tracing::warn!(
                    dir = %dir.display(),
                    "pool file store: direct I/O unsupported ({err}), using buffered I/O",
                );
                (open_files(dir, sizes.len(), page, false)?, IoMode::Buffered)
            }
            Err(err) => return Err(err),
        };
        let classes = sizes
            .into_iter()
            .zip_eq(files)
            .map(|(size, file)| Class {
                size,
                file,
                slots: Mutex::new(SlotAllocator::new(u32::MAX)),
                warm_bytes: AtomicU64::new(0),
            })
            .collect();
        Ok(FileStore {
            classes,
            io_mode,
            page,
            capacity: AtomicU64::new(capacity),
            allocated: AtomicU64::new(0),
            writes_disabled: AtomicBool::new(false),
            enospc_seen: AtomicBool::new(false),
            counters: Counters::default(),
        })
    }

    pub(crate) fn io_mode(&self) -> IoMode {
        self.io_mode
    }

    /// The class for a stored payload of `comp_len` bytes, `None` if none fits.
    pub(crate) fn class_for(&self, comp_len: usize) -> Option<usize> {
        self.classes.iter().position(|c| c.size >= comp_len)
    }

    pub(crate) fn class_size(&self, class: usize) -> usize {
        self.classes[class].size
    }

    /// Reserves a slot with allocated blocks.
    pub(crate) fn alloc(&self, class: usize) -> Result<FileSlot, AllocError> {
        if self.writes_disabled() {
            return Err(AllocError::WritesDisabled);
        }
        let c = &self.classes[class];
        let size = u64::cast_from(c.size);
        let allocated = {
            let mut slots = c.slots();
            let allocated = slots.alloc();
            if let Some((_, true)) = allocated {
                c.warm_bytes.fetch_sub(size, Ordering::Relaxed);
            }
            allocated
        };
        let Some((index, warm)) = allocated else {
            // Every `u32` index of the class is in use, far beyond any
            // capacity a volume provides.
            return Err(AllocError::Full);
        };
        let slot = FileSlot { class, index };
        if warm {
            return Ok(slot);
        }
        // A cold or never-touched slot has no blocks.
        if !self.reserve(size) {
            self.punch_for(class, size);
            if !self.reserve(size) {
                c.slots().free(index, false);
                return Err(AllocError::Full);
            }
        }
        match sys::fallocate(&c.file, self.offset(slot), c.size) {
            Ok(()) => Ok(slot),
            Err(err) => {
                self.allocated.fetch_sub(size, Ordering::Relaxed);
                c.slots().free(index, false);
                if err.raw_os_error() == Some(libc::ENOSPC) {
                    self.lower_capacity("fallocate");
                    Err(AllocError::Full)
                } else {
                    self.disable_writes("fallocate", &err);
                    Err(AllocError::WritesDisabled)
                }
            }
        }
    }

    /// Returns a slot. Its blocks stay allocated. Must not be called while a
    /// write to the slot is in flight.
    pub(crate) fn free(&self, slot: FileSlot) {
        let c = &self.classes[slot.class];
        let mut slots = c.slots();
        slots.free(slot.index, true);
        c.warm_bytes
            .fetch_add(u64::cast_from(c.size), Ordering::Relaxed);
    }

    /// Writes `buf[..round_up(len, page)]` to `slot`. `buf` must be at least
    /// that long, and page aligned in [`IoMode::Direct`]. Fails without I/O
    /// once writes are disabled. On `ENOSPC`, takes the slot back and fails
    /// with [`WriteError::Full`]. On any other I/O error, disables further
    /// writes for the store's lifetime.
    pub(crate) fn write(&self, slot: FileSlot, buf: &[u8], len: usize) -> Result<(), WriteError> {
        if self.writes_disabled() {
            return Err(WriteError::Disabled);
        }
        let c = &self.classes[slot.class];
        let n = len.next_multiple_of(self.page);
        assert!(
            n <= c.size,
            "write of {len} bytes exceeds class size {}",
            c.size
        );
        assert!(
            n <= buf.len(),
            "write of {n} bytes from a {}-byte buffer",
            buf.len()
        );
        if self.io_mode == IoMode::Direct {
            assert_eq!(
                buf.as_ptr().addr() % self.page,
                0,
                "write buffer is not page aligned"
            );
        }
        let offset = self.offset(slot);
        let result = write_all(&c.file, &buf[..n], offset).and_then(|()| match self.io_mode {
            IoMode::Direct => Ok(()),
            IoMode::Buffered => sys::writeback_and_drop(&c.file, offset, n),
        });
        match result {
            Ok(()) => {
                self.counters.writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err) if err.raw_os_error() == Some(libc::ENOSPC) => {
                self.reclaim_after_enospc(slot);
                Err(WriteError::Full)
            }
            Err(err) => {
                self.disable_writes("write", &err);
                Err(WriteError::Disabled)
            }
        }
    }

    /// Takes back `slot` after a write to it hit `ENOSPC`, and lowers the
    /// capacity.
    fn reclaim_after_enospc(&self, slot: FileSlot) {
        let c = &self.classes[slot.class];
        let size = u64::cast_from(c.size);
        // A filesystem that allocates on overwrite (copy-on-write, or thin
        // provisioning) can fail the write despite the `fallocate`. Its
        // blocks are then in an unknown state, and a warm slot would hand
        // the same failure to the next demotion of this class. Punching
        // returns the blocks and makes the slot cold.
        match sys::punch_hole(&c.file, self.offset(slot), c.size) {
            Ok(()) => {
                c.slots().free(slot.index, false);
                self.allocated.fetch_sub(size, Ordering::Relaxed);
                self.counters
                    .holes_punched_bytes
                    .fetch_add(size, Ordering::Relaxed);
            }
            Err(err) => {
                tracing::warn!("pool file store: punching a slot after ENOSPC failed: {err}");
                c.slots().free(slot.index, true);
                c.warm_bytes.fetch_add(size, Ordering::Relaxed);
            }
        }
        self.lower_capacity("write");
    }

    /// Lowers the capacity to the bytes allocated now, after `op` hit
    /// `ENOSPC`.
    fn lower_capacity(&self, op: &str) {
        // The headroom was too small for this filesystem. The bytes
        // allocated now are evidently what fits.
        let allocated = self.allocated.load(Ordering::Relaxed);
        self.capacity.fetch_min(allocated, Ordering::Relaxed);
        if !self.enospc_seen.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                capacity = allocated,
                "pool file store: {op} hit ENOSPC, lowering capacity",
            );
        }
    }

    /// Reads the slot's first `len` stored bytes into `dst` (resized to
    /// `len`), verifying `crc`. Panics on I/O error, short read, or checksum
    /// mismatch.
    pub(crate) fn read(&self, slot: FileSlot, len: usize, crc: u32, dst: &mut AlignedBuf) {
        let c = &self.classes[slot.class];
        let n = len.next_multiple_of(self.page);
        assert!(
            n <= c.size,
            "read of {len} bytes exceeds class size {}",
            c.size
        );
        let offset = self.offset(slot);
        dst.resize(n);
        let start = Instant::now();
        let mut done = 0;
        while done < n {
            match sys::pread(
                &c.file,
                &mut dst.as_mut_slice()[done..],
                offset + u64::cast_from(done),
            ) {
                Ok(0) => panic!(
                    "pool file store: short read: class {}, index {}, offset {offset}, \
                     {done} of {n} bytes",
                    slot.class, slot.index,
                ),
                Ok(k) => done += k,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => panic!(
                    "pool file store: read failed: class {}, index {}, offset {offset}: {err}",
                    slot.class, slot.index,
                ),
            }
        }
        let micros = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        if self.io_mode == IoMode::Buffered {
            // The read left the range in the page cache, charged to the
            // process's cgroup. A failed advice leaves the read correct.
            let _ = sys::drop_cache(&c.file, offset, n);
        }
        dst.truncate(len);
        let observed = self::crc(dst.as_slice());
        assert!(
            observed == crc,
            "pool file store: checksum mismatch: class {}, index {}, offset {offset}, \
             expected {crc:#010x}, observed {observed:#010x}",
            slot.class,
            slot.index,
        );
        self.counters.reads.fetch_add(1, Ordering::Relaxed);
        self.counters
            .read_bytes
            .fetch_add(u64::cast_from(n), Ordering::Relaxed);
        self.counters.read_latency_buckets[read_latency_bucket(micros)]
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn allocated_bytes(&self) -> u64 {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Test hook: the number of slots allocated across classes and not
    /// free.
    #[cfg(test)]
    pub(crate) fn slots_in_use(&self) -> usize {
        self.classes.iter().map(|c| c.slots().in_use()).sum()
    }

    pub(crate) fn writes_disabled(&self) -> bool {
        self.writes_disabled.load(Ordering::Relaxed)
    }

    /// Whether an `alloc` of `class` could currently succeed without I/O
    /// errors: writes enabled and either a warm free slot, capacity room, or
    /// room after punching other classes' warm slots exists. Takes no lock.
    pub(crate) fn can_alloc(&self, class: usize) -> bool {
        if self.writes_disabled() {
            return false;
        }
        let c = &self.classes[class];
        let size = u64::cast_from(c.size);
        if c.warm_bytes.load(Ordering::Relaxed) > 0 || self.fits(size) {
            return true;
        }
        let punchable: u64 = self
            .classes
            .iter()
            .enumerate()
            .filter(|&(i, _)| i != class)
            .map(|(_, c)| c.warm_bytes.load(Ordering::Relaxed))
            .sum();
        self.allocated
            .load(Ordering::Relaxed)
            .saturating_sub(punchable)
            .saturating_add(size)
            <= self.capacity_bytes()
    }

    /// Whether [`FileStore::can_alloc`] holds for some class.
    pub(crate) fn can_alloc_any(&self) -> bool {
        (0..self.classes.len()).any(|class| self.can_alloc(class))
    }

    pub(crate) fn stats(&self) -> FileStoreStats {
        let c = &self.counters;
        FileStoreStats {
            writes: c.writes.load(Ordering::Relaxed),
            reads: c.reads.load(Ordering::Relaxed),
            read_bytes: c.read_bytes.load(Ordering::Relaxed),
            read_latency_buckets: std::array::from_fn(|i| {
                c.read_latency_buckets[i].load(Ordering::Relaxed)
            }),
            write_errors: c.write_errors.load(Ordering::Relaxed),
            holes_punched_bytes: c.holes_punched_bytes.load(Ordering::Relaxed),
        }
    }

    /// Overwrites the slot's first page with `0xFF`, bypassing the fault
    /// seam.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn corrupt(&self, slot: FileSlot) {
        use std::os::unix::fs::FileExt;
        let garbage = AlignedBuf::from_bytes(&vec![0xFF; self.page]);
        self.classes[slot.class]
            .file
            .write_all_at(garbage.as_slice(), self.offset(slot))
            .expect("corrupt slot");
    }

    fn offset(&self, slot: FileSlot) -> u64 {
        u64::cast_from(slot.index) * u64::cast_from(self.classes[slot.class].size)
    }

    fn fits(&self, size: u64) -> bool {
        self.allocated.load(Ordering::Relaxed).saturating_add(size) <= self.capacity_bytes()
    }

    /// Charges `size` bytes against capacity, or returns `false` if they do
    /// not fit.
    fn reserve(&self, size: u64) -> bool {
        self.allocated
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |allocated| {
                allocated
                    .checked_add(size)
                    .filter(|&n| n <= self.capacity_bytes())
            })
            .is_ok()
    }

    /// Punches warm slots of classes other than `class` until `size` more
    /// bytes fit or no other class has a warm slot.
    fn punch_for(&self, class: usize, size: u64) {
        while !self.fits(size) {
            // Choose from the lock-free warm-bytes snapshot: holding two
            // class mutexes at once could deadlock against a concurrent
            // punch.
            let victim = self
                .classes
                .iter()
                .enumerate()
                .filter(|&(i, _)| i != class)
                .map(|(i, c)| (c.warm_bytes.load(Ordering::Relaxed), i))
                .filter(|&(warm_bytes, _)| warm_bytes > 0)
                .max();
            let Some((_, victim)) = victim else {
                return;
            };
            let c = &self.classes[victim];
            let mut slots = c.slots();
            while !self.fits(size) {
                let Some(index) = slots.pop_warm() else {
                    break;
                };
                let slot = FileSlot {
                    class: victim,
                    index,
                };
                if let Err(err) = sys::punch_hole(&c.file, self.offset(slot), c.size) {
                    slots.free(index, true);
                    tracing::warn!("pool file store: punching a free slot failed: {err}");
                    return;
                }
                slots.free(index, false);
                let class_size = u64::cast_from(c.size);
                c.warm_bytes.fetch_sub(class_size, Ordering::Relaxed);
                self.allocated.fetch_sub(class_size, Ordering::Relaxed);
                self.counters
                    .holes_punched_bytes
                    .fetch_add(class_size, Ordering::Relaxed);
            }
        }
    }

    fn disable_writes(&self, op: &str, err: &io::Error) {
        self.counters.write_errors.fetch_add(1, Ordering::Relaxed);
        if !self.writes_disabled.swap(true, Ordering::Relaxed) {
            tracing::warn!("pool file store: {op} failed, disabling writes: {err}");
        }
    }
}

/// Opens `classes` anonymous files and probes the first. With
/// `direct`, an `EINVAL` from the open or the probe means direct I/O is
/// unusable here.
fn open_files(dir: &Path, classes: usize, page: usize, direct: bool) -> io::Result<Vec<File>> {
    let files = (0..classes)
        .map(|class| sys::open_anonymous(dir, class, direct))
        .collect::<io::Result<Vec<_>>>()?;
    probe(&files[0], page)?;
    Ok(files)
}

/// Allocates, writes, reads back, and punches one page at offset 0.
fn probe(file: &File, page: usize) -> io::Result<()> {
    let pattern: Vec<u8> = (0..page)
        .map(|i| u8::try_from(i % 251).expect("fits"))
        .collect();
    let src = AlignedBuf::from_bytes(&pattern);
    sys::fallocate(file, 0, page)?;
    write_all(file, src.as_slice(), 0)?;
    let mut dst = AlignedBuf::new();
    dst.resize(page);
    let mut done = 0;
    while done < page {
        match sys::pread(file, &mut dst.as_mut_slice()[done..], u64::cast_from(done)) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(k) => done += k,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    if dst.as_slice() != src.as_slice() {
        return Err(io::Error::other("probe read back different bytes"));
    }
    sys::punch_hole(file, 0, page)
}

/// Writes all of `buf` at `offset`, retrying on `EINTR` and short writes.
fn write_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        match sys::pwrite(file, &buf[done..], offset + u64::cast_from(done)) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(k) => done += k,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Page-aligned, growable byte buffer for `O_DIRECT` reads.
#[derive(Debug)]
pub(crate) struct AlignedBuf {
    /// Valid for `cap` initialized bytes, or dangling when `cap == 0`.
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: `AlignedBuf` exclusively owns its allocation, like `Vec<u8>`.
unsafe impl Send for AlignedBuf {}
// SAFETY: shared access only reads through `as_slice`, like `Vec<u8>`.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    pub(crate) const fn new() -> AlignedBuf {
        AlignedBuf {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `cap >= len` initialized bytes, or
        // dangling and well aligned with `len == 0`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_slice`, and `&mut self` makes the borrow unique.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Releases the allocation if its capacity exceeds `max` bytes, which
    /// also empties the buffer.
    pub(crate) fn shrink_above(&mut self, max: usize) {
        if self.cap > max {
            self.release();
        }
    }

    /// Sets the length to `len`, growing capacity to a page multiple if
    /// needed. Contents are unspecified but initialized.
    fn resize(&mut self, len: usize) {
        if len > self.cap {
            self.release();
            let cap = len.next_multiple_of(page_size());
            let layout = Self::layout(cap);
            // SAFETY: `layout` has nonzero size because `len > 0`. Zeroing
            // keeps every byte up to `cap` initialized.
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            self.ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
            self.cap = cap;
        }
        self.len = len;
    }

    fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }

    fn release(&mut self) {
        if self.cap > 0 {
            // SAFETY: `ptr` was allocated in `resize` with exactly this
            // layout, and no borrow outlives `&mut self`.
            unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.cap)) };
        }
        // NOTE: assigning `*self` would drop the old value and recurse
        // through `Drop`.
        self.ptr = NonNull::dangling();
        self.len = 0;
        self.cap = 0;
    }

    fn layout(cap: usize) -> Layout {
        Layout::from_size_align(cap, page_size()).expect("valid aligned buffer layout")
    }

    /// A buffer holding a copy of `bytes`.
    pub(crate) fn from_bytes(bytes: &[u8]) -> AlignedBuf {
        let mut buf = AlignedBuf::new();
        buf.resize(bytes.len());
        buf.as_mut_slice().copy_from_slice(bytes);
        buf
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        self.release();
    }
}

/// A temporary directory on a disk-backed filesystem, for tests: the store
/// rejects tmpfs, which is a common `/tmp`. Uses `$MZ_POOL_TEST_DIR` if set,
/// else the system temporary directory if it is disk-backed, else
/// `pool-test-tmp` in the build profile directory holding the test binary.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn disk_tempdir() -> tempfile::TempDir {
    if let Some(dir) = std::env::var_os("MZ_POOL_TEST_DIR") {
        return tempfile::tempdir_in(dir).expect("tempdir in $MZ_POOL_TEST_DIR");
    }
    let tmp = std::env::temp_dir();
    if !sys::is_memory_backed(&tmp).unwrap_or(true) {
        return tempfile::tempdir_in(tmp).expect("tempdir");
    }
    // Cargo places test binaries in `<target>/<profile>/deps`, honoring
    // `CARGO_TARGET_DIR`.
    let exe = std::env::current_exe().expect("test binary path");
    let profile = exe
        .ancestors()
        .find(|dir| dir.file_name().is_some_and(|name| name == "deps"))
        .and_then(Path::parent)
        .expect("test binary runs from a cargo deps directory");
    let dir = profile.join("pool-test-tmp");
    std::fs::create_dir_all(&dir).expect("create pool-test-tmp");
    tempfile::tempdir_in(dir).expect("tempdir in the target directory")
}

/// Test seam over the store's syscalls. The queue is thread-local, so a
/// test injects faults only into the I/O it drives on its own thread.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod fault {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io;

    /// A syscall the seam can fail.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Op {
        /// `fallocate` growing a slot, not a hole punch.
        Fallocate,
        Write,
        Read,
        /// The `O_TMPFILE` open of a class file. Its fault needs a nonzero
        /// errno.
        OpenTmpfile,
    }

    thread_local! {
        static QUEUE: RefCell<VecDeque<(Op, i32)>> = const { RefCell::new(VecDeque::new()) };
    }

    /// Makes the next `op` on this thread fail with `errno`. Errno 0 makes
    /// it succeed with 0 bytes transferred.
    pub(crate) fn fail_next(op: Op, errno: i32) {
        QUEUE.with(|q| q.borrow_mut().push_back((op, errno)));
    }

    /// Drops every fault queued on this thread.
    pub(crate) fn clear() {
        QUEUE.with(|q| q.borrow_mut().clear());
    }

    /// Consumes the oldest fault queued for `op`, as the syscall's result.
    pub(super) fn inject(op: Op) -> Option<io::Result<usize>> {
        QUEUE.with(|q| {
            let mut q = q.borrow_mut();
            let pos = q.iter().position(|&(o, _)| o == op)?;
            let (_, errno) = q.remove(pos).expect("position is in range");
            Some(match errno {
                0 => Ok(0),
                errno => Err(io::Error::from_raw_os_error(errno)),
            })
        })
    }
}

/// The platform seam: every filesystem syscall the store makes.
#[cfg(target_os = "linux")]
mod sys {
    use std::ffi::CString;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileExt, OpenOptionsExt};
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::cast::CastFrom;

    fn c_path(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
    }

    fn off_t(offset: u64) -> io::Result<libc::off_t> {
        libc::off_t::try_from(offset).map_err(|_| io::Error::from_raw_os_error(libc::EFBIG))
    }

    /// Whether `path` is on tmpfs or ramfs.
    pub(super) fn is_memory_backed(path: &Path) -> io::Result<bool> {
        let path = c_path(path)?;
        let mut st = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: `path` is NUL-terminated and `st` is valid for writes of
        // one `statfs`.
        if unsafe { libc::statfs(path.as_ptr(), st.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `statfs` succeeded, so it initialized `st`.
        let f_type = unsafe { st.assume_init() }.f_type;
        // From `linux/magic.h`. `f_type`'s width and signedness differ
        // across targets, and `libc` lacks `RAMFS_MAGIC`.
        const TMPFS_MAGIC: u32 = 0x0102_1994;
        const RAMFS_MAGIC: u32 = 0x8584_58f6;
        let f_type = i128::from(f_type);
        Ok(f_type == i128::from(TMPFS_MAGIC) || f_type == i128::from(RAMFS_MAGIC))
    }

    /// Bytes available to unprivileged writers on the volume holding `path`.
    pub(super) fn available_bytes(path: &Path) -> io::Result<u64> {
        let path = c_path(path)?;
        let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        // SAFETY: `path` is NUL-terminated and `st` is valid for writes of
        // one `statvfs`.
        if unsafe { libc::statvfs(path.as_ptr(), st.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `statvfs` succeeded, so it initialized `st`.
        let st = unsafe { st.assume_init() };
        // The field widths differ across targets.
        #[allow(clippy::useless_conversion)]
        let (bavail, frsize) = (u64::from(st.f_bavail), u64::from(st.f_frsize));
        Ok(bavail.saturating_mul(frsize))
    }

    /// Opens a nameless read-write file in `dir`, close-on-exec, with
    /// `O_DIRECT` if `direct`.
    pub(super) fn open_anonymous(dir: &Path, class: usize, direct: bool) -> io::Result<File> {
        let direct = if direct { libc::O_DIRECT } else { 0 };
        // `std` opens every file `O_CLOEXEC`.
        let tmpfile = match tmpfile_fault() {
            Some(err) => Err(err),
            None => OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_TMPFILE | direct)
                .open(dir),
        };
        match tmpfile {
            Err(err) if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP | libc::EISDIR)) => {
                // The filesystem lacks `O_TMPFILE`: create a unique name and
                // unlink it at once, leaving the same lifetime as an
                // anonymous file.
                static NONCE: AtomicU64 = AtomicU64::new(0);
                let name = format!(
                    ".mz-pool-extents-{}-{class}-{}",
                    std::process::id(),
                    NONCE.fetch_add(1, Ordering::Relaxed),
                );
                let path = dir.join(name);
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(direct)
                    .open(&path)?;
                std::fs::remove_file(&path)?;
                Ok(file)
            }
            result => result,
        }
    }

    /// The injected failure of the next `O_TMPFILE` open, if any.
    fn tmpfile_fault() -> Option<io::Error> {
        #[cfg(test)]
        if let Some(result) = super::fault::inject(super::fault::Op::OpenTmpfile) {
            return Some(result.expect_err("an OpenTmpfile fault carries an errno"));
        }
        None
    }

    /// Allocates blocks for `[offset, offset + len)`.
    pub(super) fn fallocate(file: &File, offset: u64, len: usize) -> io::Result<()> {
        #[cfg(test)]
        if let Some(result) = super::fault::inject(super::fault::Op::Fallocate) {
            return result.map(|_| ());
        }
        fallocate_mode(file, 0, offset, len)
    }

    /// Deallocates the blocks of `[offset, offset + len)`, keeping the file
    /// size.
    pub(super) fn punch_hole(file: &File, offset: u64, len: usize) -> io::Result<()> {
        fallocate_mode(
            file,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset,
            len,
        )
    }

    fn fallocate_mode(file: &File, mode: libc::c_int, offset: u64, len: usize) -> io::Result<()> {
        let offset = off_t(offset)?;
        let len = off_t(u64::cast_from(len))?;
        loop {
            // SAFETY: plain syscall on an owned descriptor, no memory
            // arguments.
            if unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, len) } == 0 {
                return Ok(());
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }

    /// One `pwrite` of `buf` at `offset`.
    pub(super) fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(result) = super::fault::inject(super::fault::Op::Write) {
            return result;
        }
        file.write_at(buf, offset)
    }

    /// One `pread` into `buf` at `offset`.
    pub(super) fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(result) = super::fault::inject(super::fault::Op::Read) {
            return result;
        }
        file.read_at(buf, offset)
    }

    /// Writes back `[offset, offset + len)` synchronously and drops it from
    /// the page cache.
    pub(super) fn writeback_and_drop(file: &File, offset: u64, len: usize) -> io::Result<()> {
        let range_offset = off_t(offset)?;
        let range_len = off_t(u64::cast_from(len))?;
        // SAFETY: plain syscall on an owned descriptor, no memory arguments.
        let synced = unsafe {
            libc::sync_file_range(
                file.as_raw_fd(),
                range_offset,
                range_len,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
        if synced != 0 {
            return Err(io::Error::last_os_error());
        }
        drop_cache(file, offset, len)
    }

    /// Drops the clean pages of `[offset, offset + len)` from the page cache.
    pub(super) fn drop_cache(file: &File, offset: u64, len: usize) -> io::Result<()> {
        let offset = off_t(offset)?;
        let len = off_t(u64::cast_from(len))?;
        // SAFETY: plain syscall on an owned descriptor, no memory arguments.
        let errno = unsafe {
            libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED)
        };
        match errno {
            0 => Ok(()),
            errno => Err(io::Error::from_raw_os_error(errno)),
        }
    }
}

/// File mode is Linux-only: every operation fails as unsupported, so
/// [`FileStore::open`] does.
#[cfg(not(target_os = "linux"))]
mod sys {
    use std::fs::File;
    use std::io;
    use std::path::Path;

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "file-backed extents need Linux")
    }

    pub(super) fn is_memory_backed(_path: &Path) -> io::Result<bool> {
        Err(unsupported())
    }

    pub(super) fn available_bytes(_path: &Path) -> io::Result<u64> {
        Err(unsupported())
    }

    pub(super) fn open_anonymous(_dir: &Path, _class: usize, _direct: bool) -> io::Result<File> {
        Err(unsupported())
    }

    pub(super) fn fallocate(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn punch_hole(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn pwrite(_file: &File, _buf: &[u8], _offset: u64) -> io::Result<usize> {
        Err(unsupported())
    }

    pub(super) fn pread(_file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<usize> {
        Err(unsupported())
    }

    pub(super) fn writeback_and_drop(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn drop_cache(_file: &File, _offset: u64, _len: usize) -> io::Result<()> {
        Err(unsupported())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
