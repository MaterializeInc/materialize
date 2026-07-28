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

//! File backend for the pager. See `mz_ore::pager` for the public API.
//!
//! Stale-scratch cleanup is intentionally out of scope: production runs on
//! Kubernetes with per-pod ephemeral volumes, so a crashed predecessor's
//! files are reclaimed with the volume. Local dev tooling is responsible
//! for sweeping leftovers from crashed processes.

use std::fs::File;
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::cast::CastFrom;
use crate::pager::Handle;

static SCRATCH_DIR: OnceLock<PathBuf> = OnceLock::new();
static SUBDIR: OnceLock<PathBuf> = OnceLock::new();
static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);
/// Serializes `set_scratch_dir` callers so init failures can be retried on
/// the next call. A plain `Once` would burn the only retry opportunity.
static INIT_LOCK: Mutex<()> = Mutex::new(());

/// Configures the scratch directory for the file backend.
///
/// Idempotent across multiple calls with the same path. A different path on a
/// subsequent call is logged and ignored. If the first call fails to initialize
/// the subdir, later calls retry — the scratch directory is committed only
/// after a successful init.
pub fn set_scratch_dir(root: PathBuf) {
    let _guard = INIT_LOCK.lock().expect("mz_ore::pager INIT_LOCK poisoned");
    if let Some(existing) = SCRATCH_DIR.get() {
        if *existing != root {
            tracing::warn!(
                ?root,
                ?existing,
                "mz_ore::pager scratch dir already set; ignoring",
            );
        }
        return;
    }
    match init_subdir(&root) {
        Ok(()) => {
            // We hold INIT_LOCK and just verified the OnceLock is empty above,
            // so this set always wins.
            let _ = SCRATCH_DIR.set(root);
        }
        Err(err) => {
            tracing::warn!(
                ?root,
                %err,
                "mz_ore::pager: failed to initialize scratch subdir; will retry on next call",
            );
        }
    }
}

fn init_subdir(root: &Path) -> std::io::Result<()> {
    let nonce: u64 = rand::random();
    let pid = std::process::id();
    let subdir = root.join(format!("mz-pager-{pid}-{nonce:016x}"));
    std::fs::create_dir_all(&subdir)?;
    let _ = SUBDIR.set(subdir);
    Ok(())
}

/// Identifies a scratch file within the backend's subdir. Wraps the monotonic
/// counter value so file ids don't blend into the many other `u64`s in this
/// module (offsets, lengths, byte counts); the backing file is
/// `scratch_path(id)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileId(u64);

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) fn scratch_path(id: FileId) -> PathBuf {
    SUBDIR
        .get()
        .expect("mz_ore::pager file backend used before set_scratch_dir")
        .join(format!("{id}.bin"))
}

pub(crate) fn alloc_scratch_id() -> FileId {
    FileId(SCRATCH_ID.fetch_add(1, Ordering::Relaxed))
}

/// Free-list of scratch slots: named files that still exist on disk but back no
/// live handle. `take`/drop returns a slot here instead of unlinking, so the
/// inode and its warm page cache survive for the next `pageout` to overwrite.
/// This avoids the synchronous inode eviction and page-cache truncate that
/// dominate unlink cost for the write-once / read-once / delete spill pattern.
static FREE_SLOTS: Mutex<Vec<FileId>> = Mutex::new(Vec::new());

/// Maximum slots retained on the free-list. A free slot pins up to
/// [`MAX_RECYCLE_BYTES`] of (writeback-reclaimable) page cache, so the cap
/// bounds retained memory at `FREE_LIST_CAP * MAX_RECYCLE_BYTES`. Reuse during
/// a merge saturates at a handful of slots because `pageout` and `take`
/// interleave (a `take` frees a slot the next `pageout` immediately reuses);
/// the cap mostly absorbs burst phase transitions. Tuning candidate.
const FREE_LIST_CAP: usize = 16;

/// Don't recycle a slot whose payload exceeds this. A slot's on-disk file is
/// the high-water mark of payloads written to it (reuse overwrites from offset
/// 0 without truncating); refusing to recycle past this bound keeps every
/// free-list file — and thus retained page cache — under the threshold.
///
/// Enforced at drop, not at pageout: an oversize payload can still pop and
/// overwrite a free slot, growing that file past the bound, but the resulting
/// handle's `len_u64s` then exceeds the threshold, so its drop unlinks rather
/// than recycling. The grown file never re-enters the free-list, so free-listed
/// files stay within the bound regardless of the unchecked pop.
///
/// Other recycling sites (the column pager's warm read-buffer pool, the merge
/// batcher's column recycler) hold their own size caps; they share this value
/// today but are tuned independently.
const MAX_RECYCLE_BYTES: usize = 1 << 22;

/// Locks the free-list, recovering from poisoning instead of propagating it.
/// [`push_free_slot`] runs in [`FileInner::drop`], where an unwrap-on-poison
/// would panic-in-drop and abort the process. A panic mid-operation can't
/// corrupt the list beyond a lost or duplicated slot id, both harmless: a lost
/// id just unlinks later, a duplicate is overwritten on reuse.
fn lock_free_slots() -> std::sync::MutexGuard<'static, Vec<FileId>> {
    FREE_SLOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Pops a recycled slot id, or `None` if the free-list is empty. The returned
/// id usually names an existing file at `scratch_path(id)` holding stale bytes
/// the caller overwrites — but not always: a slot recycled after its file was
/// unlinked out from under us (a `take` that unlinked, then failed) names a
/// missing file. The reused-slot write path opens with `create(true)` for
/// exactly this case, recreating the file rather than erroring.
fn pop_free_slot() -> Option<FileId> {
    lock_free_slots().pop()
}

/// Returns slot `id` to the free-list, retaining its file for reuse. Returns
/// `false` (and keeps nothing) if the list is already at [`FREE_LIST_CAP`], so
/// the caller unlinks instead.
fn push_free_slot(id: FileId) -> bool {
    let mut slots = lock_free_slots();
    if slots.len() >= FREE_LIST_CAP {
        return false;
    }
    slots.push(id);
    true
}

#[cfg(test)]
fn free_slots_snapshot() -> Vec<FileId> {
    lock_free_slots().clone()
}

#[cfg(test)]
fn clear_free_slots() {
    lock_free_slots().clear();
}

/// Storage for a file-backed handle. The file at `scratch_path(id)` holds the
/// bytes for non-empty handles. For `len_u64s == 0`, no file is created; drop
/// is a no-op.
/// No file descriptor is retained.
#[derive(Debug)]
pub(crate) struct FileInner {
    pub(crate) id: FileId,
    pub(crate) len_u64s: usize,
}

impl FileInner {
    pub(crate) fn new(id: FileId, len_u64s: usize) -> Self {
        Self { id, len_u64s }
    }
}

impl Drop for FileInner {
    fn drop(&mut self) {
        // Empty handles never created a file.
        if self.len_u64s == 0 {
            return;
        }
        // If the scratch dir was never set up (e.g. construction failed before
        // any I/O was attempted), there is nothing on disk to clean up.
        let Some(subdir) = SUBDIR.get() else {
            return;
        };
        // Recycle the slot instead of unlinking: keep the file (and its warm
        // page cache) alive for the next `pageout` to overwrite, sidestepping
        // the inode eviction + page-cache truncate that unlink forces. Slots
        // over the size bound, or past the free-list cap, fall through to
        // unlink so retained page cache stays bounded.
        if self.len_u64s.saturating_mul(8) <= MAX_RECYCLE_BYTES && push_free_slot(self.id) {
            return;
        }
        let path = subdir.join(format!("{}.bin", self.id));
        if let Err(err) = std::fs::remove_file(&path) {
            // ENOENT is fine: a successful `take` already unlinked.
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(?path, %err, "mz_ore::pager: failed to unlink scratch file");
            }
        }
    }
}

/// Fallible counterpart to [`pageout_file`]. Returns the underlying I/O error
/// instead of falling back to a different backend, so callers can distinguish
/// "scratch volume is broken" from "everything is fine."
pub(crate) fn try_pageout_file(chunks: &mut [Vec<u64>]) -> std::io::Result<Handle> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    if total == 0 {
        // Honor the backend invariant: an empty handle from the file backend
        // is still a file-variant handle. No file is created on disk; drop
        // short-circuits when `len_u64s == 0`.
        return Ok(Handle::from_file(FileInner::new(alloc_scratch_id(), 0)));
    }
    // Reuse a recycled slot if one is free; otherwise mint a fresh id. A reused
    // slot's file already exists and is overwritten from offset 0; a fresh slot
    // must not collide with an existing file.
    let (id, reused) = match pop_free_slot() {
        Some(id) => (id, true),
        None => (alloc_scratch_id(), false),
    };
    let path = scratch_path(id);
    match write_chunks(&path, chunks, reused) {
        Ok(()) => {
            for c in chunks.iter_mut() {
                c.clear();
            }
            Ok(Handle::from_file(FileInner::new(id, total)))
        }
        Err(err) => {
            // Best-effort cleanup; ignore secondary errors here so we surface
            // the primary write error to the caller. A reused slot is unlinked
            // rather than returned to the free-list: a failed write may have
            // left it in an unknown state on a possibly-broken volume.
            let _ = std::fs::remove_file(&path);
            Err(err)
        }
    }
}

pub(crate) fn pageout_file(chunks: &mut [Vec<u64>]) -> Handle {
    try_pageout_file(chunks)
        .unwrap_or_else(|err| panic!("mz_ore::pager: file pageout failed: {err}"))
}

fn write_chunks(path: &Path, chunks: &[Vec<u64>], reused: bool) -> std::io::Result<()> {
    // Reused slots already have a file; open it and overwrite from offset 0.
    // Deliberately no `truncate`: shrinking would re-evict the tail pages — the
    // cost recycling exists to avoid — and reads are length-bounded, so a stale
    // tail is never observed. Fresh slots use `create_new` to catch id-reuse
    // bugs: a fresh id must never name an existing file.
    let mut opts = File::options();
    opts.write(true);
    if reused {
        opts.create(true);
    } else {
        opts.create_new(true);
    }
    let file = opts.open(path)?;
    let mut slices: Vec<IoSlice<'_>> = chunks
        .iter()
        .filter(|c| !c.is_empty())
        .map(|c| IoSlice::new(bytemuck::cast_slice(c.as_slice())))
        .collect();
    write_all_vectored(&file, slices.as_mut_slice())?;
    Ok(())
}

fn write_all_vectored(mut file: &File, mut slices: &mut [IoSlice<'_>]) -> std::io::Result<()> {
    use std::io::Write;
    while !slices.is_empty() {
        let written = file.write_vectored(slices)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}

/// Fallible counterpart to [`read_at_file`]. Returns `Err` instead of
/// panicking on I/O failure (open/pread errors). Bounds violations and
/// non-file handles still panic — those are caller bugs, not I/O failures.
pub(crate) fn try_read_at_file(
    handle: &Handle,
    ranges: &[(usize, usize)],
    dst: &mut Vec<u64>,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    let inner = handle
        .file_inner()
        .expect("try_read_at_file called on non-file handle");
    let total = inner.len_u64s;
    for &(off, len) in ranges {
        let end = off.checked_add(len).expect("range offset+len overflow");
        assert!(
            end <= total,
            "read range out of bounds: {off}+{len} > {total}"
        );
    }
    // Empty handle: all ranges must be `(_, 0)` after the bounds check above,
    // so there is no I/O to perform. Skip opening the (nonexistent) file.
    if total == 0 {
        return Ok(());
    }
    let path = scratch_path(inner.id);
    let file = File::open(&path).map_err(|err| {
        std::io::Error::new(err.kind(), format!("mz_ore::pager: open {path:?}: {err}"))
    })?;

    let coalesced = coalesce(ranges);
    for (range_idx, (off, len)) in coalesced.iter().copied().enumerate() {
        // Multiply in `u64` space: `off * 8` would overflow on 32-bit targets
        // for handles holding more than 512Mi `u64`s.
        let byte_off = u64::cast_from(off)
            .checked_mul(8)
            .expect("byte offset overflow");
        let byte_len = len.checked_mul(8).expect("byte length overflow");
        let buf_start = dst.len();
        dst.resize(buf_start + len, 0);
        let buf: &mut [u8] = bytemuck::cast_slice_mut(&mut dst[buf_start..buf_start + len]);
        let mut filled = 0;
        while filled < byte_len {
            let pos = byte_off + u64::cast_from(filled);
            let n = file
                .read_at(&mut buf[filled..byte_len], pos)
                .map_err(|err| {
                    std::io::Error::new(
                        err.kind(),
                        format!(
                            "mz_ore::pager: pread {path:?} pos={pos} \
                         (range #{range_idx}, off={off} len={len}): {err}",
                        ),
                    )
                })?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "mz_ore::pager: pread short read at {path:?} pos={pos} \
                         (range #{range_idx}, off={off} len={len}): \
                         filled {filled} of {byte_len} bytes for this range",
                    ),
                ));
            }
            filled += n;
        }
    }
    Ok(())
}

pub(crate) fn read_at_file(handle: &Handle, ranges: &[(usize, usize)], dst: &mut Vec<u64>) {
    try_read_at_file(handle, ranges, dst)
        .unwrap_or_else(|err| panic!("mz_ore::pager: read_at_file failed: {err}"));
}

fn coalesce(ranges: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for &(off, len) in ranges {
        if let Some(last) = out.last_mut() {
            if last.0 + last.1 == off {
                last.1 += len;
                continue;
            }
        }
        out.push((off, len));
    }
    out
}

/// Fallible counterpart to [`take_file`]. Returns `Err` instead of panicking
/// on I/O failure. The handle is consumed in either case; on `Err`, `dst` may
/// hold partial data and the scratch file is unlinked when the inner is
/// dropped.
pub(crate) fn try_take_file(handle: Handle, dst: &mut Vec<u64>) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    let inner = handle
        .into_file_inner()
        .expect("try_take_file called on non-file handle");
    dst.clear();
    if inner.len_u64s == 0 {
        // Empty handle: no file exists; nothing to read or unlink.
        drop(inner);
        return Ok(());
    }
    let path = scratch_path(inner.id);
    let file = File::open(&path).map_err(|err| {
        std::io::Error::new(
            err.kind(),
            format!("mz_ore::pager: take open {path:?}: {err}"),
        )
    })?;
    dst.resize(inner.len_u64s, 0);
    let buf: &mut [u8] = bytemuck::cast_slice_mut(dst.as_mut_slice());
    let buf_len = buf.len();
    let mut filled = 0;
    while filled < buf_len {
        let pos = u64::cast_from(filled);
        let n = file.read_at(&mut buf[filled..], pos).map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("mz_ore::pager: take pread {path:?} pos={pos}: {err}"),
            )
        })?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "mz_ore::pager: take short read at {path:?}: filled {filled} of {buf_len} bytes",
                ),
            ));
        }
        filled += n;
    }
    drop(file);
    // FileInner::drop reclaims the scratch file: recycled to the free-list for
    // reuse, or unlinked if the list is full or the payload is oversize.
    drop(inner);
    Ok(())
}

pub(crate) fn take_file(handle: Handle, dst: &mut Vec<u64>) {
    try_take_file(handle, dst)
        .unwrap_or_else(|err| panic!("mz_ore::pager: take_file failed: {err}"));
}

#[cfg(test)]
mod backend_tests {
    use super::*;

    /// Serializes backend tests so the process-global free-list is exclusive to
    /// one test at a time. `cargo test` runs a binary's tests on shared threads,
    /// so without this a peer's `pageout`/`take` would pop or push slots
    /// mid-assertion. (Under nextest each test is its own process and the lock
    /// is uncontended.)
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Acquires the serialization lock, sets up the shared scratch dir, and
    /// hands back a clean free-list. The returned guard (already `#[must_use]`
    /// as a `MutexGuard`) must outlive the test body. Recovers from a poisoned
    /// lock so a `should_panic` test doesn't wedge its peers.
    fn setup_dir() -> std::sync::MutexGuard<'static, ()> {
        let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let _ = super::tests::shared_scratch();
        clear_free_slots();
        guard
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn pageout_writes_file_and_clears_capacity() {
        let _guard = setup_dir();
        let mut chunks = [vec![10u64, 20, 30], vec![40, 50]];
        let cap_before_0 = chunks[0].capacity();
        let cap_before_1 = chunks[1].capacity();
        let h = pageout_file(&mut chunks);
        assert_eq!(h.len(), 5);
        assert!(chunks[0].is_empty());
        assert!(chunks[1].is_empty());
        // File backend preserves capacity:
        assert_eq!(chunks[0].capacity(), cap_before_0);
        assert_eq!(chunks[1].capacity(), cap_before_1);

        let inner = h.file_inner().expect("file inner");
        let path = scratch_path(inner.id);
        assert!(path.exists());
        let bytes = std::fs::read(&path).expect("read scratch");
        assert_eq!(bytes.len(), 5 * 8);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_read_at_basic() {
        let _guard = setup_dir();
        let mut chunks = [vec![1u64, 2, 3, 4, 5]];
        let h = pageout_file(&mut chunks);
        let mut dst = Vec::new();
        read_at_file(&h, &[(1, 3)], &mut dst);
        assert_eq!(dst, vec![2, 3, 4]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_read_at_many_concats_and_coalesces() {
        let _guard = setup_dir();
        let mut chunks = [vec![10u64, 20, 30, 40, 50, 60]];
        let h = pageout_file(&mut chunks);
        let mut dst = Vec::new();
        // (0,2) and (2,2) are adjacent => single pread internally.
        read_at_file(&h, &[(0, 2), (2, 2), (5, 1)], &mut dst);
        assert_eq!(dst, vec![10, 20, 30, 40, 60]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    #[should_panic(expected = "out of bounds")]
    fn file_read_at_panics_on_oob() {
        let _guard = setup_dir();
        let mut chunks = [vec![1u64, 2]];
        let h = pageout_file(&mut chunks);
        let mut dst = Vec::new();
        read_at_file(&h, &[(0, 99)], &mut dst);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_take_returns_data_and_recycles() {
        let _guard = setup_dir();
        let mut chunks = [vec![7u64; 100]];
        let h = pageout_file(&mut chunks);
        let inner_id = h.file_inner().unwrap().id;
        let path = scratch_path(inner_id);
        assert!(path.exists());
        let mut dst = Vec::new();
        take_file(h, &mut dst);
        assert_eq!(dst, vec![7u64; 100]);
        // Recycled, not unlinked: the file survives for reuse and the slot is
        // parked on the free-list.
        assert!(path.exists(), "scratch file should survive take for reuse");
        assert!(
            free_slots_snapshot().contains(&inner_id),
            "taken slot should be recycled onto the free-list"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_drop_recycles_when_not_taken() {
        let _guard = setup_dir();
        let mut chunks = [vec![1u64, 2, 3]];
        let h = pageout_file(&mut chunks);
        let id = h.file_inner().unwrap().id;
        let path = scratch_path(id);
        assert!(path.exists());
        drop(h);
        assert!(path.exists(), "scratch file should survive drop for reuse");
        assert!(
            free_slots_snapshot().contains(&id),
            "dropped slot should be recycled onto the free-list"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_pageout_reuses_recycled_slot() {
        let _guard = setup_dir();
        // First handle: page out, then take to recycle the slot.
        let mut a = [vec![1u64, 2, 3, 4, 5]];
        let h1 = pageout_file(&mut a);
        let id1 = h1.file_inner().unwrap().id;
        let mut dst = Vec::new();
        take_file(h1, &mut dst);
        assert_eq!(dst, vec![1, 2, 3, 4, 5]);

        // Second, shorter handle should reuse the freed slot and read back
        // cleanly — the stale tail from the longer first payload is past the
        // logical length and never observed.
        let mut b = [vec![9u64; 3]];
        let h2 = pageout_file(&mut b);
        assert_eq!(
            h2.file_inner().unwrap().id,
            id1,
            "pageout should reuse the recycled slot"
        );
        let mut dst2 = Vec::new();
        take_file(h2, &mut dst2);
        assert_eq!(dst2, vec![9u64; 3], "reused slot must not leak stale tail");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_oversize_payload_is_unlinked_not_recycled() {
        let _guard = setup_dir();
        // One u64 past the recycle threshold.
        let len = (MAX_RECYCLE_BYTES / 8) + 1;
        let mut chunks = [vec![0u64; len]];
        let h = pageout_file(&mut chunks);
        let id = h.file_inner().unwrap().id;
        let path = scratch_path(id);
        drop(h);
        assert!(!path.exists(), "oversize scratch file should be unlinked");
        assert!(
            !free_slots_snapshot().contains(&id),
            "oversize slot must not be recycled"
        );
    }

    #[mz_ore::test]
    fn free_list_respects_cap() {
        let _guard = setup_dir();
        for i in 0..u64::cast_from(FREE_LIST_CAP) {
            assert!(
                push_free_slot(FileId(i)),
                "slots under the cap are accepted"
            );
        }
        assert!(
            !push_free_slot(FileId(u64::MAX)),
            "a slot past the cap is rejected so the caller unlinks"
        );
        assert_eq!(free_slots_snapshot().len(), FREE_LIST_CAP);
    }

    #[mz_ore::test]
    fn file_empty_handle_round_trips() {
        let _guard = setup_dir();
        let mut chunks: [Vec<u64>; 0] = [];
        let h = pageout_file(&mut chunks);
        assert_eq!(h.len(), 0);
        // Variant must be `File`, honoring the documented backend invariant.
        assert!(
            h.file_inner().is_some(),
            "empty handle should be File-variant"
        );

        let mut dst = vec![0xdeadu64];
        read_at_file(&h, &[], &mut dst);
        assert_eq!(dst, vec![0xdeadu64], "empty read leaves dst untouched");

        let mut dst = Vec::new();
        take_file(h, &mut dst);
        assert!(dst.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn try_read_at_file_surfaces_missing_file() {
        let _guard = setup_dir();
        let mut chunks = [vec![1u64, 2, 3]];
        let h = pageout_file(&mut chunks);
        // Concurrently unlink the scratch file out from under us.
        let path = scratch_path(h.file_inner().unwrap().id);
        std::fs::remove_file(&path).expect("unlink scratch");
        let mut dst = Vec::new();
        let err = try_read_at_file(&h, &[(0, 3)], &mut dst).expect_err("should surface ENOENT");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    static TEST_DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();

    pub(super) fn shared_scratch() -> &'static std::path::Path {
        let dir = TEST_DIR.get_or_init(|| tempdir().expect("tempdir"));
        set_scratch_dir(dir.path().to_owned());
        dir.path()
    }

    #[mz_ore::test]
    fn set_scratch_dir_creates_subdir() {
        let root = shared_scratch();
        let subdir = SUBDIR.get().expect("subdir was initialized");
        assert!(subdir.exists());
        assert!(subdir.starts_with(root));
        assert!(
            subdir
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("mz-pager-")
        );
    }
}
