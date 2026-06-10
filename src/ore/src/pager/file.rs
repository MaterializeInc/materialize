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

pub(crate) fn scratch_path(id: u64) -> PathBuf {
    SUBDIR
        .get()
        .expect("mz_ore::pager file backend used before set_scratch_dir")
        .join(format!("{id}.bin"))
}

pub(crate) fn alloc_scratch_id() -> u64 {
    SCRATCH_ID.fetch_add(1, Ordering::Relaxed)
}

/// Storage for a file-backed handle. The file at `scratch_path(id)` holds the
/// bytes for non-empty handles. For `len_u64s == 0`, no file is created; drop
/// is a no-op.
/// No file descriptor is retained.
#[derive(Debug)]
pub(crate) struct FileInner {
    pub(crate) id: u64,
    pub(crate) len_u64s: usize,
}

impl FileInner {
    pub(crate) fn new(id: u64, len_u64s: usize) -> Self {
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
    let id = alloc_scratch_id();
    let path = scratch_path(id);
    match write_chunks(&path, chunks) {
        Ok(()) => {
            for c in chunks.iter_mut() {
                c.clear();
            }
            Ok(Handle::from_file(FileInner::new(id, total)))
        }
        Err(err) => {
            // Best-effort cleanup; ignore secondary errors here so we surface
            // the primary write error to the caller.
            let _ = std::fs::remove_file(&path);
            Err(err)
        }
    }
}

pub(crate) fn pageout_file(chunks: &mut [Vec<u64>]) -> Handle {
    try_pageout_file(chunks)
        .unwrap_or_else(|err| panic!("mz_ore::pager: file pageout failed: {err}"))
}

fn write_chunks(path: &Path, chunks: &[Vec<u64>]) -> std::io::Result<()> {
    let file = File::options().write(true).create_new(true).open(path)?;
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
    // FileInner::drop will unlink the scratch file.
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

    fn setup_dir() {
        let _ = super::tests::shared_scratch();
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn pageout_writes_file_and_clears_capacity() {
        setup_dir();
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
        setup_dir();
        let mut chunks = [vec![1u64, 2, 3, 4, 5]];
        let h = pageout_file(&mut chunks);
        let mut dst = Vec::new();
        read_at_file(&h, &[(1, 3)], &mut dst);
        assert_eq!(dst, vec![2, 3, 4]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_read_at_many_concats_and_coalesces() {
        setup_dir();
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
        setup_dir();
        let mut chunks = [vec![1u64, 2]];
        let h = pageout_file(&mut chunks);
        let mut dst = Vec::new();
        read_at_file(&h, &[(0, 99)], &mut dst);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_take_returns_data_and_unlinks() {
        setup_dir();
        let mut chunks = [vec![7u64; 100]];
        let h = pageout_file(&mut chunks);
        let inner_id = h.file_inner().unwrap().id;
        let path = scratch_path(inner_id);
        assert!(path.exists());
        let mut dst = Vec::new();
        take_file(h, &mut dst);
        assert_eq!(dst, vec![7u64; 100]);
        assert!(!path.exists(), "scratch file should be unlinked after take");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn file_drop_unlinks_when_not_taken() {
        setup_dir();
        let mut chunks = [vec![1u64, 2, 3]];
        let h = pageout_file(&mut chunks);
        let id = h.file_inner().unwrap().id;
        let path = scratch_path(id);
        assert!(path.exists());
        drop(h);
        assert!(!path.exists(), "scratch file should be unlinked on drop");
    }

    #[mz_ore::test]
    fn file_empty_handle_round_trips() {
        setup_dir();
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
        setup_dir();
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
