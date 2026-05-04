//! File backend for the pager. See `mz_ore::pager` for the public API.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Once, OnceLock};

static SCRATCH_DIR: OnceLock<PathBuf> = OnceLock::new();
static SUBDIR: OnceLock<PathBuf> = OnceLock::new();
static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);
static SCRATCH_INIT: Once = Once::new();

/// Configures the scratch directory for the file backend. Idempotent across multiple
/// calls with the same path; logs and ignores subsequent calls with a different path.
pub fn set_scratch_dir(root: PathBuf) {
    SCRATCH_INIT.call_once(|| {
        if let Err(err) = init_subdir(&root) {
            tracing::warn!(?root, %err, "mz_ore::pager: failed to initialize scratch subdir");
        }
        let _ = SCRATCH_DIR.set(root.clone());
    });
    if let Some(existing) = SCRATCH_DIR.get() {
        if *existing != root {
            tracing::warn!(
                ?root,
                ?existing,
                "mz_ore::pager scratch dir already set; ignoring",
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
    reap_stale(root);
    Ok(())
}

fn reap_stale(root: &Path) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(?root, %err, "mz_ore::pager: scratch dir scan failed");
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        let Some(rest) = name.strip_prefix("mz-pager-") else {
            continue;
        };
        let pid: u32 = match rest.split_once('-').and_then(|(p, _)| p.parse().ok()) {
            Some(p) => p,
            None => continue,
        };
        if pid == std::process::id() {
            continue;
        }
        if std::path::Path::new(&format!("/proc/{pid}")).exists() {
            continue;
        }
        if let Err(err) = std::fs::remove_dir_all(entry.path()) {
            tracing::warn!(path = ?entry.path(), %err, "mz_ore::pager: reap failed");
        }
    }
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

/// Storage for a file-backed handle. The file at `scratch_path(id)` holds the bytes.
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
        let path = scratch_path(self.id);
        if let Err(err) = std::fs::remove_file(&path) {
            // ENOENT is fine: a successful `take` already unlinked.
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(?path, %err, "mz_ore::pager: failed to unlink scratch file");
            }
        }
    }
}

use std::fs::File;
use std::io::IoSlice;

use crate::pager::Handle;
use crate::pager::swap::{SwapInner, pageout_swap};

pub(crate) fn pageout_file(chunks: &mut [Vec<u64>]) -> Handle {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    if total == 0 {
        return Handle::from_swap(SwapInner::new(Vec::new()));
    }
    let id = alloc_scratch_id();
    let path = scratch_path(id);
    match write_chunks(&path, chunks) {
        Ok(()) => {
            for c in chunks.iter_mut() {
                c.clear();
            }
            Handle::from_file(FileInner::new(id, total))
        }
        Err(err) => {
            tracing::warn!(?path, %err, "mz_ore::pager: file pageout failed; falling back to swap");
            let _ = std::fs::remove_file(&path);
            pageout_swap(chunks)
        }
    }
}

fn write_chunks(path: &Path, chunks: &[Vec<u64>]) -> std::io::Result<()> {
    let file = File::options().write(true).create_new(true).open(path)?;
    let slices: Vec<IoSlice<'_>> = chunks
        .iter()
        .filter(|c| !c.is_empty())
        .map(|c| IoSlice::new(bytemuck::cast_slice(c.as_slice())))
        .collect();
    write_all_vectored(&file, &slices)?;
    Ok(())
}

#[cfg(unix)]
fn write_all_vectored(file: &File, slices: &[IoSlice<'_>]) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    let mut offset: i64 = 0;
    let mut idx = 0;
    let mut consumed_in_idx: usize = 0;
    while idx < slices.len() {
        let remaining = &slices[idx..];
        let iovs: Vec<libc::iovec> = remaining
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let base_off = if i == 0 { consumed_in_idx } else { 0 };
                // SAFETY: building an iovec from a live `IoSlice` is safe;
                // the pointer/length describe the caller's buffer.
                libc::iovec {
                    iov_base: unsafe { s.as_ptr().add(base_off) } as *mut libc::c_void,
                    iov_len: s.len() - base_off,
                }
            })
            .collect();
        // SAFETY: fd is valid and open for writing; iovs point into the live `slices`
        // owned by the caller; pwritev does not retain pointers past the syscall.
        let written =
            unsafe { libc::pwritev(fd, iovs.as_ptr(), iovs.len() as libc::c_int, offset) };
        if written < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut left = written as usize;
        offset += written as i64;
        while left > 0 && idx < slices.len() {
            let avail = slices[idx].len() - consumed_in_idx;
            if left >= avail {
                left -= avail;
                idx += 1;
                consumed_in_idx = 0;
            } else {
                consumed_in_idx += left;
                left = 0;
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_all_vectored(file: &File, slices: &[IoSlice<'_>]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = file;
    for s in slices {
        file.write_all(s)?;
    }
    Ok(())
}

pub(crate) fn read_at_file(_h: &Handle, _ranges: &[(usize, usize)], _dst: &mut Vec<u64>) {
    unimplemented!("file backend read_at: see Task 10")
}

pub(crate) fn take_file(_h: Handle, _dst: &mut Vec<u64>) {
    unimplemented!("file backend take: see Task 11")
}

#[cfg(test)]
mod backend_tests {
    use super::*;

    fn setup_dir() {
        let _ = super::tests::shared_scratch();
    }

    #[mz_ore::test]
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
