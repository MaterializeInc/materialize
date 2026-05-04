//! File backend for the pager. See `mz_ore::pager` for the public API.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

static SCRATCH_DIR: OnceLock<PathBuf> = OnceLock::new();
static SUBDIR: OnceLock<PathBuf> = OnceLock::new();
static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);

/// Configures the scratch directory for the file backend. Idempotent across multiple
/// calls with the same path; logs and ignores subsequent calls with a different path.
pub fn set_scratch_dir(root: PathBuf) {
    if let Err(existing) = SCRATCH_DIR.set(root.clone()) {
        if existing != root {
            tracing::warn!(
                ?root,
                ?existing,
                "mz_ore::pager scratch dir already set; ignoring",
            );
        }
        return;
    }
    if let Err(err) = init_subdir(&root) {
        tracing::warn!(?root, %err, "mz_ore::pager: failed to initialize scratch subdir");
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

use crate::pager::Handle;

pub(crate) fn pageout_file(_chunks: &mut [Vec<u64>]) -> Handle {
    unimplemented!("file backend pageout: see Task 9")
}

pub(crate) fn read_at_file(_h: &Handle, _ranges: &[(usize, usize)], _dst: &mut Vec<u64>) {
    unimplemented!("file backend read_at: see Task 10")
}

pub(crate) fn take_file(_h: Handle, _dst: &mut Vec<u64>) {
    unimplemented!("file backend take: see Task 11")
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
