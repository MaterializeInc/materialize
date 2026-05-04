//! Explicit pager for cold data. See `doc/developer/design/20260504_pager.md`.

use std::sync::atomic::{AtomicU8, Ordering};

mod file;
mod swap;

pub use file::set_scratch_dir;

use crate::pager::file::FileInner;
use crate::pager::swap::SwapInner;

/// An opaque handle to data paged out via [`pageout`]. The handle's backend variant
/// is fixed at `pageout` time and is independent of any later `set_backend` call.
#[derive(Debug)]
pub struct Handle {
    inner: HandleInner,
}

#[derive(Debug)]
enum HandleInner {
    Swap(SwapInner),
    File(FileInner),
}

impl Handle {
    /// Returns the logical length of the handle's payload in `u64`s.
    pub fn len(&self) -> usize {
        match &self.inner {
            HandleInner::Swap(s) => *s.prefix.last().unwrap_or(&0),
            HandleInner::File(f) => f.len_u64s,
        }
    }

    /// Returns the logical length of the handle's payload in bytes (`len() * 8`).
    pub fn len_bytes(&self) -> usize {
        self.len() * 8
    }

    /// Returns `true` if the handle holds no data.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn from_swap(inner: SwapInner) -> Self {
        Self {
            inner: HandleInner::Swap(inner),
        }
    }

    pub(crate) fn from_file(inner: FileInner) -> Self {
        Self {
            inner: HandleInner::File(inner),
        }
    }

    pub(crate) fn swap_inner(&self) -> Option<&SwapInner> {
        match &self.inner {
            HandleInner::Swap(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn file_inner(&self) -> Option<&FileInner> {
        match &self.inner {
            HandleInner::File(f) => Some(f),
            _ => None,
        }
    }
}

/// Selects which backend stores paged-out data.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Backend {
    /// Hold allocations resident; hint the kernel via `MADV_COLD`.
    Swap,
    /// Write to a named scratch file; no file descriptor retained.
    File,
}

const BACKEND_SWAP: u8 = 0;
const BACKEND_FILE: u8 = 1;

static BACKEND: AtomicU8 = AtomicU8::new(BACKEND_SWAP);

/// Returns the currently active backend.
pub fn backend() -> Backend {
    match BACKEND.load(Ordering::Relaxed) {
        BACKEND_SWAP => Backend::Swap,
        BACKEND_FILE => Backend::File,
        _ => unreachable!("BACKEND atomic holds invalid discriminant"),
    }
}

/// Sets the active backend for future `pageout` calls. Existing handles are unaffected.
pub fn set_backend(b: Backend) {
    let raw = match b {
        Backend::Swap => BACKEND_SWAP,
        Backend::File => BACKEND_FILE,
    };
    BACKEND.store(raw, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn backend_round_trip() {
        set_backend(Backend::File);
        assert_eq!(backend(), Backend::File);
        set_backend(Backend::Swap);
        assert_eq!(backend(), Backend::Swap);
    }
}
