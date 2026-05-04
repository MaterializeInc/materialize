//! Explicit pager for cold data. See `doc/developer/design/20260504_pager.md`.

use std::sync::atomic::{AtomicU8, Ordering};

mod file;
mod swap;

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
