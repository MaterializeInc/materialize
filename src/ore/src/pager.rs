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
            HandleInner::File(_) => None,
        }
    }

    pub(crate) fn file_inner(&self) -> Option<&FileInner> {
        match &self.inner {
            HandleInner::File(f) => Some(f),
            HandleInner::Swap(_) => None,
        }
    }

    pub(crate) fn into_swap_inner(self) -> Option<SwapInner> {
        match self.inner {
            HandleInner::Swap(s) => Some(s),
            HandleInner::File(_) => None,
        }
    }

    pub(crate) fn into_file_inner(self) -> Option<FileInner> {
        match self.inner {
            HandleInner::File(f) => Some(f),
            HandleInner::Swap(_) => None,
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

/// Scatter pageout. Logical layout = chunks concatenated in order.
/// After return, each `Vec` in `chunks` is empty.
/// File backend preserves capacity; swap backend moves the alloc into the handle.
/// Empty input returns a `len == 0` handle and performs no I/O.
pub fn pageout(chunks: &mut [Vec<u64>]) -> Handle {
    if total_len(chunks) == 0 {
        return Handle::from_swap(SwapInner::new(Vec::new()));
    }
    match backend() {
        Backend::Swap => swap::pageout_swap(chunks),
        Backend::File => file::pageout_file(chunks),
    }
}

/// Reads multiple ranges. Output appended to `dst` in request order (concat).
/// Panics if any range is out of bounds.
pub fn read_at_many(handle: &Handle, ranges: &[(usize, usize)], dst: &mut Vec<u64>) {
    match &handle.inner {
        HandleInner::Swap(_) => swap::read_at_swap(handle, ranges, dst),
        HandleInner::File(_) => file::read_at_file(handle, ranges, dst),
    }
}

/// Reads a single range. Convenience wrapper around `read_at_many`.
pub fn read_at(handle: &Handle, offset: usize, len: usize, dst: &mut Vec<u64>) {
    read_at_many(handle, &[(offset, len)], dst);
}

/// Hints that the entire payload will be read soon. Best-effort.
/// Swap backend issues `madvise(MADV_WILLNEED)`; file backend issues
/// `posix_fadvise(POSIX_FADV_WILLNEED)`. Both are async — the call returns
/// promptly and the kernel populates pages or page cache in the background.
/// Useful for overlapping I/O with computation in pipelines that know which
/// handles they will read next.
pub fn prefetch(handle: &Handle) {
    prefetch_at(handle, 0, handle.len());
}

/// Hints that the range `[offset, offset+len)` of the handle will be read soon.
/// Panics if the range is out of bounds.
pub fn prefetch_at(handle: &Handle, offset: usize, len: usize) {
    match &handle.inner {
        HandleInner::Swap(_) => swap::prefetch_at_swap(handle, offset, len),
        HandleInner::File(_) => file::prefetch_at_file(handle, offset, len),
    }
}

/// Consumes handle, writing the entire payload into `dst` (cleared first), then reclaims storage.
/// Swap fast path: single-chunk handle into empty `dst` swaps in place, no copy.
pub fn take(handle: Handle, dst: &mut Vec<u64>) {
    match &handle.inner {
        HandleInner::Swap(_) => swap::take_swap(handle, dst),
        HandleInner::File(_) => file::take_file(handle, dst),
    }
}

fn total_len(chunks: &[Vec<u64>]) -> usize {
    chunks.iter().map(|c| c.len()).sum()
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

#[cfg(test)]
mod dispatch_tests {
    use super::*;

    #[mz_ore::test]
    fn end_to_end_swap() {
        set_backend(Backend::Swap);
        let mut chunks = [vec![1u64, 2, 3, 4]];
        let h = pageout(&mut chunks);
        assert_eq!(h.len(), 4);
        assert!(chunks[0].is_empty());

        let mut dst = Vec::new();
        read_at(&h, 1, 2, &mut dst);
        assert_eq!(dst, vec![2, 3]);

        let mut dst2 = Vec::new();
        take(h, &mut dst2);
        assert_eq!(dst2, vec![1, 2, 3, 4]);
    }
}
