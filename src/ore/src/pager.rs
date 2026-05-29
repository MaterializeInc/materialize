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
            HandleInner::Swap(s) => s.total_len(),
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
/// Empty input returns a `len == 0` handle of the active backend's variant
/// (no I/O is performed in either backend).
///
/// Panics on I/O failure. Use [`try_pageout`] to surface the error.
pub fn pageout(chunks: &mut [Vec<u64>]) -> Handle {
    pageout_with(backend(), chunks)
}

/// Same as [`pageout`], but selects the backend explicitly. Bypasses the global
/// atomic so callers (such as the column-pager layer) can dispatch per call
/// without racing other writers.
///
/// Panics on I/O failure. Use [`try_pageout_with`] to surface the error.
pub fn pageout_with(b: Backend, chunks: &mut [Vec<u64>]) -> Handle {
    match b {
        Backend::Swap => swap::pageout_swap(chunks),
        Backend::File => file::pageout_file(chunks),
    }
}

/// Fallible counterpart to [`pageout`]. Returns the underlying I/O error
/// instead of panicking. The swap backend cannot fail at I/O, so this is
/// equivalent to `Ok(pageout(chunks))` when [`backend`] is [`Backend::Swap`].
pub fn try_pageout(chunks: &mut [Vec<u64>]) -> std::io::Result<Handle> {
    try_pageout_with(backend(), chunks)
}

/// Fallible counterpart to [`pageout_with`].
pub fn try_pageout_with(b: Backend, chunks: &mut [Vec<u64>]) -> std::io::Result<Handle> {
    match b {
        Backend::Swap => Ok(swap::pageout_swap(chunks)),
        Backend::File => file::try_pageout_file(chunks),
    }
}

/// Reads multiple ranges. Output appended to `dst` in request order (concat).
///
/// Ranges must be pairwise non-overlapping; ordering is otherwise unconstrained.
/// Panics if any range is out of bounds, if two ranges overlap, or on I/O
/// failure. Use [`try_read_at_many`] for fallible reads.
pub fn read_at_many(handle: &Handle, ranges: &[(usize, usize)], dst: &mut Vec<u64>) {
    assert_ranges_disjoint(ranges);
    match &handle.inner {
        HandleInner::Swap(_) => swap::read_at_swap(handle, ranges, dst),
        HandleInner::File(_) => file::read_at_file(handle, ranges, dst),
    }
}

/// Fallible counterpart to [`read_at_many`]. Caller-side preconditions
/// (out-of-bounds, overlapping ranges) still panic; I/O failures return `Err`.
pub fn try_read_at_many(
    handle: &Handle,
    ranges: &[(usize, usize)],
    dst: &mut Vec<u64>,
) -> std::io::Result<()> {
    assert_ranges_disjoint(ranges);
    match &handle.inner {
        HandleInner::Swap(_) => {
            swap::read_at_swap(handle, ranges, dst);
            Ok(())
        }
        HandleInner::File(_) => file::try_read_at_file(handle, ranges, dst),
    }
}

/// Asserts that no two ranges share any byte position. Both backends assume
/// disjoint ranges: the file backend coalesces adjacent ranges into a single
/// pread and writes into a contiguous slice of `dst`, and the swap backend
/// likewise concatenates into `dst` per range. Overlapping ranges would
/// duplicate bytes in `dst` silently, which is almost certainly not what the
/// caller meant; reject upfront so misuse fails loud.
fn assert_ranges_disjoint(ranges: &[(usize, usize)]) {
    // Skip zero-length ranges; they cannot overlap anything by definition.
    let mut sorted: Vec<(usize, usize)> = ranges.iter().copied().filter(|&(_, l)| l > 0).collect();
    sorted.sort_by_key(|&(off, _)| off);
    let mut prev_end: usize = 0;
    for (off, len) in sorted {
        assert!(
            off >= prev_end,
            "read_at_many: overlapping ranges (range starting at {off} overlaps a previous range ending at {prev_end})",
        );
        prev_end = off
            .checked_add(len)
            .expect("range offset+len overflow in assert_ranges_disjoint");
    }
}

/// Reads a single range. Convenience wrapper around [`read_at_many`].
pub fn read_at(handle: &Handle, offset: usize, len: usize, dst: &mut Vec<u64>) {
    read_at_many(handle, &[(offset, len)], dst);
}

/// Fallible counterpart to [`read_at`].
pub fn try_read_at(
    handle: &Handle,
    offset: usize,
    len: usize,
    dst: &mut Vec<u64>,
) -> std::io::Result<()> {
    try_read_at_many(handle, &[(offset, len)], dst)
}

/// Consumes handle, writing the entire payload into `dst` (cleared first), then reclaims storage.
/// Swap fast path: single-chunk handle into empty `dst` swaps in place, no copy.
///
/// Panics on I/O failure. Use [`try_take`] to surface the error.
pub fn take(handle: Handle, dst: &mut Vec<u64>) {
    match &handle.inner {
        HandleInner::Swap(_) => swap::take_swap(handle, dst),
        HandleInner::File(_) => file::take_file(handle, dst),
    }
}

/// Fallible counterpart to [`take`]. On I/O error the handle is consumed and
/// `dst` may hold partial data; the scratch file is unlinked on inner drop.
pub fn try_take(handle: Handle, dst: &mut Vec<u64>) -> std::io::Result<()> {
    match &handle.inner {
        HandleInner::Swap(_) => {
            swap::take_swap(handle, dst);
            Ok(())
        }
        HandleInner::File(_) => file::try_take_file(handle, dst),
    }
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

    #[mz_ore::test]
    fn disjoint_check_accepts_sorted_adjacent_and_unsorted_disjoint() {
        // Adjacent: not overlapping.
        assert_ranges_disjoint(&[(0, 4), (4, 4)]);
        // Sorted with gaps.
        assert_ranges_disjoint(&[(0, 2), (10, 3), (100, 1)]);
        // Unsorted but disjoint.
        assert_ranges_disjoint(&[(10, 3), (0, 2), (100, 1)]);
        // Zero-length ranges are always OK.
        assert_ranges_disjoint(&[(5, 0), (5, 0), (5, 0)]);
    }

    #[mz_ore::test]
    #[should_panic(expected = "overlapping ranges")]
    fn disjoint_check_rejects_overlap() {
        assert_ranges_disjoint(&[(0, 4), (2, 4)]);
    }

    #[mz_ore::test]
    #[should_panic(expected = "overlapping ranges")]
    fn disjoint_check_rejects_overlap_unsorted() {
        assert_ranges_disjoint(&[(10, 5), (0, 2), (12, 1)]);
    }

    #[mz_ore::test]
    #[should_panic(expected = "overlapping ranges")]
    fn disjoint_check_rejects_duplicate_range() {
        assert_ranges_disjoint(&[(3, 2), (3, 2)]);
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
