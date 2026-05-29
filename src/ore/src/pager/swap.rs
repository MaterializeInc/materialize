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

//! Swap backend for the pager. See `mz_ore::pager` for the public API.

use crate::pager::Handle;

/// Storage for a swap-backed handle.
#[derive(Debug)]
pub(crate) struct SwapInner {
    /// Logical chunks; logical layout is concatenation in this order.
    pub(crate) chunks: Vec<Vec<u64>>,
    /// Cumulative element counts; `prefix[i]` = sum of `chunks[..i]` lengths.
    /// `prefix[0] == 0`, `prefix.last() == total_len`.
    pub(crate) prefix: Vec<usize>,
}

impl SwapInner {
    pub(crate) fn new(chunks: Vec<Vec<u64>>) -> Self {
        let mut prefix = Vec::with_capacity(chunks.len() + 1);
        prefix.push(0);
        let mut sum = 0;
        for c in &chunks {
            sum += c.len();
            prefix.push(sum);
        }
        Self { chunks, prefix }
    }

    pub(crate) fn total_len(&self) -> usize {
        // `new` always pushes the initial 0, so `prefix` has at least one element.
        *self
            .prefix
            .last()
            .expect("SwapInner::prefix invariant: at least [0]")
    }
}

pub(crate) fn pageout_swap(chunks: &mut [Vec<u64>]) -> Handle {
    let mut taken: Vec<Vec<u64>> = Vec::with_capacity(chunks.len());
    for c in chunks.iter_mut() {
        taken.push(std::mem::take(c));
    }
    for c in &taken {
        madvise_cold(c);
    }
    Handle::from_swap(SwapInner::new(taken))
}

#[cfg(target_os = "linux")]
fn madvise_cold(chunk: &[u64]) {
    if chunk.is_empty() {
        return;
    }
    let page = page_size();
    let base_ptr = chunk.as_ptr();
    let base_addr = base_ptr.addr();
    // `Vec<u64>` cannot exceed `isize::MAX` bytes, so this multiplication
    // cannot overflow on any supported target. Use `checked_mul` for
    // defense-in-depth: a corrupted length should fail loudly, not wrap.
    let Some(len_bytes) = chunk.len().checked_mul(std::mem::size_of::<u64>()) else {
        return;
    };
    // Round the start up and the end down to page boundaries. Both additions
    // use `checked_add` so that an allocation sitting near the top of the
    // address space can never silently wrap into a tiny range.
    let Some(start_unaligned) = base_addr.checked_add(page - 1) else {
        return;
    };
    let Some(end_unaligned) = base_addr.checked_add(len_bytes) else {
        return;
    };
    let aligned_start_addr = start_unaligned & !(page - 1);
    let aligned_end_addr = end_unaligned & !(page - 1);
    if aligned_end_addr <= aligned_start_addr {
        return;
    }
    let aligned_len = aligned_end_addr - aligned_start_addr;
    // SAFETY: `aligned_start_addr` lies in `[base_addr, base_addr + len_bytes]`
    // by construction (rounding up the start cannot exceed `end_unaligned`,
    // which equals `base_addr + len_bytes`; the early-return above guarantees
    // `start ≤ end`). That interval is exactly the range covered by the live
    // `&[u64]`, so `byte_add` stays in-bounds and preserves provenance.
    let aligned_ptr = unsafe { base_ptr.byte_add(aligned_start_addr - base_addr) }
        .cast::<libc::c_void>()
        .cast_mut();
    // SAFETY: pointer/length describe a fully page-aligned subrange contained
    // within the live `&[u64]` (justified above). `MADV_COLD` is non-mutating;
    // it only signals reclaim preference to the kernel, so concurrent reads
    // of the slice remain sound.
    unsafe {
        libc::madvise(aligned_ptr, aligned_len, libc::MADV_COLD);
    }
}

#[cfg(not(target_os = "linux"))]
fn madvise_cold(_chunk: &[u64]) {}

#[cfg(target_os = "linux")]
fn page_size() -> usize {
    // SAFETY: `sysconf` with a valid argument is safe.
    let raw = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(raw).expect("page size is positive and fits usize")
}

pub(crate) fn read_at_swap(handle: &Handle, ranges: &[(usize, usize)], dst: &mut Vec<u64>) {
    let inner = handle
        .swap_inner()
        .expect("read_at_swap called on non-swap handle");
    let total = inner.total_len();
    let total_out: usize = ranges.iter().map(|(_, l)| *l).sum();
    dst.reserve(total_out);
    for &(off, len) in ranges {
        let end = off.checked_add(len).expect("range offset+len overflow");
        assert!(
            end <= total,
            "read range out of bounds: {off}+{len} > {total}"
        );
        copy_range(inner, off, len, dst);
    }
}

fn copy_range(inner: &SwapInner, off: usize, len: usize, dst: &mut Vec<u64>) {
    if len == 0 {
        return;
    }
    let mut remaining = len;
    let mut cur = off;
    let mut idx = match inner.prefix.binary_search(&cur) {
        Ok(i) => i,
        Err(i) => i.saturating_sub(1),
    };
    while remaining > 0 {
        let chunk_start = inner.prefix[idx];
        let chunk = &inner.chunks[idx];
        let local = cur - chunk_start;
        let take = std::cmp::min(remaining, chunk.len() - local);
        dst.extend_from_slice(&chunk[local..local + take]);
        cur += take;
        remaining -= take;
        idx += 1;
    }
}

pub(crate) fn take_swap(handle: Handle, dst: &mut Vec<u64>) {
    let inner = match handle.into_swap_inner() {
        Some(s) => s,
        None => panic!("take_swap called on non-swap handle"),
    };
    dst.clear();
    let mut chunks = inner.chunks;
    if chunks.len() == 1 && dst.capacity() == 0 {
        let only = chunks.pop().unwrap();
        *dst = only;
        return;
    }
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    dst.reserve(total);
    for c in chunks {
        dst.extend_from_slice(&c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::Handle;

    #[mz_ore::test]
    fn pageout_takes_chunks_and_records_lengths() {
        let a = vec![1u64, 2, 3];
        let b = vec![4u64, 5];
        let mut chunks = [a, b];
        let h: Handle = pageout_swap(&mut chunks);
        assert_eq!(h.len(), 5);
        assert!(chunks[0].is_empty());
        assert!(chunks[1].is_empty());
    }

    #[mz_ore::test]
    fn read_at_within_single_chunk() {
        let mut chunks = [vec![10u64, 11, 12, 13, 14]];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        read_at_swap(&h, &[(1, 3)], &mut dst);
        assert_eq!(dst, vec![11, 12, 13]);
    }

    #[mz_ore::test]
    fn read_at_spans_chunks() {
        let mut chunks = [vec![1u64, 2, 3], vec![4, 5, 6]];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        read_at_swap(&h, &[(2, 3)], &mut dst);
        assert_eq!(dst, vec![3, 4, 5]);
    }

    #[mz_ore::test]
    fn read_at_many_concats() {
        let mut chunks = [vec![1u64, 2, 3, 4, 5]];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        read_at_swap(&h, &[(0, 2), (3, 2)], &mut dst);
        assert_eq!(dst, vec![1, 2, 4, 5]);
    }

    #[mz_ore::test]
    #[should_panic(expected = "out of bounds")]
    fn read_at_panics_on_oob() {
        let mut chunks = [vec![1u64, 2]];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        read_at_swap(&h, &[(1, 5)], &mut dst);
    }

    #[mz_ore::test]
    fn take_single_chunk_zero_copy() {
        let v = vec![100u64; 1024];
        let ptr_before = v.as_ptr();
        let mut chunks = [v];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        take_swap(h, &mut dst);
        assert_eq!(dst.len(), 1024);
        assert_eq!(
            dst.as_ptr(),
            ptr_before,
            "single-chunk take should be zero-copy"
        );
    }

    #[mz_ore::test]
    fn take_multi_chunk_concats() {
        let mut chunks = [vec![1u64, 2], vec![3, 4, 5]];
        let h = pageout_swap(&mut chunks);
        let mut dst = Vec::new();
        take_swap(h, &mut dst);
        assert_eq!(dst, vec![1, 2, 3, 4, 5]);
    }
}
