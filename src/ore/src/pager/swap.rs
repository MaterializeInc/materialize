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
        *self.prefix.last().unwrap_or(&0)
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
    let ptr = chunk.as_ptr() as usize;
    let len_bytes = chunk.len() * std::mem::size_of::<u64>();
    let aligned_start = (ptr + page - 1) & !(page - 1);
    let aligned_end = (ptr + len_bytes) & !(page - 1);
    if aligned_end <= aligned_start {
        return;
    }
    // SAFETY: pointer/length come from a live `&[u64]`; we restrict to a fully
    // page-aligned subrange contained within that slice; `MADV_COLD` does not
    // mutate the contents.
    unsafe {
        libc::madvise(
            aligned_start as *mut libc::c_void,
            aligned_end - aligned_start,
            libc::MADV_COLD,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn madvise_cold(_chunk: &[u64]) {}

#[cfg(target_os = "linux")]
fn page_size() -> usize {
    // SAFETY: `sysconf` with a valid argument is safe.
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

#[cfg(not(target_os = "linux"))]
fn page_size() -> usize {
    4096
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
}
