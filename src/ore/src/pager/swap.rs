//! Swap backend for the pager. See `mz_ore::pager` for the public API.

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
