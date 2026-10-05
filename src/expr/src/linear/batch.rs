// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sizing the batches of rows that
//! [`SafeMfpPlan::evaluate_batch`](crate::SafeMfpPlan::evaluate_batch)
//! evaluates.

/// Sizes the batches of rows an operator evaluates an MFP over, so that the
/// memory one batch holds stays near [`MfpBatchSizer::TARGET_BYTES`].
///
/// A batch holds its input rows, its outputs, and an arena with every value
/// its evaluation produces until the batch is emitted, including WebAssembly
/// results that a later predicate discards, so a fixed row count does not
/// bound it. The operator reports all three. The sizer starts at
/// [`MfpBatchSizer::MIN_ROWS`], doubles after a full batch that stayed under
/// half the target, and after a batch over the target shrinks to half the
/// target's worth of rows at that batch's bytes per row. A jump in row sizes
/// can therefore overshoot the target once before the size adapts.
///
/// Grouping never changes results, since batched evaluation gives every row
/// the outcome per-row evaluation would.
#[derive(Clone, Debug)]
pub struct MfpBatchSizer {
    rows: usize,
    max_rows: usize,
}

impl MfpBatchSizer {
    /// The bytes one batch aims to stay under.
    pub const TARGET_BYTES: usize = 64 << 20;
    /// The batch size the sizer starts from.
    pub const MIN_ROWS: usize = 64;

    /// A sizer whose batches never exceed `max_rows` rows.
    pub fn new(max_rows: usize) -> Self {
        let max_rows = max_rows.max(1);
        MfpBatchSizer {
            rows: Self::MIN_ROWS.min(max_rows),
            max_rows,
        }
    }

    /// The number of rows the next batch should have.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Records that a batch of `rows` rows held `bytes`.
    pub fn observe(&mut self, rows: usize, bytes: usize) {
        if bytes > Self::TARGET_BYTES {
            let per_row = bytes / rows.max(1);
            self.rows = (Self::TARGET_BYTES / 2 / per_row.max(1)).clamp(1, self.max_rows);
        } else if rows >= self.rows && bytes < Self::TARGET_BYTES / 2 {
            self.rows = self.rows.saturating_mul(2).min(self.max_rows);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MfpBatchSizer;

    #[mz_ore::test]
    fn batch_sizer_follows_batch_bytes() {
        let mut sizer = MfpBatchSizer::new(16 << 10);
        assert_eq!(sizer.rows(), MfpBatchSizer::MIN_ROWS);
        // Small rows grow the batch to the maximum, one doubling per batch.
        while sizer.rows() < 16 << 10 {
            let rows = sizer.rows();
            sizer.observe(rows, rows * 100);
        }
        // A tail batch smaller than the size does not grow it further.
        sizer.observe(10, 1000);
        assert_eq!(sizer.rows(), 16 << 10);
        // Rows of 1 MiB shrink the batch to half the target's worth.
        sizer.observe(16 << 10, (16 << 10) * (1 << 20));
        assert_eq!(sizer.rows(), MfpBatchSizer::TARGET_BYTES / 2 / (1 << 20));
        // A single row larger than the target still makes a batch of one.
        sizer.observe(1, 2 * MfpBatchSizer::TARGET_BYTES);
        assert_eq!(sizer.rows(), 1);
    }
}
