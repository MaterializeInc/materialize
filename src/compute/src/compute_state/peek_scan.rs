// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cooperative accumulation of an index peek's result.
//!
//! Walking an arrangement can take arbitrarily long, and the compute worker is
//! a single thread that also has to schedule dataflows and handle commands. A
//! [`PeekScan`] therefore does its work in bounded slices: each
//! [`step`](PeekScan::step) spends at most one [`NestedBudget`] and then hands
//! the worker back, keeping enough state to resume where it left off.
//!
//! The scan owns its cursor, and the cursor owns the batches it reads rather
//! than borrowing them from the trace. So a scan is self-contained and parking
//! one between activations is safe. Nothing can compact the data out from under
//! it either, since batches are immutable and a merge allocates new ones.
//!
//! The flip side is that a parked scan keeps its whole batch set alive. A peek
//! already pinned those batches for its lifetime, but a small budget stretches
//! that lifetime out in wall-clock terms, so the memory is held for longer.

use std::num::{NonZeroI64, NonZeroUsize};
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::PeekResponse;
use mz_expr::row::RowCollection;
use mz_expr::{ColumnOrder, RowComparator};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_repr::{Diff, Row, Timestamp};

use crate::arrangement::manager::PaddedTrace;
use crate::compute_state::IndexPeekMetrics;
use crate::compute_state::peek_result_iterator::{PeekResultIterator, Step};
use crate::typedefs::RowRowAgent;
use crate::yielding::NestedBudget;

/// Per-entry overhead we charge on top of the row itself, matching how the
/// result is laid out in a [`RowCollection`].
const COUNT_BYTE_SIZE: usize = size_of::<NonZeroUsize>();

/// The result of one slice of scanning work.
pub(super) enum ScanOutcome {
    /// The budget ran out with work remaining. Call [`PeekScan::step`] again.
    Yielded,
    /// The scan is finished and this is the peek's response.
    Complete(PeekResponse),
    /// The result outgrew what we are willing to send inline. The caller
    /// should abandon this scan and stash the response instead.
    UsePeekStash,
}

/// Why the scan loop stopped.
enum Stop {
    /// No further rows are needed, or none are left. Either way the
    /// accumulated results are the answer.
    Complete,
    /// The scan ended early with this response.
    Response(PeekResponse),
    /// The result outgrew the inline threshold.
    UsePeekStash,
}

/// An index peek's result scan, resumable across worker activations.
pub(super) struct PeekScan {
    iter: PeekResultIterator<PaddedTrace<RowRowAgent<Timestamp, Diff>>>,
    /// Rows accumulated so far, periodically thinned down to `max_results`.
    results: Vec<(Row, NonZeroUsize)>,
    /// Byte size of `results`, kept in sync as rows are added and thinned out.
    total_size: usize,
    /// A bound on the number of records the finishing can need, if it has one.
    ///
    /// Further limiting happens once the results are collected, so we don't
    /// need exactly this many, just at least those that would be returned.
    max_results: Option<usize>,
    order_by: Vec<ColumnOrder>,
    comparator: RowComparator,
    max_result_size: usize,
    /// When set, a result that grows past this many bytes goes to the peek
    /// stash rather than being sent inline.
    peek_stash_threshold_bytes: Option<usize>,
    /// Wall time spent scanning, summed over all slices.
    row_iteration_time: Duration,
    /// Wall time spent sorting during thinning, summed over all slices.
    thinning_time: Duration,
    /// Rows fed to a thinning sort, summed over all slices.
    rows_sorted: usize,
}

impl PeekScan {
    /// Sets up a scan of `oks` at the peek's timestamp.
    ///
    /// The caller must have established that the trace's frontiers permit a
    /// read at `peek.timestamp`. Taking the cursor fixes what the scan will
    /// read, so that check has no meaning once the scan exists.
    pub fn new(
        peek: &Peek,
        oks: &mut PaddedTrace<RowRowAgent<Timestamp, Diff>>,
        max_result_size: usize,
        peek_stash_threshold_bytes: Option<usize>,
        metrics: &IndexPeekMetrics<'_>,
    ) -> Self {
        let cursor_setup_start = Instant::now();

        // NOTE: Setting up the cursor is not budgeted. With literal constraints
        // it sorts them and seeks to the first match, which for a large `IN`
        // list is real work the peek cannot yield out of. Nor is a unit of fuel
        // during the scan a bounded amount of work, see
        // `PeekResultIterator::step`. So the budget bounds how often we get to
        // yield, not the length of any one slice.
        //
        // We clone `literal_constraints` here because we don't want to move the
        // constraints out of the peek struct, and don't want to modify in-place.
        let iter = PeekResultIterator::new(
            peek.target.id(),
            peek.map_filter_project.clone(),
            peek.timestamp,
            peek.literal_constraints.clone().as_deref_mut(),
            oks,
        );

        metrics
            .cursor_setup_seconds
            .observe(cursor_setup_start.elapsed().as_secs_f64());

        Self {
            iter,
            results: Vec::new(),
            total_size: 0,
            max_results: peek.finishing.num_rows_needed(),
            order_by: peek.finishing.order_by.clone(),
            comparator: RowComparator::new(peek.finishing.order_by.clone()),
            max_result_size,
            peek_stash_threshold_bytes,
            row_iteration_time: Duration::ZERO,
            thinning_time: Duration::ZERO,
            rows_sorted: 0,
        }
    }

    /// Performs one slice of scanning work, bounded by `budget`.
    pub fn step(
        &mut self,
        budget: &mut NestedBudget<'_>,
        metrics: &IndexPeekMetrics<'_>,
    ) -> ScanOutcome {
        let slice_start = Instant::now();
        let stop = self.scan(budget);
        self.row_iteration_time += slice_start.elapsed();

        let Some(stop) = stop else {
            return ScanOutcome::Yielded;
        };

        // The scan is over, so the accumulated timings are final.
        metrics
            .row_iteration_seconds
            .observe(self.row_iteration_time.as_secs_f64());
        metrics
            .row_iteration_rows
            .observe(f64::cast_lossy(self.iter.rows_processed()));
        metrics
            .result_sort_seconds
            .observe(self.thinning_time.as_secs_f64());
        metrics
            .result_sort_rows
            .observe(f64::cast_lossy(self.rows_sorted));

        match stop {
            Stop::Complete => {
                let collection_start = Instant::now();
                let results = std::mem::take(&mut self.results);
                let collection = RowCollection::new(results, &self.order_by);
                metrics
                    .row_collection_seconds
                    .observe(collection_start.elapsed().as_secs_f64());
                ScanOutcome::Complete(PeekResponse::Rows(vec![collection]))
            }
            Stop::Response(response) => ScanOutcome::Complete(response),
            Stop::UsePeekStash => ScanOutcome::UsePeekStash,
        }
    }

    /// Runs the cursor until the budget is spent, returning `None` in that
    /// case, or until the scan reaches a terminal state.
    ///
    /// Always advances the cursor by at least one position, even on an
    /// already-spent budget. A yielded peek keeps the worker from parking, so
    /// a slice that does no work at all is a livelock rather than a slow peek.
    /// That makes progress a property of this loop instead of something the
    /// operator has to preserve when setting the budget.
    fn scan(&mut self, budget: &mut NestedBudget<'_>) -> Option<Stop> {
        loop {
            // The iterator charges fuel per outer step, including steps its
            // `map_filter_project` rejects, so a selective filter over a large
            // arrangement still comes back here to have the budget checked.
            let allowance = budget.allowance().max(1);
            let mut fuel = allowance;
            let step = self.iter.step(&mut fuel);
            budget.charge(allowance - fuel);

            match step {
                Step::OutOfFuel => (),
                Step::Done => return Some(Stop::Complete),
                Step::Row(Err(err)) => return Some(Stop::Response(PeekResponse::Error(err))),
                Step::Row(Ok((row, copies))) => {
                    if let Some(stop) = self.absorb(row, copies) {
                        return Some(stop);
                    }
                }
            }

            if budget.is_spent() {
                return None;
            }
        }
    }

    /// Folds one result row into the accumulated results, thinning them down
    /// if they have outgrown what the finishing can need.
    fn absorb(&mut self, row: Row, copies: NonZeroI64) -> Option<Stop> {
        let copies: NonZeroUsize = NonZeroUsize::try_from(copies).expect("fits into usize");

        self.total_size = self
            .total_size
            .saturating_add(row.byte_len())
            .saturating_add(COUNT_BYTE_SIZE);

        if let Some(threshold) = self.peek_stash_threshold_bytes
            && self.total_size > threshold
        {
            return Some(Stop::UsePeekStash);
        }
        if self.total_size > self.max_result_size {
            return Some(Stop::Response(PeekResponse::Error(format!(
                "result exceeds max size of {}",
                ByteSize::b(u64::cast_from(self.max_result_size))
            ))));
        }

        self.results.push((row, copies));

        let Some(max_results) = self.max_results else {
            return None;
        };
        // We use a threshold twice what we intend, to amortize the work across
        // all of the insertions. We could tighten this, but it works for the
        // moment.
        //
        // A `LIMIT` near `i64::MAX` makes that double overflow. Such a peek can
        // never hold that many rows anyway, the result size limit stops it long
        // before, so there is nothing to thin and we just keep accumulating.
        // Wrapping instead would be a worker panic with an `ORDER BY` and a
        // silently truncated answer without one.
        let Some(thin_at) = max_results.checked_mul(2) else {
            return None;
        };
        if self.results.len() < thin_at {
            return None;
        }

        if self.order_by.is_empty() {
            // Any `max_results` rows are as good as any others, so we're done.
            self.results.truncate(max_results);
            return Some(Stop::Complete);
        }

        // Sorting and truncating has an effect similar to a priority queue,
        // without its interactive dequeueing properties.
        // TODO: Had we left these as `Vec<Datum>` we would avoid the unpacking.
        // We should consider doing that, although it will require a re-pivot of
        // the code to branch on this inner test (as we prefer not to maintain
        // `Vec<Datum>` in the other case).
        let sort_start = Instant::now();
        self.rows_sorted = self.rows_sorted.saturating_add(self.results.len());
        let comparator = &self.comparator;
        self.results.sort_by(|left, right| {
            comparator.compare_rows(&left.0, &right.0, || left.0.cmp(&right.0))
        });
        self.thinning_time += sort_start.elapsed();

        let dropped_size = self
            .results
            .drain(max_results..)
            .fold(0usize, |acc, (row, _count)| {
                acc.saturating_add(row.byte_len().saturating_add(COUNT_BYTE_SIZE))
            });
        self.total_size = self.total_size.saturating_sub(dropped_size);

        None
    }
}
