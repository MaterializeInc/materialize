// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! What an index peek reports about the walk that answers it.
//!
//! A walk is run by one driver or by two: the inline slice runs on the timely worker, and a
//! promoted walk finishes what that slice started. The numbers a [`PeekScan`] carries are
//! cumulative over every slice, whichever driver ran it, so the rule that keeps each histogram
//! observed exactly once per walk is that **the driver which produces the walk's terminal outcome
//! reports it**. Promotion is not a terminal outcome, so an inline slice that promotes reports
//! nothing and the task that finishes the walk reports the whole set.
//!
//! A walk that is cancelled reports nothing, which is why the substrate counters count walks that
//! ended rather than walks that started.
//!
//! [`PeekScan`]: super::peek_scan::PeekScan

use std::num::NonZeroUsize;
use std::time::Instant;

use mz_compute_client::protocol::response::PeekResponse;
use mz_expr::ColumnOrder;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastLossy;
use mz_ore::metrics::UIntGauge;
use prometheus::{Histogram, IntCounter};

use crate::compute_state::peek_scan::{RowBatch, WalkPhases};
use crate::metrics::WorkerMetrics;

/// The metrics an index peek walk reports, on either substrate.
///
/// Cloned into a promoted walk's task, so that a walk reports the same phases wherever it ran.
#[derive(Clone, Debug)]
pub(super) struct PeekWalkMetrics {
    /// Counts walks that ended on the timely worker.
    walks_inline: IntCounter,
    /// Counts walks that ended away from the timely worker.
    walks_offloaded: IntCounter,
    /// Counts walks that answered from the peek response stash.
    walks_stashed: IntCounter,
    error_scan_seconds: Histogram,
    cursor_setup_seconds: Histogram,
    row_iteration_seconds: Histogram,
    row_iteration_rows: Histogram,
    result_sort_seconds: Histogram,
    result_sort_rows: Histogram,
    row_collection_seconds: Histogram,
    /// How many promoted walks are waiting for a permit. Reported by the promoted driver alone,
    /// which is the only one that queues.
    permit_queue_depth: UIntGauge,
    /// How long a promoted walk waited for its permit. Reported by the promoted driver alone.
    permit_wait_seconds: Histogram,
}

impl PeekWalkMetrics {
    pub(super) fn new(metrics: &WorkerMetrics) -> Self {
        Self {
            walks_inline: metrics.index_peek_walks_inline.clone(),
            walks_offloaded: metrics.index_peek_walks_offloaded.clone(),
            walks_stashed: metrics.index_peek_stashed_total.clone(),
            error_scan_seconds: metrics.index_peek_error_scan_seconds.clone(),
            cursor_setup_seconds: metrics.index_peek_cursor_setup_seconds.clone(),
            row_iteration_seconds: metrics.index_peek_row_iteration_seconds.clone(),
            row_iteration_rows: metrics.index_peek_row_iteration_rows.clone(),
            result_sort_seconds: metrics.index_peek_result_sort_seconds.clone(),
            result_sort_rows: metrics.index_peek_result_sort_rows.clone(),
            row_collection_seconds: metrics.index_peek_row_collection_seconds.clone(),
            permit_queue_depth: metrics.index_peek_permit_queue_depth.clone(),
            permit_wait_seconds: metrics.index_peek_permit_wait_seconds.clone(),
        }
    }

    /// Accounts for a promoted walk joining the queue for a permit.
    ///
    /// The queue is the drain-rate signal the permit bound is watched through, and it has no
    /// second bound, so both numbers have to come from the walks themselves. The returned guard
    /// leaves the queue however the wait ends, a cancellation and an abort included, which is what
    /// keeps the depth from drifting up over a process's life.
    pub(super) fn queued_for_permit(&self) -> PermitWait {
        self.permit_queue_depth.inc();
        PermitWait {
            queue_depth: self.permit_queue_depth.clone(),
            wait_seconds: self.permit_wait_seconds.clone(),
            since: Instant::now(),
        }
    }

    /// Counts a walk that the timely worker drove to an outcome.
    ///
    /// A walk that suspends leaves the worker instead of finishing here, so a peek whose answer
    /// goes to the peek stash never counts here: the driver that writes to the stash is the
    /// promoted one.
    pub(super) fn walked_inline(&self) {
        self.walks_inline.inc();
    }

    /// Counts a walk that a promoted task drove to an outcome, whatever that outcome is.
    ///
    /// A walk cancelled while queued for a permit or while running counts on neither substrate,
    /// which is what makes the two substrates sum to the walks that ended. How many walks were
    /// admitted is a different question, and one the permit queue's own metrics answer.
    pub(super) fn walked_offloaded(&self) {
        self.walks_offloaded.inc();
    }

    /// Counts a walk that answered with a handle to the peek response stash.
    ///
    /// Counted alongside [`Self::walked_offloaded`] rather than instead of it, so the two
    /// substrates still sum to the walks that ended while this reports how many of them the stash
    /// answered.
    pub(super) fn walked_to_stash(&self) {
        self.walks_stashed.inc();
    }

    /// Reports the phases that precede the walk over the ok trace.
    ///
    /// Reported for every terminal outcome, including a walk that hands its rows to the peek
    /// stash, because both phases are over by then whatever the walk does next.
    pub(super) fn observe_error_phase(&self, phases: &WalkPhases) {
        // A peek that its error trace answered reports neither number: that walk stopped where the
        // error was rather than at the end of the trace, and the cursor the second number times
        // was never used.
        if !phases.error_trace_clean {
            return;
        }

        self.error_scan_seconds
            .observe(phases.error_scan.as_secs_f64());
        self.cursor_setup_seconds
            .observe(phases.cursor_setup.as_secs_f64());
    }

    /// Reports the walk over the ok trace, for a walk that completed it.
    ///
    /// A walk that ends any other way reports nothing here, because the rows it examined are not
    /// the rows an answer took.
    pub(super) fn observe_ok_phase(&self, phases: &WalkPhases) {
        self.row_iteration_seconds
            .observe(phases.row_iteration.as_secs_f64());
        self.row_iteration_rows
            .observe(f64::cast_lossy(phases.rows_processed));
        self.result_sort_seconds
            .observe(phases.result_sort.as_secs_f64());
        self.result_sort_rows
            .observe(f64::cast_lossy(phases.rows_sorted));
    }

    /// Builds the peek's answer out of the rows a completed walk produced.
    ///
    /// Both drivers build the answer here, and the build is timed here, so the time is reported
    /// wherever the walk finished rather than only where it started.
    pub(super) fn rows_response(&self, rows: RowBatch, order_by: &[ColumnOrder]) -> PeekResponse {
        let start = Instant::now();

        let rows = rows
            .into_iter()
            .map(|(row, copies)| {
                let copies = NonZeroUsize::try_from(copies).expect("fits into usize");
                (row, copies)
            })
            .collect();
        let collection = RowCollection::new(rows, order_by);

        self.row_collection_seconds
            .observe(start.elapsed().as_secs_f64());

        PeekResponse::Rows(vec![collection])
    }
}

/// A promoted walk's place in the queue for a permit, for as long as it holds one.
///
/// Leaving the queue is a drop rather than a call, so a walk that is cancelled or aborted while it
/// waits leaves the queue as surely as one that is admitted.
pub(super) struct PermitWait {
    queue_depth: UIntGauge,
    wait_seconds: Histogram,
    since: Instant,
}

impl PermitWait {
    /// Reports the wait of a walk that was admitted.
    ///
    /// A walk that leaves the queue any other way reports nothing, so the histogram describes
    /// waits that ended in a permit rather than waits that ended.
    pub(super) fn admitted(self) {
        self.wait_seconds
            .observe(self.since.elapsed().as_secs_f64());
    }
}

impl Drop for PermitWait {
    fn drop(&mut self) {
        self.queue_depth.dec();
    }
}

/// The metrics an index peek reports from the worker that owns it.
///
/// The two histograms here time the worker's own handling of a peek, which no promoted walk
/// repeats, so unlike the walk's own metrics they have a single observer by construction.
pub(super) struct IndexPeekMetrics<'a> {
    pub seek_fulfillment_seconds: &'a Histogram,
    pub frontier_check_seconds: &'a Histogram,
    /// The metrics of the walk itself, which a promoted walk reports too.
    pub walk: &'a PeekWalkMetrics,
}
