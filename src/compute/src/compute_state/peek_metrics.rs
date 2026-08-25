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
    error_scan_seconds: Histogram,
    cursor_setup_seconds: Histogram,
    row_iteration_seconds: Histogram,
    row_iteration_rows: Histogram,
    result_sort_seconds: Histogram,
    result_sort_rows: Histogram,
    row_collection_seconds: Histogram,
}

impl PeekWalkMetrics {
    pub(super) fn new(metrics: &WorkerMetrics) -> Self {
        Self {
            walks_inline: metrics.index_peek_walks_inline.clone(),
            walks_offloaded: metrics.index_peek_walks_offloaded.clone(),
            error_scan_seconds: metrics.index_peek_error_scan_seconds.clone(),
            cursor_setup_seconds: metrics.index_peek_cursor_setup_seconds.clone(),
            row_iteration_seconds: metrics.index_peek_row_iteration_seconds.clone(),
            row_iteration_rows: metrics.index_peek_row_iteration_rows.clone(),
            result_sort_seconds: metrics.index_peek_result_sort_seconds.clone(),
            result_sort_rows: metrics.index_peek_result_sort_rows.clone(),
            row_collection_seconds: metrics.index_peek_row_collection_seconds.clone(),
        }
    }

    /// Counts a walk that the timely worker drove to an outcome.
    ///
    /// A peek that reaches the peek stash counts here, because the walk that decided that ran on
    /// the worker. The stash's own walk of the same trace counts on neither substrate.
    pub(super) fn walked_inline(&self) {
        self.walks_inline.inc();
    }

    /// Counts a walk that was promoted and admitted, whatever it goes on to report.
    ///
    /// A walk cancelled while queued for a permit counts on neither substrate, since it never ran
    /// anywhere but on the inline slice that promoted it.
    pub(super) fn walked_offloaded(&self) {
        self.walks_offloaded.inc();
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
