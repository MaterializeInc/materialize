// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! What an index peek reports about the walk that answers it.
//!
//! A walk runs on one driver or two, and the numbers a [`PeekScan`] carries are cumulative over
//! every slice either drove. Each histogram is observed once per walk, by the driver that produces
//! the terminal outcome. An offload is not terminal, so a slice that offloads reports nothing and
//! the task that finishes reports the whole set. A cancelled walk reports nothing, so the substrate
//! counters count walks that ended.
//!
//! [`PeekScan`]: super::peek_scan::PeekScan

use std::time::{Duration, Instant};

use mz_ore::cast::CastLossy;
use mz_ore::metrics::UIntGauge;
use prometheus::{Histogram, IntCounter};

use crate::compute_state::peek_scan::WalkPhases;
use crate::metrics::WorkerMetrics;

/// The metrics an index peek walk reports, on either substrate.
///
/// Cloned into an offloaded walk's task, so that a walk reports the same phases wherever it ran.
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
    /// How many offloaded walks are waiting for a permit. Reported by the offloaded driver alone,
    /// which is the only one that queues.
    permit_queue_depth: UIntGauge,
    /// How long an offloaded walk waited for its permit. Reported by the offloaded driver alone.
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

    /// Accounts for an offloaded walk joining the queue for a permit.
    ///
    /// The returned guard leaves the queue however the wait ends, a cancellation and an abort
    /// included, so the depth cannot drift up over a process's life.
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
    /// A walk that suspends leaves the worker rather than finishing here, so a peek answered from
    /// the peek stash never counts here: the driver that writes to the stash is the offloaded one.
    pub(super) fn walked_inline(&self) {
        self.walks_inline.inc();
    }

    /// Counts a walk that an offloaded task drove to an outcome, whatever that outcome is.
    ///
    /// A walk cancelled while queued or while running counts on neither substrate, as does one
    /// whose task died without an outcome, which the worker answers with an error of its own. The
    /// two therefore sum to the walks that reached an outcome rather than to the peeks answered.
    pub(super) fn walked_offloaded(&self) {
        self.walks_offloaded.inc();
    }

    /// Counts a walk that answered with a handle to the peek response stash.
    ///
    /// Counted alongside [`Self::walked_offloaded`] rather than instead of it, so the two
    /// substrates still sum to the walks that ended.
    pub(super) fn walked_to_stash(&self) {
        self.walks_stashed.inc();
    }

    /// Reports the phases that precede the walk over the ok trace.
    ///
    /// Reported for every terminal outcome, a hand-off to the peek stash included, because both
    /// phases are over by then whatever the walk does next.
    pub(super) fn observe_error_phase(&self, phases: &WalkPhases) {
        // A peek its error trace answered reports neither number: that walk stopped where the
        // error was, and the cursor the second number times was never used.
        if !phases.error_trace_clean {
            return;
        }

        self.error_scan_seconds
            .observe(phases.error_scan.as_secs_f64());
        self.cursor_setup_seconds
            .observe(phases.cursor_setup.as_secs_f64());
    }

    /// Reports the walk over the ok trace, for a walk that completed it. A walk that ends any
    /// other way reports nothing here: the rows it examined are not the rows an answer took.
    pub(super) fn observe_ok_phase(&self, phases: &WalkPhases) {
        self.row_iteration_seconds
            .observe(phases.row_iteration.as_secs_f64());
        self.row_iteration_rows
            .observe(f64::cast_lossy(phases.rows_processed));
        self.result_sort_seconds
            .observe(phases.thinning.as_secs_f64());
        self.result_sort_rows
            .observe(f64::cast_lossy(phases.rows_thinned));
    }

    /// Reports the time [`rows_response`](super::peek_scan::rows_response) took.
    pub(super) fn observe_row_collection(&self, elapsed: Duration) {
        self.row_collection_seconds.observe(elapsed.as_secs_f64());
    }
}

/// An offloaded walk's place in the queue for a permit, for as long as it holds one.
///
/// Leaving the queue is a drop rather than a call, so a cancelled or aborted walk leaves it as
/// surely as an admitted one.
pub(super) struct PermitWait {
    queue_depth: UIntGauge,
    wait_seconds: Histogram,
    since: Instant,
}

impl PermitWait {
    /// Reports the wait of a walk that was admitted. A walk that leaves the queue any other way
    /// reports nothing, so the histogram describes waits that ended in a permit.
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
/// These time the worker's own handling of a peek, which no offloaded walk repeats, so unlike the
/// walk's metrics they have a single observer by construction.
pub(super) struct IndexPeekMetrics<'a> {
    pub seek_fulfillment_seconds: &'a Histogram,
    pub frontier_check_seconds: &'a Histogram,
    /// The metrics of the walk itself, which an offloaded walk reports too.
    pub walk: &'a PeekWalkMetrics,
}
