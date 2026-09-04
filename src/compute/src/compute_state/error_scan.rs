// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An index peek's walk over its error trace, the phase that runs before
//! [`PeekResultIterator`](super::peek_result_iterator::PeekResultIterator) walks the ok trace.

use std::time::{Duration, Instant};

use differential_dataflow::trace::{Cursor, TraceReader};
use mz_compute_client::protocol::response::PeekError;
use mz_repr::{Diff, GlobalId, Timestamp};
use timely::order::PartialOrder;
use tracing::error;

use crate::arrangement::manager::PaddedTrace;
use crate::compute_state::{PeekRowIterationTracker, peek_result_iterator};
use crate::typedefs::ErrAgent;

/// The error trace of an index, as
/// [`TraceBundle::errs_mut`](crate::arrangement::manager::TraceBundle::errs_mut) hands it out.
pub(super) type ErrsHandle = PaddedTrace<ErrAgent<Timestamp, Diff>>;

/// The state of an index peek's walk over its error trace.
pub(super) enum ErrorScanState {
    /// The walk has not started, and holds no cursor yet.
    NotStarted,
    /// The walk is under way, and resumes from the cursor position it stopped on.
    Scanning(ErrorScan),
    /// The walk is over, holding what it needs to report the same outcome again without a second
    /// walk and without a cursor: the rows it examined reaching a trace with no error at the
    /// peek's timestamp, or the error that answers the peek.
    Finished(Result<usize, PeekError>),
}

/// A walk over an index peek's error trace, suspendable between cursor positions.
///
/// Holds nothing of the ok trace or of the rows a peek returns. A peek reaches those only once
/// this walk reports [`ErrorScanStep::Finished`] with an `Ok`.
pub(super) struct ErrorScan {
    cursor: peek_result_iterator::TraceCursor<ErrsHandle>,
    storage: peek_result_iterator::TraceStorage<ErrsHandle>,
    /// The limit spans this walk and the ok scan after it, so the count accrued here is handed
    /// on with [`ErrorScanStep::Finished`].
    row_iteration_tracker: PeekRowIterationTracker,
    /// Worker time spent walking, summed over the calls the walk was sliced into.
    pub(super) scan_time: Duration,
}

/// The outcome of a fueled [`ErrorScan::step`].
#[derive(Debug, PartialEq)]
pub(super) enum ErrorScanStep {
    /// The walk reached its end. `Ok` carries the rows it examined over a trace holding no error
    /// at the peek's timestamp, which the ok scan continues from; `Err` is the peek's answer,
    /// either an error the trace holds or a failure of the walk itself.
    Finished(Result<usize, PeekError>),
    /// The fuel ran out first. The walk resumes at the cursor position it stopped on, and that
    /// position has not been examined yet.
    OutOfFuel,
}

impl ErrorScan {
    /// Opens a walk over `errs`.
    ///
    /// The walk starts without a row-iteration limit. The limit in effect is the caller's to
    /// supply through [`ErrorScan::set_row_iteration_limit`] before each step.
    pub(super) fn new(errs: &mut ErrsHandle) -> Self {
        let scan_start = Instant::now();
        let (cursor, storage) = errs.cursor();
        Self {
            cursor,
            storage,
            row_iteration_tracker: PeekRowIterationTracker::new(None, 0),
            scan_time: scan_start.elapsed(),
        }
    }

    /// Adopts the row-iteration limit that is in effect, without forgetting the rows the walk has
    /// already examined.
    pub(super) fn set_row_iteration_limit(&mut self, limit: Option<usize>) {
        self.row_iteration_tracker.set_limit(limit);
    }

    /// Advances the walk until it has an answer for the peek, the cursor is exhausted, or `fuel`
    /// runs out, whichever comes first. Decrements `fuel` by the number of cursor positions
    /// visited.
    ///
    /// A key whose diffs cancel to zero at `peek_timestamp` yields no answer, so fuel is charged
    /// per position rather than per answer. Otherwise a trace holding many such keys would run to
    /// its end within a single step.
    ///
    /// This walk does not remember that it ended, and stepping it again after it reported
    /// [`ErrorScanStep::Finished`] walks a spent cursor.
    /// [`IndexPeek::step_error_scan`](super::IndexPeek::step_error_scan) remembers instead: it
    /// keeps the outcome and drops the walk, so a finished peek stops pinning error batches.
    pub(super) fn step(
        &mut self,
        peek_timestamp: Timestamp,
        target_id: GlobalId,
        fuel: &mut usize,
    ) -> ErrorScanStep {
        let step_start = Instant::now();

        let outcome = loop {
            // Charged after this, so that finding the trace exhausted costs nothing and an
            // exhausted walk reports the end rather than asking for a budget it cannot spend.
            if !self.cursor.key_valid(&self.storage) {
                break ErrorScanStep::Finished(Ok(self.row_iteration_tracker.rows_iterated()));
            }

            if *fuel == 0 {
                break ErrorScanStep::OutOfFuel;
            }
            *fuel -= 1;

            if let Err(error) = self.row_iteration_tracker.track_next() {
                break ErrorScanStep::Finished(Err(error));
            }

            let mut copies = Diff::ZERO;
            self.cursor.map_times(&self.storage, |time, diff| {
                if time.less_equal(&peek_timestamp) {
                    copies += diff;
                }
            });
            if copies.is_negative() {
                let error = self.cursor.key(&self.storage);
                error!(
                    target = %target_id, diff = %copies, %error,
                    "index peek encountered negative multiplicities in error trace",
                );
                break ErrorScanStep::Finished(Err(PeekError::unstructured(format!(
                    "Invalid data in source errors, \
                    saw retractions ({}) for row that does not exist: {}",
                    -copies, error,
                ))));
            }
            if copies.is_positive() {
                let error = self.cursor.key(&self.storage).deserialize();
                break ErrorScanStep::Finished(Err(error.into()));
            }
            self.cursor.step_key(&self.storage);
        };

        self.scan_time += step_start.elapsed();
        outcome
    }
}

#[cfg(test)]
mod tests;
