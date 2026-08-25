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
use crate::typedefs::ErrAgent;

use super::{PeekRowIterationTracker, peek_result_iterator};

/// The error trace of an index, as
/// [`TraceBundle::errs_mut`](crate::arrangement::manager::TraceBundle::errs_mut) hands it out.
pub(super) type ErrsHandle = PaddedTrace<ErrAgent<Timestamp, Diff>>;

/// A walk over an index peek's error trace, suspendable between cursor positions.
///
/// Owns the cursor, the batches it reads from, the count of rows the walk has examined, and the
/// worker time it has spent. Each of those outlives a single [`ErrorScan::step`]: the cursor and
/// its batches because a suspended walk resumes at the key it stopped on, the count because the
/// row-iteration limit spans the error trace and the ok trace together, and the time because the
/// scan's cost is the sum of its slices rather than the wall clock spanning them.
///
/// It owns nothing of the ok trace, of the rows a peek returns, or of their size: a peek reaches
/// those only once this walk reports [`ErrorScanStep::Clean`].
pub(super) struct ErrorScan {
    cursor: peek_result_iterator::TraceCursor<ErrsHandle>,
    storage: peek_result_iterator::TraceStorage<ErrsHandle>,
    row_iteration_tracker: PeekRowIterationTracker,
    /// Worker time spent walking, summed over the calls the walk was sliced into.
    pub(super) scan_time: Duration,
}

/// The outcome of a fueled [`ErrorScan::step`].
#[derive(Debug, PartialEq)]
pub(super) enum ErrorScanStep {
    /// The error trace holds no error at the peek's timestamp, so the peek's answer comes from
    /// the ok trace. `rows_iterated` is the count the walk accrued, which the ok scan continues
    /// from.
    Clean { rows_iterated: usize },
    /// The peek's answer: an error the trace holds at the peek's timestamp, or a failure of the
    /// peek itself.
    Answer(PeekError),
    /// The fuel ran out before the walk reached either. The walk resumes at the cursor position
    /// it stopped on, and that position has not been examined yet.
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
        let mut scan = Self::from_cursor(cursor, storage);
        scan.scan_time = scan_start.elapsed();
        scan
    }

    /// Opens a walk over an already-opened cursor.
    pub(super) fn from_cursor(
        cursor: peek_result_iterator::TraceCursor<ErrsHandle>,
        storage: peek_result_iterator::TraceStorage<ErrsHandle>,
    ) -> Self {
        Self {
            cursor,
            storage,
            row_iteration_tracker: PeekRowIterationTracker::new(None, 0),
            scan_time: Duration::ZERO,
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
    /// per position rather than per answer. Otherwise a trace that has accumulated many such keys
    /// would run to its end within a single step, which is the stall the budget exists to bound.
    ///
    /// A terminal outcome does not latch here.
    /// [`PeekScan`](super::peek_scan::PeekScan)'s error phase holds the state that latches it, so
    /// that a caller which steps again gets that outcome back without the walk examining the
    /// position it stopped on a second time. The latch cannot live in the walk itself: a walk that
    /// latched its own outcome could not release the `cursor` and `storage` it is reading through,
    /// short of making both fields `Option`s that every step unwraps, and a finished peek would go
    /// on pinning error batches it will never read again.
    pub(super) fn step(
        &mut self,
        peek_timestamp: Timestamp,
        target_id: GlobalId,
        fuel: &mut usize,
    ) -> ErrorScanStep {
        let step_start = Instant::now();

        let outcome = loop {
            if *fuel == 0 {
                break ErrorScanStep::OutOfFuel;
            }
            *fuel -= 1;

            if !self.cursor.key_valid(&self.storage) {
                break ErrorScanStep::Clean {
                    rows_iterated: self.row_iteration_tracker.rows_iterated(),
                };
            }

            if let Err(error) = self.row_iteration_tracker.track_next() {
                break ErrorScanStep::Answer(error);
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
                break ErrorScanStep::Answer(PeekError::unstructured(format!(
                    "Invalid data in source errors, \
                    saw retractions ({}) for row that does not exist: {}",
                    -copies, error,
                )));
            }
            if copies.is_positive() {
                let error = self.cursor.key(&self.storage).deserialize();
                break ErrorScanStep::Answer(error.into());
            }
            self.cursor.step_key(&self.storage);
        };

        self.scan_time += step_start.elapsed();
        outcome
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use differential_dataflow::trace::cursor::CursorList;
    use differential_dataflow::trace::{Batcher, Builder, Navigable};
    use mz_expr::EvalError;
    use mz_timely_util::columnation::ColumnationStack;
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::render::errors::DataflowErrorSer;
    use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

    use super::*;

    /// The time at which the peeks in these tests read.
    pub(crate) const PEEK_TIMESTAMP: Timestamp = Timestamp::new(1);

    /// The updates that make up an error trace, in the form its batcher takes them.
    pub(crate) type ErrorUpdates = Vec<((DataflowErrorSer, ()), Timestamp, Diff)>;

    /// A distinct error for `index`.
    ///
    /// The order of the serialized form does not follow `index`, so a test that cares where a
    /// key falls in the walk sorts the errors and picks by position.
    pub(crate) fn error(index: usize) -> DataflowErrorSer {
        DataflowErrorSer::from(EvalError::Internal(format!("error {index}").into()))
    }

    /// Builds a single batch holding `updates`, covering `[0, Timestamp::MAX)`.
    pub(crate) fn error_batch(
        updates: ErrorUpdates,
    ) -> <ErrSpine<Timestamp, Diff> as TraceReader>::Batch {
        let mut batcher = ErrBatcher::<Timestamp, Diff>::new(None, 0);
        let mut chunk = ColumnationStack::with_capacity(updates.len());
        for update in updates {
            chunk.push_into(update);
        }
        batcher.push_into(chunk);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(Timestamp::MAX));
        ErrBuilder::<Timestamp, Diff>::seal(&mut chain, description)
    }

    /// Builds a walk over a single-batch error trace holding `updates`, bounded by
    /// `row_iteration_limit`.
    pub(crate) fn error_scan(
        updates: ErrorUpdates,
        row_iteration_limit: Option<usize>,
    ) -> ErrorScan {
        let storage = vec![error_batch(updates)];
        let cursor = CursorList::new(vec![storage[0].cursor()], &storage);
        let mut scan = ErrorScan::from_cursor(cursor, storage);
        scan.set_row_iteration_limit(row_iteration_limit);
        scan
    }

    /// Updates that put `error` in the trace at a multiplicity that cancels to zero at
    /// [`PEEK_TIMESTAMP`].
    ///
    /// The two updates sit at different times so that they survive consolidation: a key whose
    /// updates consolidate away is not in the trace at all, and the walk never sees it.
    pub(crate) fn cancelling(error: &DataflowErrorSer) -> ErrorUpdates {
        vec![
            ((error.clone(), ()), Timestamp::new(0), Diff::ONE),
            ((error.clone(), ()), PEEK_TIMESTAMP, Diff::MINUS_ONE),
        ]
    }

    /// Updates that put `error` in the trace at a multiplicity of one at [`PEEK_TIMESTAMP`], so
    /// that a walk reaching this key answers the peek with it.
    pub(crate) fn holding(error: &DataflowErrorSer) -> ErrorUpdates {
        vec![((error.clone(), ()), Timestamp::new(0), Diff::ONE)]
    }

    /// Runs `scan` to an answer in slices of `fuel_per_step` units, and returns that answer, the
    /// fuel the walk spent, and the number of calls it took.
    fn run_sliced(scan: &mut ErrorScan, fuel_per_step: usize) -> (ErrorScanStep, usize, usize) {
        let mut consumed = 0;
        // Bounded so that a walk which restarts from the first key on each resumption fails the
        // test instead of hanging it.
        for calls in 1..=100 {
            let mut fuel = fuel_per_step;
            let outcome = scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel);
            consumed += fuel_per_step - fuel;
            if !matches!(outcome, ErrorScanStep::OutOfFuel) {
                return (outcome, consumed, calls);
            }
        }
        panic!("walk did not answer within 100 resumptions");
    }

    /// An error trace whose keys cancel to zero at the peek's timestamp is walked key by key
    /// before the error behind them is found. That walk spends fuel per key, so it suspends and
    /// resumes rather than running the trace out in one call, and slicing it changes neither the
    /// answer nor the work it costs.
    #[mz_ore::test]
    fn error_scan_is_fueled_and_resumable() {
        let mut errors: Vec<DataflowErrorSer> = (0..64).map(error).collect();
        errors.sort();
        let (answering, cancelled) = errors.split_last().expect("non-empty");

        let mut updates: Vec<_> = cancelled.iter().flat_map(cancelling).collect();
        updates.push(((answering.clone(), ()), Timestamp::new(0), Diff::ONE));
        let expected = || ErrorScanStep::Answer(answering.deserialize().into());

        // The walk visits every cancelling key and then the key that answers.
        let expected_fuel = errors.len();

        let mut scan = error_scan(updates.clone(), None);
        let mut fuel = usize::MAX;
        let unbudgeted = scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel);
        assert_eq!(unbudgeted, expected());
        assert_eq!(usize::MAX - fuel, expected_fuel);

        // One unit of fuel per call: the walk answers only if each resumption picks up where the
        // last stopped, and the fuel it spends in total says it neither repeated nor skipped a
        // key.
        let mut scan = error_scan(updates, None);
        let (sliced, consumed, calls) = run_sliced(&mut scan, 1);
        assert_eq!(sliced, expected());
        assert_eq!(consumed, expected_fuel);
        assert!(calls > 1, "the budget did not slice the walk");
    }

    /// A walk that finds no error hands the ok scan the number of rows it examined, and slicing
    /// the walk neither loses nor repeats a key in that count.
    #[mz_ore::test]
    fn error_scan_threads_row_count_across_suspensions() {
        let errors: Vec<DataflowErrorSer> = (0..64).map(error).collect();
        let updates: Vec<_> = errors.iter().flat_map(cancelling).collect();

        // Every key cancels, so the walk visits all of them and then the position past the last
        // key, which is where it learns the trace is exhausted.
        let expected_fuel = errors.len() + 1;

        let mut scan = error_scan(updates.clone(), None);
        let mut fuel = usize::MAX;
        let unbudgeted = scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel);
        assert_eq!(
            unbudgeted,
            ErrorScanStep::Clean {
                rows_iterated: errors.len()
            }
        );
        assert_eq!(usize::MAX - fuel, expected_fuel);

        let mut scan = error_scan(updates, None);
        let (sliced, consumed, calls) = run_sliced(&mut scan, 3);
        assert_eq!(
            sliced,
            ErrorScanStep::Clean {
                rows_iterated: errors.len()
            }
        );
        assert_eq!(consumed, expected_fuel);
        assert!(calls > 1, "the budget did not slice the walk");
    }

    /// The row-iteration limit bounds the error walk too, not only the ok scan that follows it.
    /// A walk that trips the limit answers with [`PeekError::RowIterationLimitExceeded`] at the
    /// key that exceeds it, and it answers at that same key whether it ran in one call or was
    /// sliced into fueled steps. The count the limit is checked against is a count of keys the
    /// walk examined, so slicing must move neither the answer nor the key it comes from.
    #[mz_ore::test]
    fn error_scan_row_iteration_limit_trips_at_the_same_key_when_sliced() {
        let errors: Vec<DataflowErrorSer> = (0..64).map(error).collect();
        let updates: Vec<_> = errors.iter().flat_map(cancelling).collect();

        // Well short of the number of keys, so the limit ends the walk rather than the end of
        // the trace does.
        let limit = 20;
        assert!(limit < errors.len(), "the limit must trip mid-walk");
        let expected = || ErrorScanStep::Answer(PeekError::RowIterationLimitExceeded { limit });
        // The walk examines `limit` keys and trips on the one after them. That count is also the
        // fuel it spends, because both are charged once per cursor position.
        let expected_fuel = limit + 1;

        let mut scan = error_scan(updates.clone(), Some(limit));
        let mut fuel = usize::MAX;
        let unbudgeted = scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel);
        assert_eq!(unbudgeted, expected());
        assert_eq!(usize::MAX - fuel, expected_fuel);

        let mut scan = error_scan(updates, Some(limit));
        let (sliced, consumed, calls) = run_sliced(&mut scan, 3);
        assert_eq!(sliced, expected());
        assert_eq!(
            consumed, expected_fuel,
            "slicing the walk must not move the key the limit trips on"
        );
        assert!(calls > 1, "the budget did not slice the walk");
    }
}
