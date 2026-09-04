// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! An index peek's walk over the two traces that answer it, as one suspendable object.
//!
//! A [`PeekScan`] owns both cursors, the rows it has accumulated, and the accounting that bounds
//! them, and it spends a single budget across both phases. It performs no IO and never awaits, so
//! the same scan runs wherever its driver puts it, and a driver that stops it between two cursor
//! positions picks it up again without repeating work.

use std::mem;
use std::num::{NonZeroI64, NonZeroUsize};
use std::time::{Duration, Instant};

use differential_dataflow::trace::cursor::BatchCursor;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_expr::row::RowCollection;
use mz_expr::{ColumnOrder, RowComparator};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use timely::order::PartialOrder;

use crate::compute_state::error_scan::{ErrorScan, ErrorScanStep, ErrsHandle};
use crate::compute_state::peek_result_iterator::{PeekResultIterator, Step};

/// The scan an index peek builds, over the ok trace of the arrangement that answers it.
pub(super) type IndexPeekScan = PeekScan<
    crate::arrangement::manager::PaddedTrace<crate::typedefs::RowRowAgent<Timestamp, Diff>>,
>;

/// Rows a scan hands to its driver, in the order the scan produced them.
///
/// The form [`PeekResultIterator`] yields and the form the peek stash carries, so the path that
/// moves large volumes never converts.
pub(super) type RowBatch = Vec<(Row, NonZeroI64)>;

/// Builds the peek's answer out of the rows a completed walk produced, sorted by `order_by`.
pub(super) fn rows_response(rows: RowBatch, order_by: &[ColumnOrder]) -> PeekResponse {
    let rows = rows
        .into_iter()
        .map(|(row, copies)| {
            let copies = NonZeroUsize::try_from(copies).expect("fits into usize");
            (row, copies)
        })
        .collect();
    PeekResponse::Rows(vec![RowCollection::new(rows, order_by)])
}

/// The byte size of a row's count, as an answer built from a [`RowBatch`] stores it.
pub(super) const COUNT_BYTE_SIZE: usize = size_of::<NonZeroUsize>();

/// The byte size of a row's offset into the answer's packed row data.
const OFFSET_BYTE_SIZE: usize = size_of::<usize>();

/// The bytes one `(row, count)` entry contributes to the answer it ends up in.
///
/// This is `RowCollection::byte_len` per entry, the ruler `max_result_size` is applied with
/// wherever a result is measured against it: the row's packed data, its offset into that data,
/// and its count. The `Row` struct's own bytes are not part of it, because no answer carries them,
/// and charging them measures a narrow row at up to twice what the client receives.
pub(super) fn entry_byte_len(row: &Row) -> usize {
    row.data_len()
        .saturating_add(OFFSET_BYTE_SIZE)
        .saturating_add(COUNT_BYTE_SIZE)
}

/// What a walk has spent, in the phases the peek metrics report.
///
/// Every number is cumulative over the slices the walk was cut into, wherever those slices ran.
#[derive(Clone, Copy, Debug)]
pub(super) struct WalkPhases {
    /// Worker time the error walk spent.
    pub error_scan: Duration,
    /// Worker time spent opening the ok cursor.
    pub cursor_setup: Duration,
    /// Whether the error walk ended without finding an error. The two numbers above describe a
    /// finished phase only when it did.
    pub error_trace_clean: bool,
    /// Worker time the ok walk spent, including the time thinning spent sorting.
    pub row_iteration: Duration,
    /// Cursor positions the ok walk evaluated.
    pub rows_processed: usize,
    /// Worker time thinning spent sorting.
    pub result_sort: Duration,
    /// Rows handed to a sort, summed over the times thinning ran.
    pub rows_sorted: usize,
}

/// How a scan's rows may leave for the peek stash.
#[derive(Clone, Copy, Debug)]
pub(super) struct StashBounds {
    /// Whether the peek may divert its rows to the stash at all.
    pub eligible: bool,
    /// The accumulated size past which the first batch is handed over, which is where the answer
    /// stops being an inline one.
    pub threshold_bytes: usize,
    /// The accumulated size past which every later batch is handed over. A batch is never smaller
    /// than the threshold, so a value below it changes nothing.
    pub batch_bytes: usize,
}

/// The outcome of a fueled [`PeekScan::step`].
#[derive(Clone, Debug, PartialEq)]
pub(super) enum ScanOutcome {
    /// Stopped with work left, because the budget ran out or because the accumulated rows have
    /// grown into a full batch.
    ///
    /// The scan retains what it accumulated. A driver that can write rows collects them through
    /// [`PeekScan::take_batch`], and one that cannot is never handed rows it would have to drop.
    ///
    /// A driver must take every batch it is offered. A scan holding one makes no progress when
    /// stepped, so a driver that steps without taking spins forever.
    Suspended,
    /// The walk is over. `Ok` carries the rows accumulated since the last batch was taken, which
    /// together with the batches already taken are the peek's answer. `Err` is the peek's answer
    /// instead, and the scan has dropped the rows it had accumulated, since they are part of no
    /// answer.
    Finished(Result<RowBatch, PeekError>),
}

/// The state of a [`PeekScan`]'s walk over its error trace.
///
/// Both ended states drop the walk, so a peek pins error batches only while it reads them.
enum ErrorPhase {
    /// The walk is under way, and resumes from the cursor position it stopped on.
    Scanning(ErrorScan),
    /// The error trace holds no error at the peek's timestamp, which is the only way to the ok
    /// trace. The rows the walk examined have been handed to the ok walk.
    Clean,
    /// The error trace answered the peek. The answer is the scan's latched outcome.
    Failed,
}

/// An index peek's walk over its error trace and its ok trace.
///
/// The walk suspends between any two cursor positions. Both phases spend one budget: the ok walk
/// gets what the error walk leaves.
///
/// A stash-eligible scan retains at most the threshold before its first batch and the batch size
/// after, plus the row that crossed either. A scan that cannot use the stash fills no batch, and
/// `max_result_size` alone bounds its prefix.
pub(super) struct PeekScan<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
{
    /// The time at which the error trace is read.
    peek_timestamp: Timestamp,
    /// The collection the peek reads, for logging.
    target_id: GlobalId,
    error_phase: ErrorPhase,
    /// The walk over the ok trace, reached only once the error walk reports the error trace
    /// clean. Its cursor is opened with the scan, and nothing advances it before then.
    oks: PeekResultIterator<Tr>,
    /// The outcome the scan ended with, or `None` while it can still be stepped. The `Ok` carries
    /// no rows: those left with the [`ScanOutcome::Finished`] that reported the end.
    ended: Option<ScanOutcome>,
    /// Rows accumulated since the last batch was taken.
    results: RowBatch,
    /// The byte size of `results`, as an answer built from them would store them.
    total_size: usize,
    /// The rows the answer holds so far, batches already handed to a driver included, counted in
    /// copies because that is what the finishing's limit counts. Thinning takes its drops back off.
    answer_rows: u64,
    /// The ceiling on what the scan may hold, above which the peek fails.
    max_result_size: usize,
    stash: StashBounds,
    /// Whether a batch has been handed over, which is where the answer stopped being an inline
    /// one and the batch size took over from the threshold.
    stash_bound: bool,
    /// A bound on the rows the peek's finishing needs, `limit + offset`.
    ///
    /// Further limiting happens when the results are collected, so the scan does not have to hold
    /// exactly this many rows, just at least those that would have been returned.
    max_results: Option<usize>,
    /// Orders the rows that thinning keeps. `None` when the finishing imposes no ordering, in
    /// which case the walk ends at the limit rather than thinning at all.
    comparator: Option<RowComparator>,
    /// Worker time the error walk spent, summed over the slices it was cut into.
    pub(super) error_scan_time: Duration,
    /// Worker time spent opening the ok cursor.
    pub(super) cursor_setup_time: Duration,
    /// Worker time the ok walk spent, summed over the slices it was cut into. Includes the time
    /// thinning spent sorting.
    pub(super) row_iteration_time: Duration,
    /// Worker time thinning spent sorting, summed over the times it ran.
    pub(super) result_sort_time: Duration,
    /// Rows handed to a sort, summed over the times thinning ran.
    pub(super) rows_sorted: usize,
}

impl<Tr> PeekScan<Tr>
where
    Tr: TraceReader<Batch: Navigable>,
    for<'a> BatchCursor<Tr>: Cursor<
            Key<'a>: ExtendDatums + Eq,
            KeyContainer: BatchContainer<Owned = Row>,
            Val<'a>: ExtendDatums,
            TimeGat<'a>: PartialOrder<Timestamp>,
            DiffGat<'a> = &'a Diff,
        >,
{
    /// Opens a scan of `peek` over the traces that answer it.
    ///
    /// Both cursors are opened here, so that the scan holds everything it reads and needs neither
    /// trace handle again. The walks start without a row-iteration limit. The limit in effect is
    /// the caller's to supply to each [`PeekScan::step`].
    pub(super) fn new(
        peek: &Peek,
        errs_handle: &mut ErrsHandle,
        oks_handle: &mut Tr,
        max_result_size: u64,
        stash: StashBounds,
    ) -> Self {
        let error_scan = ErrorScan::new(errs_handle);
        let error_scan_time = error_scan.scan_time;

        let cursor_setup_start = Instant::now();
        // The literal constraints are cloned rather than moved out of the peek, which outlives
        // this scan.
        let oks = PeekResultIterator::new(
            peek.target.id(),
            peek.map_filter_project.clone(),
            peek.timestamp,
            peek.literal_constraints.clone().as_deref_mut(),
            oks_handle,
            None,
            0,
        );
        let cursor_setup_time = cursor_setup_start.elapsed();

        let comparator = (!peek.finishing.order_by.is_empty())
            .then(|| RowComparator::new(peek.finishing.order_by.clone()));

        Self {
            peek_timestamp: peek.timestamp,
            target_id: peek.target.id(),
            error_phase: ErrorPhase::Scanning(error_scan),
            oks,
            ended: None,
            results: Vec::new(),
            total_size: 0,
            answer_rows: 0,
            max_result_size: usize::cast_from(max_result_size),
            stash,
            stash_bound: false,
            max_results: peek.finishing.num_rows_needed(),
            comparator,
            error_scan_time,
            cursor_setup_time,
            row_iteration_time: Duration::ZERO,
            result_sort_time: Duration::ZERO,
            rows_sorted: 0,
        }
    }

    /// Advances the scan until it has an answer for the peek, the accumulated rows make a full
    /// batch, or `fuel` runs out, whichever comes first. Decrements `fuel` by the number of cursor
    /// positions visited, in either phase.
    ///
    /// `row_iteration_limit` is the limit in effect now rather than at the scan's start, and the
    /// count it bounds spans both phases.
    ///
    /// [`ScanOutcome::Suspended`] is not an end of scan: stepping again resumes where this call
    /// stopped. [`ScanOutcome::Finished`] is, and stepping past one is a defect in the driver.
    pub(super) fn step(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> ScanOutcome {
        // The repeat is not the answer this scan gave: the rows left with the first `Finished`.
        // Reporting it rather than panicking keeps a driver that loses track of its scan from
        // taking the replica with it.
        if let Some(ended) = &self.ended {
            soft_panic_or_log!("index peek scan stepped after it ended");
            return ended.clone();
        }

        let outcome = match self.step_error_phase(row_iteration_limit, fuel) {
            Some(outcome) => outcome,
            None => self.step_ok_phase(row_iteration_limit, fuel),
        };

        // Latched here rather than in the arms of either walk, so every way the scan can end
        // passes one place.
        match &outcome {
            ScanOutcome::Suspended => {}
            ScanOutcome::Finished(Ok(_)) => {
                self.ended = Some(ScanOutcome::Finished(Ok(RowBatch::new())));
            }
            ScanOutcome::Finished(Err(error)) => {
                self.ended = Some(ScanOutcome::Finished(Err(error.clone())));
            }
        }

        outcome
    }

    /// Takes the accumulated rows once they have crossed the stash threshold.
    ///
    /// Returns `None` while they have not, and for a peek that cannot use the stash at all, so a
    /// driver with nowhere to write rows is never handed any.
    pub(super) fn take_batch(&mut self) -> Option<RowBatch> {
        if !self.batch_ready() {
            return None;
        }
        self.stash_bound = true;
        Some(self.take_results())
    }

    /// The number of cursor positions the ok walk has evaluated.
    pub(super) fn rows_processed(&self) -> usize {
        self.oks.rows_processed()
    }

    /// What the walk has spent so far, in the phases the peek metrics report.
    pub(super) fn phases(&self) -> WalkPhases {
        WalkPhases {
            error_scan: self.error_scan_time,
            cursor_setup: self.cursor_setup_time,
            error_trace_clean: self.error_trace_clean(),
            row_iteration: self.row_iteration_time,
            rows_processed: self.rows_processed(),
            result_sort: self.result_sort_time,
            rows_sorted: self.rows_sorted,
        }
    }

    /// Whether the walk over the error trace has ended without finding an error, which is the only
    /// way the ok walk runs at all.
    ///
    /// False while that walk is under way, and false once it has answered the peek.
    pub(super) fn error_trace_clean(&self) -> bool {
        matches!(self.error_phase, ErrorPhase::Clean)
    }

    /// Whether this scan may divert rows to the peek stash, and so whether it ever fills a batch.
    ///
    /// A driver reads this rather than deciding eligibility again, so it cannot end up holding a
    /// batch it has nowhere to write. A scan holding an untaken batch makes no progress.
    pub(super) fn stash_eligible(&self) -> bool {
        self.stash.eligible
    }

    /// Whether the accumulated rows have grown past what this peek may answer with inline, which
    /// is when [`PeekScan::take_batch`] hands them over.
    ///
    /// A scan whose batch is ready stays where it stands until the batch is taken, so this is also
    /// whether stepping the scan again can make progress.
    pub(super) fn batch_ready(&self) -> bool {
        let cut = if self.stash_bound {
            self.stash.threshold_bytes.max(self.stash.batch_bytes)
        } else {
            self.stash.threshold_bytes
        };
        self.stash.eligible && self.total_size > cut
    }

    /// Takes the accumulated rows and the size accounted to them.
    ///
    /// Every path that hands rows out goes through here, so `total_size` stays an account of
    /// `results`.
    fn take_results(&mut self) -> RowBatch {
        self.total_size = 0;
        mem::take(&mut self.results)
    }

    /// Fails the peek with `error`, dropping the rows the scan had accumulated, so that
    /// [`PeekScan::take_batch`] never hands a driver the prefix of an answer that will not be
    /// given.
    fn fail(&mut self, error: PeekError) -> ScanOutcome {
        let _dropped = self.take_results();
        ScanOutcome::Finished(Err(error))
    }

    /// Advances the walk over the error trace.
    ///
    /// Returns `None` once the error trace is known to hold no error at the peek's timestamp,
    /// which is the only way to the ok trace.
    fn step_error_phase(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> Option<ScanOutcome> {
        let scan = match &mut self.error_phase {
            ErrorPhase::Scanning(scan) => scan,
            // `step` reports an ended scan before it reaches here, so a failed phase is never
            // seen here.
            ErrorPhase::Clean | ErrorPhase::Failed => return None,
        };

        // The limit bounds the peek, not the call, so a walk already under way adopts the limit
        // that is in effect now rather than the one that was in effect when it started.
        scan.set_row_iteration_limit(row_iteration_limit);
        let outcome = scan.step(self.peek_timestamp, self.target_id, fuel);
        self.error_scan_time = scan.scan_time;

        match outcome {
            ErrorScanStep::Finished(Ok(rows_iterated)) => {
                // The rows the error walk examined count against the peek's limit, so the ok walk
                // continues that count. Runs once per scan, since `Clean` never steps the walk.
                self.oks.add_rows_iterated(rows_iterated);
                self.error_phase = ErrorPhase::Clean;
                None
            }
            ErrorScanStep::Finished(Err(error)) => {
                self.error_phase = ErrorPhase::Failed;
                Some(self.fail(error))
            }
            ErrorScanStep::OutOfFuel => Some(ScanOutcome::Suspended),
        }
    }

    /// Whether the answer holds every row the peek's finishing can use.
    ///
    /// Only ever true without an ordering: an ordered finishing ranks rows against the whole
    /// trace, so no prefix of the walk satisfies it.
    fn finishing_satisfied(&self) -> bool {
        self.comparator.is_none()
            && self
                .max_results
                .is_some_and(|max_results| self.answer_rows >= u64::cast_from(max_results))
    }

    /// Advances the walk over the ok trace, accumulating the rows it produces.
    fn step_ok_phase(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> ScanOutcome {
        // Ahead of the batch guard, so a scan whose last batch completed the answer ends here
        // rather than walking one more row into a batch of its own.
        if self.finishing_satisfied() {
            return ScanOutcome::Finished(Ok(self.take_results()));
        }

        // A scan holding a full batch stays where it is until the batch is taken, so the bound on
        // what one scan retains is the scan's own rather than a rule each driver keeps. Past the
        // stash threshold the result-size ceiling no longer bounds that growth either.
        if self.batch_ready() {
            return ScanOutcome::Suspended;
        }

        self.oks.set_row_iteration_limit(row_iteration_limit);

        let row_iteration_start = Instant::now();

        let outcome = loop {
            let (row, copies) = match self.oks.step(fuel) {
                Step::Row(Ok(row)) => row,
                Step::Row(Err(error)) => break self.fail(error),
                Step::Done => break ScanOutcome::Finished(Ok(self.take_results())),
                Step::OutOfFuel => break ScanOutcome::Suspended,
            };

            self.total_size = self.total_size.saturating_add(entry_byte_len(&row));
            let batch_ready = self.batch_ready();

            // Rows bound for the stash are answered by a handle rather than by themselves, so the
            // ceiling on an inline answer does not apply to a prefix that has grown past the
            // stash threshold, nor to a scan whose first batch has already left: what that scan
            // retains is bounded by the batch size, which may sit above the ceiling.
            if !self.stash_bound && !batch_ready && self.total_size > self.max_result_size {
                break self.fail(PeekError::ResultExceedsMaxSize {
                    max_result_size: self.max_result_size,
                });
            }

            // Positive here: the walk errors on a negative multiplicity rather than yielding it.
            self.answer_rows = self.answer_rows.saturating_add(copies.get().unsigned_abs());
            self.results.push((row, copies));

            // Ahead of thinning, so that a row which both fills a batch and completes a thinned
            // answer leaves the peek to the stash rather than answering it from the prefix.
            if batch_ready {
                break ScanOutcome::Suspended;
            }

            if self.finishing_satisfied() {
                break ScanOutcome::Finished(Ok(self.take_results()));
            }

            if let Some(outcome) = self.thin() {
                break outcome;
            }
        };

        self.row_iteration_time += row_iteration_start.elapsed();

        outcome
    }

    /// Thins the accumulated rows down to the ones an ordered finishing ranks first, once the scan
    /// holds many more than it needs.
    ///
    /// Does nothing without an ordering: such a scan ends at [`PeekScan::finishing_satisfied`]
    /// instead of accumulating past its limit.
    fn thin(&mut self) -> Option<ScanOutcome> {
        let max_results = self.max_results?;
        let Some(comparator) = &self.comparator else {
            return None;
        };

        // We use a threshold twice what we intend, to amortize the work across all of the
        // insertions. We could tighten this, but it works for the moment.
        //
        // `max_results` is `limit + offset`, so a `LIMIT` near `i64::MAX` makes the doubling
        // overflow. We then hold fewer rows than the threshold no matter what, and never thin.
        // That is the right answer: such a peek cannot accumulate that many rows anyway, the
        // result size limit stops it long before. Wrapping instead would make the threshold tiny,
        // and we would thin while holding almost nothing, dropping rows past the end of the
        // buffer.
        let thin = max_results
            .checked_mul(2)
            .is_some_and(|threshold| self.results.len() >= threshold);
        if !thin {
            return None;
        }

        // We can sort `results` and then truncate to `max_results`. This has an effect similar to
        // a priority queue, without its interactive dequeueing properties.
        // TODO: Had we left these as `Vec<Datum>` we would avoid the unpacking; we should consider
        // doing that, although it will require a re-pivot of the code to branch on this inner test
        // (as we prefer not to maintain `Vec<Datum>` in the other case).
        let sort_start = Instant::now();
        self.rows_sorted = self.rows_sorted.saturating_add(self.results.len());
        self.results.sort_by(|left, right| {
            comparator.compare_rows(&left.0, &right.0, || left.0.cmp(&right.0))
        });
        self.result_sort_time += sort_start.elapsed();

        let dropped = self.results.drain(max_results..);
        let (dropped_size, dropped_rows) = dropped.into_iter().fold(
            (0usize, 0u64),
            |(size, rows), (row, count): (Row, NonZeroI64)| {
                (
                    size.saturating_add(entry_byte_len(&row)),
                    rows.saturating_add(count.get().unsigned_abs()),
                )
            },
        );
        self.total_size = self.total_size.saturating_sub(dropped_size);
        self.answer_rows = self.answer_rows.saturating_sub(dropped_rows);

        None
    }
}

#[cfg(test)]
mod tests;
