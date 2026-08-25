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

use bytesize::ByteSize;
use differential_dataflow::trace::cursor::BatchCursor;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::PeekError;
use mz_expr::RowComparator;
use mz_ore::cast::CastFrom;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use timely::order::PartialOrder;

use super::error_scan::{ErrorScan, ErrorScanStep, ErrsHandle};
use super::peek_result_iterator::{PeekResultIterator, Step};

/// The scan an index peek builds, over the ok trace of the arrangement that answers it.
pub(super) type IndexPeekScan = PeekScan<
    crate::arrangement::manager::PaddedTrace<crate::typedefs::RowRowAgent<Timestamp, Diff>>,
>;

// A scan is driven wherever its driver runs, which includes a runtime that may move a task
// between threads, so the concrete scan an index peek builds has to cross threads. Asserting it
// here fails the build if a field is added that does not.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<IndexPeekScan>();
};

/// Rows a scan hands to its driver, in the order the scan produced them.
///
/// This is the item form [`PeekResultIterator`] yields and the form the peek stash carries, so the
/// path that moves large volumes never converts. The property that makes a batch usable, that it
/// is an in-order prefix of the rows the scan has left to give, follows from how the rows were
/// produced rather than from anything a wrapper type could enforce.
pub(super) type RowBatch = Vec<(Row, NonZeroI64)>;

/// The byte size of a row's count, as an answer built from a [`RowBatch`] stores it.
const COUNT_BYTE_SIZE: usize = size_of::<NonZeroUsize>();

/// What a walk has spent, in the phases the peek metrics report.
///
/// A snapshot rather than a view. Every number is cumulative over the slices the walk was cut
/// into, wherever those slices ran, so the driver that ends the walk reads a complete account of
/// it and the driver that promoted it reads nothing.
#[derive(Clone, Copy, Debug)]
pub(super) struct WalkPhases {
    /// Worker time the error walk spent.
    pub error_scan: Duration,
    /// Worker time spent opening the ok cursor.
    pub cursor_setup: Duration,
    /// Whether the error walk ended without finding an error, which is what makes the two numbers
    /// above describe a phase that finished rather than one that was cut short.
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

/// The outcome of a fueled [`PeekScan::step`].
#[derive(Debug, PartialEq)]
pub(super) enum ScanOutcome {
    /// Stopped with work left, because the budget ran out or because the accumulated rows have
    /// grown into a full batch.
    ///
    /// Carries nothing. The scan retains the rows it has accumulated, and a driver that can write
    /// rows collects them through [`PeekScan::take_batch`]. A driver that cannot is never handed
    /// rows it would have to drop.
    ///
    /// The two causes coincide only for a driver that takes every batch it is offered, because
    /// taking one is what lets the walk go on. A driver that cannot write batches has to ask
    /// [`PeekScan::batch_ready`] before it steps again: a scan holding a full batch makes no
    /// progress when stepped, spending no fuel and advancing no cursor, so a driver that steps it
    /// in a loop spins forever.
    Suspended,
    /// The walk is over. Carries the rows accumulated since the last batch was taken, which
    /// together with the batches already taken are the peek's answer.
    Complete(RowBatch),
    /// The peek's answer is this error. Rows accumulated before it are not part of an answer.
    Failed(PeekError),
}

/// The state of a [`PeekScan`]'s walk over its error trace.
///
/// The walk latches on the outcome that ends it, and neither terminal state has a transition out
/// of it. An error therefore stays the peek's answer, and the ok trace is read only by way of
/// [`ErrorPhase::Clean`].
enum ErrorPhase {
    /// The walk is under way, and resumes from the cursor position it stopped on.
    Scanning(ErrorScan),
    /// The walk reached the end of the error trace without finding an error. The rows it examined
    /// have been handed to the ok walk, which continues the count.
    Clean,
    /// The walk found the error that answers the peek.
    Answered(PeekError),
}

/// The outcome a [`PeekScan`]'s walk over its ok trace latched, once it has one.
///
/// Neither outcome leaves the ok cursor at the end of the trace. The unordered truncate in
/// [`PeekScan::thin`] completes the scan mid-trace, and a failure stops on the position that
/// produced it. A walk resumed past either would answer the same peek with a different row set,
/// so the latch answers instead of resuming.
enum OkWalkEnd {
    /// The walk produced every row the peek's answer needs. Those rows left with the
    /// [`ScanOutcome::Complete`] that reported it.
    Complete,
    /// The walk failed the peek with this error.
    Failed(PeekError),
}

/// An index peek's walk over its error trace and its ok trace.
///
/// The walk suspends between any two cursor positions, and both phases spend the same budget:
/// what the error walk leaves is what the ok walk gets, and a phase that exhausts the budget
/// suspends the scan where it stands.
///
/// A scan retains at most `peek_stash_threshold_bytes` of accumulated rows, plus the row that
/// crossed that threshold, because a scan holding a full batch suspends rather than growing its
/// prefix. That is a property of the scan rather than of a driver, so it holds for a scan that is
/// running and for one that is waiting to be driven again.
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
    /// The outcome the ok walk latched, `None` while it is still under way.
    ok_walk_end: Option<OkWalkEnd>,
    /// Rows accumulated since the last batch was taken.
    results: RowBatch,
    /// The byte size of `results`, as an answer built from them would store them.
    total_size: usize,
    /// The ceiling on what the scan may hold, above which the peek fails.
    max_result_size: usize,
    /// Whether this peek may divert its rows to the peek stash.
    peek_stash_eligible: bool,
    peek_stash_threshold_bytes: usize,
    /// A bound on the rows the peek's finishing needs, `limit + offset`.
    ///
    /// Further limiting happens when the results are collected, so the scan does not have to hold
    /// exactly this many rows, just at least those that would have been returned.
    max_results: Option<usize>,
    /// Orders the rows that thinning keeps. `None` when the finishing imposes no ordering, in
    /// which case thinning keeps an arbitrary `max_results` of them, which is what such a
    /// finishing asks for.
    comparator: Option<RowComparator>,
    /// Worker time the error walk spent, summed over the slices it was cut into.
    error_scan_time: Duration,
    /// Worker time spent opening the ok cursor.
    cursor_setup_time: Duration,
    /// Worker time the ok walk spent, summed over the slices it was cut into. Includes the time
    /// thinning spent sorting.
    row_iteration_time: Duration,
    /// Worker time thinning spent sorting, summed over the times it ran.
    result_sort_time: Duration,
    /// Rows handed to a sort, summed over the times thinning ran.
    rows_sorted: usize,
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
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
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
            ok_walk_end: None,
            results: Vec::new(),
            total_size: 0,
            max_result_size: usize::cast_from(max_result_size),
            peek_stash_eligible,
            peek_stash_threshold_bytes,
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
    /// `row_iteration_limit` is the limit in effect now rather than the one in effect when the
    /// scan started, because the limit bounds the peek and applies to a walk already under way.
    /// The count it bounds spans both phases.
    ///
    /// [`ScanOutcome::Suspended`] is not an end of scan: stepping again resumes where this call
    /// stopped. [`ScanOutcome::Complete`] and [`ScanOutcome::Failed`] are, and the scan latches
    /// the first one it reaches, so stepping again repeats that outcome rather than walking on. A
    /// repeated `Complete` carries no rows, because the rows left with the outcome that latched
    /// it.
    pub(super) fn step(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> ScanOutcome {
        if let Some(outcome) = self.step_error_phase(row_iteration_limit, fuel) {
            return outcome;
        }
        if let Some(outcome) = self.latched_ok_outcome() {
            return outcome;
        }

        let outcome = self.step_ok_phase(row_iteration_limit, fuel);

        // Latched here rather than in the arms of the walk, so that every way the walk can end
        // passes the one place that decides whether the scan is over.
        match &outcome {
            ScanOutcome::Suspended => {}
            ScanOutcome::Complete(_) => self.ok_walk_end = Some(OkWalkEnd::Complete),
            ScanOutcome::Failed(error) => self.ok_walk_end = Some(OkWalkEnd::Failed(error.clone())),
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

    /// Whether the accumulated rows have grown past what this peek may answer with inline, which
    /// is when [`PeekScan::take_batch`] hands them over.
    ///
    /// This is how a driver tells a [`ScanOutcome::Suspended`] it can resume from one it cannot:
    /// a scan holding a full batch stays where it stands until the batch is taken.
    pub(super) fn batch_ready(&self) -> bool {
        self.peek_stash_eligible && self.total_size > self.peek_stash_threshold_bytes
    }

    /// Takes the accumulated rows and the size accounted to them.
    ///
    /// Every path that hands the rows out goes through here, because the size is what the ceiling
    /// and the stash threshold are read against, and rows that have left the scan are bounded by
    /// neither.
    fn take_results(&mut self) -> RowBatch {
        self.total_size = 0;
        mem::take(&mut self.results)
    }

    /// The outcome the ok walk latched, or `None` while that walk can still be resumed.
    ///
    /// A latched outcome spends no fuel and reads no trace. The `Complete` it reports carries no
    /// rows, which is what a driver expects of it: the answer is the batches the driver took
    /// followed by the payload of the `Complete` that ended the walk.
    fn latched_ok_outcome(&self) -> Option<ScanOutcome> {
        match &self.ok_walk_end {
            None => None,
            Some(OkWalkEnd::Complete) => Some(ScanOutcome::Complete(RowBatch::new())),
            Some(OkWalkEnd::Failed(error)) => Some(ScanOutcome::Failed(error.clone())),
        }
    }

    /// Advances the walk over the error trace, or reports the outcome it latched.
    ///
    /// Returns `None` once the error trace is known to hold no error at the peek's timestamp,
    /// which is the only way to the ok trace. A latched outcome spends no fuel and reads no
    /// trace, so a scan whose error trace answered it never goes on to return rows instead.
    fn step_error_phase(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> Option<ScanOutcome> {
        let scan = match &mut self.error_phase {
            ErrorPhase::Scanning(scan) => scan,
            ErrorPhase::Clean => return None,
            ErrorPhase::Answered(error) => return Some(ScanOutcome::Failed(error.clone())),
        };

        // The limit bounds the peek, not the call, so a walk already under way adopts the limit
        // that is in effect now rather than the one that was in effect when it started.
        scan.set_row_iteration_limit(row_iteration_limit);
        let outcome = scan.step(self.peek_timestamp, self.target_id, fuel);
        self.error_scan_time = scan.scan_time;

        // Both terminal outcomes drop the walk over the error trace, so that a peek pins error
        // batches only while it is reading them. The ok batches are a separate matter: the scan
        // pins those from construction, because it opens that cursor there.
        match outcome {
            ErrorScanStep::Clean { rows_iterated } => {
                // The rows the error walk examined count against the peek's limit, so the ok walk
                // continues that count rather than starting from zero. This runs once per scan,
                // because the state it moves to answers every further call without stepping.
                self.oks.add_rows_iterated(rows_iterated);
                self.error_phase = ErrorPhase::Clean;
                None
            }
            ErrorScanStep::Answer(error) => {
                self.error_phase = ErrorPhase::Answered(error.clone());
                Some(ScanOutcome::Failed(error))
            }
            ErrorScanStep::OutOfFuel => Some(ScanOutcome::Suspended),
        }
    }

    /// Advances the walk over the ok trace, accumulating the rows it produces.
    fn step_ok_phase(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> ScanOutcome {
        // A scan that already holds a full batch stays where it is until the batch is taken, so
        // the bound on what one scan retains is a property of the scan rather than a rule each
        // driver keeps. Without this, a driver that steps in a loop and takes batches on its own
        // schedule would grow the prefix by a row per call, and past the stash threshold the
        // result-size ceiling no longer bounds that growth either.
        if self.batch_ready() {
            return ScanOutcome::Suspended;
        }

        self.oks.set_row_iteration_limit(row_iteration_limit);

        let row_iteration_start = Instant::now();

        let outcome = loop {
            let (row, copies) = match self.oks.step(fuel) {
                Step::Row(Ok(row)) => row,
                Step::Row(Err(error)) => break ScanOutcome::Failed(error),
                Step::Done => break ScanOutcome::Complete(self.take_results()),
                Step::OutOfFuel => break ScanOutcome::Suspended,
            };

            self.total_size = self
                .total_size
                .saturating_add(row.byte_len())
                .saturating_add(COUNT_BYTE_SIZE);
            let batch_ready = self.batch_ready();

            // Rows bound for the stash are answered by a handle rather than by themselves, so the
            // ceiling on an inline answer does not apply to a prefix that has grown past the
            // stash threshold.
            if !batch_ready && self.total_size > self.max_result_size {
                break ScanOutcome::Failed(PeekError::unstructured(format!(
                    "result exceeds max size of {}",
                    ByteSize::b(u64::cast_from(self.max_result_size))
                )));
            }

            self.results.push((row, copies));

            // A scan with a full batch to give away stops rather than growing its prefix, which is
            // what bounds what one scan retains. Decided ahead of thinning, so that a row which
            // both fills a batch and completes a thinned answer leaves the peek to the stash
            // rather than answering it from the prefix.
            if batch_ready {
                break ScanOutcome::Suspended;
            }

            if let Some(outcome) = self.thin() {
                break outcome;
            }
        };

        self.row_iteration_time += row_iteration_start.elapsed();

        outcome
    }

    /// Thins the accumulated rows down towards the ones the peek's finishing needs, once the scan
    /// holds many more than that.
    ///
    /// Returns the outcome that ends the scan if thinning has produced every row the finishing
    /// can use.
    fn thin(&mut self) -> Option<ScanOutcome> {
        let max_results = self.max_results?;

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

        let Some(comparator) = &self.comparator else {
            // Without an ordering, any `max_results` rows answer the peek, so the rows in hand are
            // an answer and the rest of the trace does not have to be walked.
            self.results.truncate(max_results);
            return Some(ScanOutcome::Complete(self.take_results()));
        };

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
        let dropped_size = dropped
            .into_iter()
            .fold(0, |acc: usize, (row, _count): (Row, _)| {
                acc.saturating_add(row.byte_len().saturating_add(COUNT_BYTE_SIZE))
            });
        self.total_size = self.total_size.saturating_sub(dropped_size);

        None
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::trace::cursor::CursorList;
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatcher;
    use differential_dataflow::trace::{Batcher, Builder, Navigable};
    use mz_expr::{ColumnOrder, RowSetFinishing};
    use mz_ore::num::NonNeg;
    use mz_row_spine::{ArcOrdValBuilder, ArcOrdValSpine};
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::arrangement::manager::{PaddedTrace, TraceBundle};
    use crate::compute_state::error_scan::tests::PEEK_TIMESTAMP;
    use crate::compute_state::index_peek_tests::{
        answering_errors, cancelling_errors, index_peek, ok_row as row, trace_bundle,
        trivial_finishing,
    };
    use crate::typedefs::RowRowAgent;

    use super::*;

    type TestTrace = ArcOrdValSpine<Row, Row, Timestamp, Diff>;

    /// The ok-trace handle a peek reads through, as [`TraceBundle::oks_errs_mut`] hands it out.
    type OksHandle = PaddedTrace<RowRowAgent<Timestamp, Diff>>;

    /// How many times a test resumes a suspended scan before it declares the scan stuck.
    ///
    /// Far above what any trace in this module needs, so a scan that resumes where it stopped
    /// finishes well inside it, and one that restarts a walk on every resumption fails the test
    /// rather than hanging it.
    const RESUMPTION_BOUND: usize = 100;

    /// The rows `value` values yield, in the order the ok walk produces them.
    fn rows(values: impl IntoIterator<Item = u8>) -> Vec<Row> {
        values.into_iter().map(row).collect()
    }

    /// The size the scan accounts a single-column row at.
    fn row_size() -> usize {
        row(0).byte_len() + COUNT_BYTE_SIZE
    }

    /// The rows and counts a completed scan over `values` hands back.
    fn expected(values: impl IntoIterator<Item = u8>) -> RowBatch {
        values
            .into_iter()
            .map(|value| (row(value), NonZeroI64::new(1).expect("non-zero")))
            .collect()
    }

    /// A walk over an ok trace holding `keys`.
    fn ok_iterator(keys: &[Row]) -> PeekResultIterator<TestTrace> {
        let updates: Vec<((Row, Row), Timestamp, Diff)> = keys
            .iter()
            .map(|key| ((key.clone(), Row::default()), Timestamp::MIN, Diff::ONE))
            .collect();
        let mut batcher = OrdValBatcher::<Row, Row, Timestamp, Diff>::new(None, 0);
        batcher.push_into(updates);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(Timestamp::MAX));
        let batch = ArcOrdValBuilder::<Row, Row, Timestamp, Diff>::seal(&mut chain, description);
        let storage = vec![batch];
        let cursor = CursorList::new(vec![storage[0].cursor()], &storage);
        // The cursor's key is the only datum. The values are empty.
        let map_filter_project = mz_expr::MapFilterProject::new(1)
            .into_plan()
            .expect("valid plan")
            .into_nontemporal()
            .expect("non-temporal plan");
        PeekResultIterator::from_cursor(
            GlobalId::User(1),
            map_filter_project,
            Timestamp::MIN,
            None,
            cursor,
            storage,
            None,
            0,
        )
    }

    /// A walk over an error trace holding `keys` errors that each cancel to zero at
    /// [`PEEK_TIMESTAMP`], so the walk examines every one of them and finds no error.
    fn clean_error_scan(keys: usize) -> ErrorScan {
        crate::compute_state::error_scan::tests::error_scan(cancelling_errors(keys), None)
    }

    /// A scan over `keys` whose error phase is `error_phase`, bounded by nothing else.
    ///
    /// Mirrors what [`PeekScan::new`] builds. Tests use this rather than `new` to hold a second
    /// cursor layout under test, and to start from an [`ErrorPhase`] that a fresh scan cannot be
    /// in.
    fn scan(error_phase: ErrorPhase, keys: &[Row]) -> PeekScan<TestTrace> {
        PeekScan {
            peek_timestamp: PEEK_TIMESTAMP,
            target_id: GlobalId::User(1),
            error_phase,
            oks: ok_iterator(keys),
            ok_walk_end: None,
            results: Vec::new(),
            total_size: 0,
            max_result_size: usize::MAX,
            peek_stash_eligible: false,
            peek_stash_threshold_bytes: usize::MAX,
            max_results: None,
            comparator: None,
            error_scan_time: Duration::ZERO,
            cursor_setup_time: Duration::ZERO,
            row_iteration_time: Duration::ZERO,
            result_sort_time: Duration::ZERO,
            rows_sorted: 0,
        }
    }

    /// An error the error trace holds answers the peek, and the ok trace stays unread. Stepping
    /// again repeats the answer rather than moving on to the ok trace, so a peek that must report
    /// an error can never return rows instead.
    #[mz_ore::test]
    fn an_error_answers_the_peek_without_reading_the_ok_trace() {
        let error = PeekError::unstructured("error in the error trace");
        let mut subject = scan(ErrorPhase::Answered(error.clone()), &rows(0..4));

        let mut fuel = 100;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Failed(error.clone())
        );
        assert_eq!(fuel, 100, "a latched answer spends no fuel");
        assert_eq!(subject.rows_processed(), 0);

        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Failed(error));
        assert_eq!(subject.rows_processed(), 0);
    }

    /// One budget covers both phases: what the error walk leaves is what the ok walk gets, and
    /// slicing the scan neither repeats nor skips a cursor position in either of them. Matching
    /// rows alone would not catch that, since a scan that re-walks positions it has already
    /// visited can still land on the right answer.
    #[mz_ore::test]
    fn one_budget_spans_both_phases() {
        let keys = rows(0..6);
        // Every error key cancels, so the error walk visits all of them and then the position
        // past the last, before the ok walk sees anything.
        let error_keys = 4;

        let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
        let mut fuel = usize::MAX;
        let unsliced = subject.step(None, &mut fuel);
        let unsliced_fuel = usize::MAX - fuel;
        assert_eq!(unsliced, ScanOutcome::Complete(expected(0..6)));
        assert!(
            unsliced_fuel > error_keys,
            "the error walk must be charged for the keys it visits"
        );

        let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
        let mut spent = 0;
        let mut sliced = None;
        // Bounded so that a scan which restarts a phase on each resumption fails the test instead
        // of hanging it.
        for _ in 0..100 {
            let mut fuel = 2;
            let outcome = subject.step(None, &mut fuel);
            spent += 2 - fuel;
            match outcome {
                ScanOutcome::Suspended => continue,
                outcome => {
                    sliced = Some(outcome);
                    break;
                }
            }
        }
        let sliced = sliced.expect("scan did not finish within 100 resumptions");

        assert_eq!(sliced, unsliced);
        assert_eq!(
            spent, unsliced_fuel,
            "slicing the scan must not repeat or skip cursor positions"
        );
    }

    /// The row-iteration limit bounds the peek rather than either walk, so the ok walk continues
    /// the count the error walk accrued. A limit that the two together exceed fails the peek
    /// part-way through the ok trace.
    #[mz_ore::test]
    fn the_row_iteration_limit_spans_both_phases() {
        let keys = rows(0..6);
        let error_keys = 4;
        // Two rows past what the error walk examines.
        let limit = error_keys + 2;

        let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(Some(limit), &mut fuel),
            ScanOutcome::Failed(PeekError::RowIterationLimitExceeded { limit })
        );
        assert_eq!(
            subject.results,
            expected(0..2),
            "the ok walk must start from the count the error walk reached"
        );

        // The same scan under a limit that covers both phases returns every row.
        let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(Some(error_keys + keys.len()), &mut fuel),
            ScanOutcome::Complete(expected(0..6))
        );
    }

    /// A batch is offered only once the accumulated rows have crossed the stash threshold, and
    /// only to a peek that may use the stash. Rows the scan has not offered stay with it, so a
    /// driver that cannot write them is never the one holding them.
    #[mz_ore::test]
    fn take_batch_yields_only_past_the_stash_threshold() {
        let keys = rows(0..8);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.peek_stash_eligible = true;
        // Crossed by the fourth row, which leaves rows on either side of it.
        subject.peek_stash_threshold_bytes = 3 * row_size();

        let mut fuel = 2;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
        assert_eq!(subject.take_batch(), None, "the threshold is not crossed");

        let mut fuel = 2;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
        assert_eq!(subject.take_batch(), Some(expected(0..4)));
        assert_eq!(subject.total_size, 0);
        assert_eq!(subject.take_batch(), None, "the rows have been taken");

        // What the scan accumulates after a batch is taken is the next part of its answer, and it
        // stops on that batch too rather than walking the trace out.
        let mut fuel = usize::MAX;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
        assert_eq!(subject.take_batch(), Some(expected(4..8)));

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(RowBatch::new())
        );
    }

    /// A scan holding a full batch stops where it stands until the batch is taken. Stepping it
    /// again pulls no row and grows no prefix, which is what bounds what one scan retains for a
    /// driver that steps in a loop and takes batches on a schedule of its own.
    #[mz_ore::test]
    fn a_batch_ready_scan_does_not_grow_when_stepped_again() {
        let keys = rows(0..8);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.peek_stash_eligible = true;
        // Crossed by the third row, which leaves rows behind it in the trace.
        subject.peek_stash_threshold_bytes = 2 * row_size();

        let mut fuel = usize::MAX;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
        let retained = subject.results.clone();
        let total_size = subject.total_size;
        let rows_processed = subject.rows_processed();
        assert_eq!(retained, expected(0..3));

        // Four resumptions are enough to expose growth of a row per step, and the bound keeps a
        // scan that walks its trace out from ending this loop on its own.
        for _ in 0..4 {
            let mut fuel = usize::MAX;
            assert_eq!(
                subject.step(None, &mut fuel),
                ScanOutcome::Suspended,
                "a scan holding a batch has nothing to report but the batch"
            );
            assert_eq!(
                subject.results, retained,
                "stepping a batch-ready scan must not grow its prefix"
            );
            assert_eq!(subject.total_size, total_size);
            assert_eq!(
                subject.rows_processed(),
                rows_processed,
                "stepping a batch-ready scan must not advance its cursor"
            );
            assert_eq!(fuel, usize::MAX, "a scan holding a batch spends no fuel");
        }

        // Taking the batch is what lets the walk go on.
        assert_eq!(subject.take_batch(), Some(retained));
        let mut fuel = usize::MAX;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
        assert_eq!(subject.results, expected(3..6));
    }

    /// Both outcomes that end the ok walk latch, and neither of them leaves the cursor at the end
    /// of the trace, so a scan that resumed from one would answer the same peek with a different
    /// row set.
    #[mz_ore::test]
    fn the_terminal_outcomes_of_the_ok_walk_latch() {
        let keys = rows(0..10);

        // A finishing without an ordering is answered by the rows in hand, which leaves rows a
        // resumed walk would go on to produce.
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_results = Some(2);

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected(0..2))
        );
        let rows_processed = subject.rows_processed();

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(RowBatch::new()),
            "a completed scan must repeat its outcome rather than resume its walk"
        );
        assert_eq!(fuel, usize::MAX, "a latched outcome spends no fuel");
        assert_eq!(subject.rows_processed(), rows_processed);

        // The result-size ceiling fails the peek on the row that crosses it, which is the other
        // way the walk ends short of the trace's end.
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_result_size = 3 * row_size();

        let mut fuel = usize::MAX;
        let failure = subject.step(None, &mut fuel);
        assert!(
            matches!(failure, ScanOutcome::Failed(_)),
            "the ceiling must fail the peek: {failure:?}"
        );
        let rows_processed = subject.rows_processed();

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            failure,
            "a failed scan must repeat its outcome rather than resume its walk"
        );
        assert_eq!(fuel, usize::MAX, "a latched outcome spends no fuel");
        assert_eq!(subject.rows_processed(), rows_processed);
    }

    /// A peek that cannot use the stash is never offered a batch, however large its accumulation
    /// grows.
    #[mz_ore::test]
    fn take_batch_yields_nothing_without_the_stash() {
        let keys = rows(0..8);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.peek_stash_threshold_bytes = 0;

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected(0..8))
        );
        assert_eq!(subject.take_batch(), None);
        assert_eq!(
            subject.total_size, 0,
            "the size accounted to rows that have left the scan must not stay behind"
        );
    }

    /// A finishing that imposes no ordering is answered by any `max_results` rows, so thinning
    /// truncates and the scan stops rather than walking the rest of the trace.
    #[mz_ore::test]
    fn unordered_thinning_truncates_and_ends_the_scan() {
        let keys = rows(0..10);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_results = Some(2);

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected(0..2))
        );
        assert_eq!(
            subject.rows_processed(),
            4,
            "the scan must stop at the threshold rather than walk the trace out"
        );
        assert_eq!(
            subject.total_size, 0,
            "the size accounted to rows that have left the scan must not stay behind"
        );
    }

    /// A finishing that imposes an ordering keeps the rows that order ranks first, thinning down
    /// to `max_results` each time it holds twice that many.
    #[mz_ore::test]
    fn ordered_thinning_keeps_the_rows_the_ordering_ranks_first() {
        let keys = rows(0..10);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_results = Some(2);
        subject.comparator = Some(RowComparator::new(vec![ColumnOrder {
            column: 0,
            desc: true,
            nulls_last: true,
        }]));

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected([9, 8]))
        );
        assert_eq!(
            subject.rows_processed(),
            keys.len(),
            "an ordering can be decided only by walking the whole trace"
        );
    }

    /// Accumulation past the result-size ceiling fails the peek, and the row that crossed it is
    /// not part of what the scan holds.
    #[mz_ore::test]
    fn accumulation_past_the_result_size_ceiling_fails_the_peek() {
        let keys = rows(0..8);
        let max_result_size = 3 * row_size();
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_result_size = max_result_size;

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Failed(PeekError::unstructured(format!(
                "result exceeds max size of {}",
                ByteSize::b(u64::cast_from(max_result_size))
            )))
        );
        assert_eq!(subject.results, expected(0..3));
    }

    /// The result-size ceiling bounds an inline answer, which rows bound for the stash are not.
    /// A scan whose batches are taken therefore walks the whole trace with a ceiling below what it
    /// produces, rather than failing the peek.
    ///
    /// Driven with unbounded fuel, so every suspension is a full batch, and a scan that grew its
    /// prefix past the threshold instead of stopping would fail on the ceiling.
    #[mz_ore::test]
    fn a_prefix_bound_for_the_stash_is_not_bound_by_the_result_size_ceiling() {
        let keys = rows(0..8);
        let mut subject = scan(ErrorPhase::Clean, &keys);
        subject.max_result_size = 3 * row_size();
        subject.peek_stash_eligible = true;
        subject.peek_stash_threshold_bytes = 2 * row_size();

        let mut collected = RowBatch::new();
        let mut completed = false;
        // Bounded so that a regression which restarts the ok walk on every resumption fails here
        // rather than spinning.
        for _ in 0..RESUMPTION_BOUND {
            let mut fuel = usize::MAX;
            match subject.step(None, &mut fuel) {
                ScanOutcome::Suspended => {
                    collected.extend(subject.take_batch().expect("a full batch"));
                }
                ScanOutcome::Complete(rest) => {
                    collected.extend(rest);
                    completed = true;
                    break;
                }
                ScanOutcome::Failed(error) => panic!("scan failed: {error:?}"),
            }
        }
        assert!(
            completed,
            "scan did not answer within {RESUMPTION_BOUND} resumptions"
        );

        assert_eq!(collected, expected(0..8));
    }

    /// Opens a scan of `peek` over `bundle`, the way the peek path opens one: through
    /// [`PeekScan::new`], from the pair of handles [`TraceBundle::oks_errs_mut`] hands out.
    fn open(
        bundle: &mut TraceBundle,
        peek: &Peek,
        max_result_size: u64,
        peek_stash_eligible: bool,
        peek_stash_threshold_bytes: usize,
    ) -> PeekScan<OksHandle> {
        let (oks, errs) = bundle.oks_errs_mut();
        PeekScan::new(
            peek,
            errs,
            oks,
            max_result_size,
            peek_stash_eligible,
            peek_stash_threshold_bytes,
        )
    }

    /// Runs `subject` to an answer in slices of `fuel_per_step` units, taking every batch it
    /// offers, and returns that answer, the batches taken in order, and the fuel spent in total.
    ///
    /// A scan that has a batch to give away is emptied before it is stepped again, so the fuel
    /// this reports is comparable across runs that cross the stash threshold and runs that do
    /// not.
    fn run_sliced(
        subject: &mut PeekScan<OksHandle>,
        fuel_per_step: usize,
        row_iteration_limit: Option<usize>,
    ) -> (ScanOutcome, RowBatch, usize) {
        let mut taken = RowBatch::new();
        let mut spent = 0;
        for _ in 0..RESUMPTION_BOUND {
            let mut fuel = fuel_per_step;
            let outcome = subject.step(row_iteration_limit, &mut fuel);
            spent += fuel_per_step - fuel;
            match outcome {
                ScanOutcome::Suspended => {
                    if let Some(batch) = subject.take_batch() {
                        taken.extend(batch);
                    }
                }
                outcome => return (outcome, taken, spent),
            }
        }
        panic!("scan did not answer within {RESUMPTION_BOUND} resumptions");
    }

    /// Opening a scan opens both cursors and reads through neither: the ok walk has evaluated no
    /// cursor position, and the error walk has not yet reported the error trace clean.
    ///
    /// This is what makes the eager open of the ok cursor safe. A scan that read an ok row before
    /// its error walk finished could return rows for a peek that owes an error.
    ///
    /// The literal seek is deferred out of construction for the same reason, and that property is
    /// pinned by `peek_result_iterator::tests::literal_seek_is_fueled_and_resumable`, because a
    /// seek moves the cursor without evaluating a position and so is invisible here.
    #[mz_ore::test]
    fn opening_a_scan_advances_neither_cursor() {
        let keys = rows(0..6);
        let peek = index_peek(trivial_finishing(), None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(4));

        let subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);

        assert_eq!(
            subject.rows_processed(),
            0,
            "the ok cursor must not advance"
        );
        assert!(
            !subject.error_trace_clean(),
            "the error trace is clean only once the error walk says so",
        );
    }

    /// An error the error trace holds answers the peek whether the scan runs in one call or is
    /// sliced a cursor position at a time, at the same cost either way, and in neither case does
    /// the scan read a row of the ok trace.
    #[mz_ore::test]
    fn an_error_answers_a_sliced_scan_without_reading_an_ok_row() {
        let keys = rows(0..6);
        let cancelling_keys = 7;
        let (errors, error) = answering_errors(cancelling_keys);
        let peek = index_peek(trivial_finishing(), None);

        let mut bundle = trace_bundle(&keys, errors.clone());
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (unsliced, _, unsliced_fuel) = run_sliced(&mut subject, usize::MAX, None);
        assert_eq!(unsliced, ScanOutcome::Failed(error.clone()));
        assert_eq!(
            subject.rows_processed(),
            0,
            "a peek its error trace answers must not read an ok row",
        );
        assert_eq!(
            unsliced_fuel,
            cancelling_keys + 1,
            "the error walk is charged for every key it visits, the answering one included",
        );

        let mut bundle = trace_bundle(&keys, errors);
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (sliced, _, sliced_fuel) = run_sliced(&mut subject, 1, None);
        assert_eq!(sliced, ScanOutcome::Failed(error.clone()));
        assert_eq!(
            subject.rows_processed(),
            0,
            "slicing the scan must not let it reach the ok trace",
        );
        assert_eq!(
            sliced_fuel, unsliced_fuel,
            "slicing the scan must not repeat or skip an error key",
        );

        // A driver that keeps stepping a scan it has an answer for gets that answer back, not the
        // ok trace behind it.
        let mut fuel = usize::MAX;
        assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Failed(error));
        assert_eq!(fuel, usize::MAX, "a latched answer spends no fuel");
        assert_eq!(subject.rows_processed(), 0);
    }

    /// One budget covers both phases of a scan over a real index: slicing it across the
    /// error-to-ok boundary changes neither the answer nor the number of cursor positions the
    /// scan visits. Matching rows alone would not say that, because a scan that re-walks
    /// positions it has already visited still lands on the right rows.
    #[mz_ore::test]
    fn one_budget_spans_both_phases_of_a_scan_over_an_index() {
        let keys = rows(0..6);
        let error_keys = 4;
        let peek = index_peek(trivial_finishing(), None);

        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (unsliced, _, unsliced_fuel) = run_sliced(&mut subject, usize::MAX, None);
        assert_eq!(unsliced, ScanOutcome::Complete(expected(0..6)));
        // Every key of the error walk and every position of the ok walk comes out of the one
        // budget the caller supplied. A phase that spent a budget of its own would leave the
        // caller's short of this, while still answering the peek and still costing the same when
        // sliced. Each walk also charges for the position on which it discovers it is done, which
        // is why the total carries one unit per phase beyond the positions that produced work.
        assert_eq!(
            unsliced_fuel,
            error_keys + 1 + subject.rows_processed() + 1,
            "one budget must be charged for the positions of both walks",
        );

        // Two positions per call, so the scan suspends inside the error walk, at the phase
        // boundary, and inside the ok walk.
        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (sliced, _, sliced_fuel) = run_sliced(&mut subject, 2, None);
        assert_eq!(sliced, unsliced);
        assert_eq!(
            sliced_fuel, unsliced_fuel,
            "slicing the scan must not repeat or skip cursor positions",
        );
    }

    /// The row-iteration limit bounds the peek rather than either walk, so a limit that trips in
    /// the ok phase counts the error phase's positions too. It trips at the same position whether
    /// the scan ran in one call or was sliced.
    #[mz_ore::test]
    fn the_row_iteration_limit_trips_at_the_same_position_sliced_or_not() {
        let keys = rows(0..6);
        let error_keys = 4;
        // Two rows past what the error walk examines, so the limit trips inside the ok walk.
        let limit = error_keys + 2;
        let peek = index_peek(trivial_finishing(), None);
        let expected_outcome =
            || ScanOutcome::Failed(PeekError::RowIterationLimitExceeded { limit });

        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (unsliced, _, unsliced_fuel) = run_sliced(&mut subject, usize::MAX, Some(limit));
        assert_eq!(unsliced, expected_outcome());
        assert_eq!(
            subject.results,
            expected(0..2),
            "the ok walk must continue the count the error walk reached",
        );
        let unsliced_rows = subject.rows_processed();

        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (sliced, _, sliced_fuel) = run_sliced(&mut subject, 2, Some(limit));
        assert_eq!(sliced, expected_outcome());
        assert_eq!(
            subject.rows_processed(),
            unsliced_rows,
            "slicing the scan must not move the position the limit trips on",
        );
        assert_eq!(subject.results, expected(0..2));
        assert_eq!(sliced_fuel, unsliced_fuel);
    }

    /// A scan offers no batch before its accumulation crosses the stash threshold, offers one
    /// once it has, and the batches it gave away followed by the payload of its final
    /// [`ScanOutcome::Complete`] are the answer the same peek gets from a single unsliced walk,
    /// in the same order and at the same cost.
    #[mz_ore::test]
    fn taken_batches_and_the_final_payload_reassemble_the_unsliced_answer() {
        let keys = rows(0..8);
        let error_keys = 3;
        let peek = index_peek(trivial_finishing(), None);

        // The answer to reassemble towards: one walk, no stash, unbounded fuel.
        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
        let (whole, taken, whole_fuel) = run_sliced(&mut subject, usize::MAX, None);
        assert_eq!(whole, ScanOutcome::Complete(expected(0..8)));
        assert_eq!(
            taken,
            RowBatch::new(),
            "a peek that cannot use the stash is offered no batch",
        );

        // The same peek, stash-eligible and sliced two cursor positions at a time.
        let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
        let mut subject = open(&mut bundle, &peek, u64::MAX, true, 2 * row_size());
        assert_eq!(
            subject.take_batch(),
            None,
            "a scan that has walked nothing has nothing to offer",
        );

        let (rest, mut reassembled, sliced_fuel) = run_sliced(&mut subject, 2, None);
        let ScanOutcome::Complete(tail) = rest else {
            panic!("the sliced scan did not complete: {rest:?}");
        };
        assert!(
            !reassembled.is_empty(),
            "the threshold must be crossed at least once for this to test anything",
        );
        reassembled.extend(tail);
        assert_eq!(reassembled, expected(0..8));
        assert_eq!(
            sliced_fuel, whole_fuel,
            "taking batches must not repeat or skip cursor positions",
        );
    }

    /// The literal constraints a peek carries reach the ok walk, so a constrained peek is answered
    /// by a seek to its literals rather than by a scan of the whole index.
    #[mz_ore::test]
    fn a_scan_reads_only_the_literals_the_peek_names() {
        let keys = rows(0..8);
        let peek = index_peek(trivial_finishing(), Some(rows([2, 5])));
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected([2, 5])),
        );
        assert_eq!(
            subject.rows_processed(),
            2,
            "a constrained peek must evaluate only the positions its literals name",
        );
    }

    /// The finishing a peek carries reaches the scan, which stops once it holds every row the
    /// finishing can use and keeps the rows the finishing's ordering ranks first.
    #[mz_ore::test]
    fn a_scan_thins_towards_the_rows_the_peeks_finishing_needs() {
        let keys = rows(0..10);
        let finishing = RowSetFinishing {
            // Descending, so the rows the ordering ranks first are the last the walk reaches. A
            // scan that thinned without the ordering would keep the first two instead.
            order_by: vec![ColumnOrder {
                column: 0,
                desc: true,
                nulls_last: true,
            }],
            limit: Some(NonNeg::try_from(2).expect("non-negative")),
            offset: 0,
            project: vec![0],
        };
        let peek = index_peek(finishing, None);
        let mut bundle = trace_bundle(&keys, cancelling_errors(2));
        let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);

        let mut fuel = usize::MAX;
        assert_eq!(
            subject.step(None, &mut fuel),
            ScanOutcome::Complete(expected([9, 8])),
        );
    }
}
