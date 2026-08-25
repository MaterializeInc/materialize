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

/// Rows a scan hands to its driver, in the order the scan produced them.
///
/// This is the item form [`PeekResultIterator`] yields and the form the peek stash carries, so the
/// path that moves large volumes never converts. The property that makes a batch usable, that it
/// is an in-order prefix of the rows the scan has left to give, follows from how the rows were
/// produced rather than from anything a wrapper type could enforce.
pub(super) type RowBatch = Vec<(Row, NonZeroI64)>;

/// The byte size of a row's count, as an answer built from a [`RowBatch`] stores it.
const COUNT_BYTE_SIZE: usize = size_of::<NonZeroUsize>();

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
    /// The two causes are not distinguishable without taking the rows: the only question a driver
    /// can ask is [`PeekScan::take_batch`], which empties the scan. Both drivers the scan is built
    /// for treat the causes alike, so no accessor separates them.
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
    /// stopped.
    pub(super) fn step(
        &mut self,
        row_iteration_limit: Option<usize>,
        fuel: &mut usize,
    ) -> ScanOutcome {
        if let Some(outcome) = self.step_error_phase(row_iteration_limit, fuel) {
            return outcome;
        }

        self.step_ok_phase(row_iteration_limit, fuel)
    }

    /// Takes the accumulated rows once they have crossed the stash threshold.
    ///
    /// Returns `None` while they have not, and for a peek that cannot use the stash at all, so a
    /// driver with nowhere to write rows is never handed any.
    pub(super) fn take_batch(&mut self) -> Option<RowBatch> {
        if !self.batch_ready() {
            return None;
        }
        // The rows leave the scan, and the ceiling bounds what the scan holds.
        self.total_size = 0;
        Some(mem::take(&mut self.results))
    }

    /// The number of cursor positions the ok walk has evaluated.
    pub(super) fn rows_processed(&self) -> usize {
        self.oks.rows_processed()
    }

    /// Worker time the error walk spent, summed over the slices it was cut into.
    pub(super) fn error_scan_time(&self) -> Duration {
        self.error_scan_time
    }

    /// Worker time spent opening the ok cursor.
    pub(super) fn cursor_setup_time(&self) -> Duration {
        self.cursor_setup_time
    }

    /// Worker time the ok walk spent, summed over the slices it was cut into. Includes the time
    /// thinning spent sorting.
    pub(super) fn row_iteration_time(&self) -> Duration {
        self.row_iteration_time
    }

    /// Worker time thinning spent sorting, summed over the times it ran.
    pub(super) fn result_sort_time(&self) -> Duration {
        self.result_sort_time
    }

    /// Rows handed to a sort, summed over the times thinning ran.
    pub(super) fn rows_sorted(&self) -> usize {
        self.rows_sorted
    }

    /// Whether the walk over the error trace has ended without finding an error, which is the only
    /// way the ok walk runs at all.
    ///
    /// False while that walk is under way, and false once it has answered the peek.
    pub(super) fn error_trace_clean(&self) -> bool {
        matches!(self.error_phase, ErrorPhase::Clean)
    }

    /// Whether the accumulated rows have grown past what this peek may answer with inline.
    fn batch_ready(&self) -> bool {
        self.peek_stash_eligible && self.total_size > self.peek_stash_threshold_bytes
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

        // Both terminal outcomes drop the walk, so that a peek pins error batches only while it
        // is reading them.
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
                Step::Done => break ScanOutcome::Complete(mem::take(&mut self.results)),
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
            return Some(ScanOutcome::Complete(mem::take(&mut self.results)));
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
    use mz_expr::ColumnOrder;
    use mz_repr::Datum;
    use mz_row_spine::{ArcOrdValBuilder, ArcOrdValSpine};
    use mz_timely_util::columnation::ColumnationStack;
    use timely::container::PushInto;
    use timely::progress::Antichain;

    use crate::render::errors::DataflowErrorSer;
    use crate::typedefs::{ErrBatcher, ErrBuilder};

    use super::*;

    type TestTrace = ArcOrdValSpine<Row, Row, Timestamp, Diff>;

    /// The time at which the peeks in these tests read.
    const PEEK_TIMESTAMP: Timestamp = Timestamp::new(1);

    fn row(value: u8) -> Row {
        Row::pack_slice(&[Datum::UInt8(value)])
    }

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
    ///
    /// The two updates of a key sit at different times so that they survive consolidation: a key
    /// whose updates consolidate away is not in the trace at all, and the walk never sees it.
    fn clean_error_scan(keys: usize) -> ErrorScan {
        let updates: Vec<((DataflowErrorSer, ()), Timestamp, Diff)> = (0..keys)
            .flat_map(|index| {
                let error = DataflowErrorSer::from(mz_expr::EvalError::Internal(
                    format!("error {index}").into(),
                ));
                [
                    ((error.clone(), ()), Timestamp::new(0), Diff::ONE),
                    ((error, ()), PEEK_TIMESTAMP, Diff::MINUS_ONE),
                ]
            })
            .collect();

        let mut batcher = ErrBatcher::<Timestamp, Diff>::new(None, 0);
        let mut chunk = ColumnationStack::with_capacity(updates.len());
        for update in updates {
            chunk.push_into(update);
        }
        batcher.push_into(chunk);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(Timestamp::MAX));
        let batch = ErrBuilder::<Timestamp, Diff>::seal(&mut chain, description);
        let storage = vec![batch];
        let cursor = CursorList::new(vec![storage[0].cursor()], &storage);
        ErrorScan::from_cursor(cursor, storage)
    }

    /// A scan over `keys` whose error phase is `error_phase`, bounded by nothing else.
    ///
    /// Mirrors what [`PeekScan::new`] builds, which cannot be used here because it takes traces
    /// rather than cursors over them.
    fn scan(error_phase: ErrorPhase, keys: &[Row]) -> PeekScan<TestTrace> {
        PeekScan {
            peek_timestamp: PEEK_TIMESTAMP,
            target_id: GlobalId::User(1),
            error_phase,
            oks: ok_iterator(keys),
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
        loop {
            let mut fuel = usize::MAX;
            match subject.step(None, &mut fuel) {
                ScanOutcome::Suspended => {
                    collected.extend(subject.take_batch().expect("a full batch"));
                }
                ScanOutcome::Complete(rest) => {
                    collected.extend(rest);
                    break;
                }
                ScanOutcome::Failed(error) => panic!("scan failed: {error:?}"),
            }
        }

        assert_eq!(collected, expected(0..8));
    }
}
