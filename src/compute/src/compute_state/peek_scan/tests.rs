// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the budgeted scan that answers an index peek.

use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::implementations::ord_neu::OrdValBatcher;
use differential_dataflow::trace::{Batcher, Builder, Navigable};
use mz_expr::row::RowCollection;
use mz_expr::{ColumnOrder, RowSetFinishing};
use mz_ore::num::NonNeg;
use mz_repr::Datum;
use mz_row_spine::{ArcOrdValBuilder, ArcOrdValSpine};
use timely::container::PushInto;
use timely::progress::Antichain;

use crate::arrangement::manager::{PaddedTrace, TraceBundle};
use crate::compute_state::error_scan::ErrsHandle;
use crate::compute_state::error_scan::tests::PEEK_TIMESTAMP;
use crate::compute_state::index_peek_tests::{
    answering_errors, cancelling_errors, index_peek, ok_row as row, trace_bundle, trivial_finishing,
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
fn rows(values: impl IntoIterator<Item = u64>) -> Vec<Row> {
    values.into_iter().map(row).collect()
}

/// The size the scan accounts one row of these fixtures at, taken as the widest of them.
///
/// `Datum::UInt8` packs into the fewest bytes that hold its value, so `Datum::UInt8(0)` is a
/// byte narrower than every other value these fixtures use. Sizing by the widest row is what
/// keeps `count * row_size()` a bound that `count` rows stay under and `count + 1` cross, for
/// the first batch and for every batch the walk reaches after a prefix reset alike.
fn row_size() -> usize {
    entry_byte_len(&row(1))
}

/// The rows and counts a completed scan over `values` hands back.
fn expected(values: impl IntoIterator<Item = u64>) -> RowBatch {
    values
        .into_iter()
        .map(|value| (row(value), NonZeroI64::new(1).expect("non-zero")))
        .collect()
}

/// Asserts that a finished scan kept `values`, as a multiset.
///
/// Thinning partitions rather than sorts, so it does not order what it keeps. The answer's order
/// is established when the rows are collected.
fn assert_kept(outcome: ScanOutcome, values: impl IntoIterator<Item = u64>) {
    let ScanOutcome::Finished(Ok(kept)) = outcome else {
        panic!("the scan did not finish with rows: {outcome:?}");
    };
    let mut kept = kept;
    let mut want = expected(values);
    kept.sort();
    want.sort();
    assert_eq!(kept, want);
}

/// A walk over an ok trace holding `keys`, each once.
fn ok_iterator(keys: &[Row]) -> PeekResultIterator<TestTrace> {
    ok_iterator_with_copies(keys, Diff::ONE)
}

/// A walk over an ok trace holding `copies` of each of `keys`.
fn ok_iterator_with_copies(keys: &[Row], copies: Diff) -> PeekResultIterator<TestTrace> {
    let updates: Vec<((Row, Row), Timestamp, Diff)> = keys
        .iter()
        .map(|key| ((key.clone(), Row::default()), Timestamp::MIN, copies))
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
fn clean_error_scan(keys: usize) -> ErrorScan<ErrsHandle> {
    crate::compute_state::error_scan::tests::error_scan(cancelling_errors(keys), None)
}

/// A scan over `keys` whose error phase is `error_phase`, bounded by nothing else.
///
/// Mirrors what [`PeekScan::new`] builds. Tests use this rather than `new` to hold a second
/// cursor layout under test, and to start from an [`ErrorPhase`] that a fresh scan cannot be
/// in.
fn scan(error_phase: ErrorPhase<ErrsHandle>, keys: &[Row]) -> PeekScan<TestTrace, ErrsHandle> {
    PeekScan {
        peek_timestamp: PEEK_TIMESTAMP,
        target_id: GlobalId::User(1),
        error_phase,
        oks: ok_iterator(keys),
        ended: None,
        results: Vec::new(),
        total_size: 0,
        answer_rows: 0,
        max_result_size: usize::MAX,
        stash: StashBounds {
            eligible: false,
            threshold_bytes: usize::MAX,
            batch_bytes: 0,
        },
        stash_bound: false,
        max_results: None,
        comparator: None,
        error_scan_time: Duration::ZERO,
        cursor_setup_time: Duration::ZERO,
        row_iteration_time: Duration::ZERO,
        thinning_time: Duration::ZERO,
        rows_thinned: 0,
    }
}

/// An error the error trace holds answers the peek, and the ok trace stays unread, so a peek
/// that must report an error can never return rows instead.
#[mz_ore::test]
fn an_error_answers_the_peek_without_reading_the_ok_trace() {
    let (errors, error) = answering_errors(0);
    let mut subject = scan(
        ErrorPhase::Scanning(crate::compute_state::error_scan::tests::error_scan(
            errors, None,
        )),
        &rows(0..4),
    );

    let mut fuel = 100;
    assert_eq!(
        subject.step(None, &mut fuel),
        ScanOutcome::Finished(Err(error))
    );
    assert_eq!(subject.rows_processed(), 0);
}

/// A scan that has answered holds spent cursors, so stepping it again is a defect in the driver
/// rather than a way to read the answer twice.
#[mz_ore::test]
#[should_panic(expected = "index peek scan stepped after it ended")]
fn stepping_a_scan_that_has_answered_is_a_defect() {
    let mut subject = scan(ErrorPhase::Clean, &rows(0..4));

    let mut fuel = usize::MAX;
    let first = subject.step(None, &mut fuel);
    assert!(
        matches!(first, ScanOutcome::Finished(Ok(_))),
        "the scan must answer before the second step: {first:?}"
    );

    let mut fuel = usize::MAX;
    let _defect = subject.step(None, &mut fuel);
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
    assert_eq!(unsliced, ScanOutcome::Finished(Ok(expected(0..6))));
    assert!(
        unsliced_fuel > error_keys,
        "the error walk must be charged for the keys it visits"
    );

    let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
    let mut spent = 0;
    let mut sliced = None;
    // Bounded so that a scan which restarts a phase on each resumption fails the test instead
    // of hanging it.
    for _ in 0..RESUMPTION_BOUND {
        let mut fuel = 2;
        let outcome = subject.step(None, &mut fuel);
        spent += 2 - fuel;
        match outcome {
            ScanOutcome::Suspended => continue,
            outcome @ ScanOutcome::Finished(_) => {
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
        ScanOutcome::Finished(Err(PeekError::RowIterationLimitExceeded { limit }))
    );
    assert_eq!(
        subject.rows_processed(),
        2,
        "the ok walk must start from the count the error walk reached"
    );
    assert!(subject.results.is_empty(), "a failed scan keeps no rows");

    // The same scan under a limit that covers both phases returns every row.
    let mut subject = scan(ErrorPhase::Scanning(clean_error_scan(error_keys)), &keys);
    let mut fuel = usize::MAX;
    assert_eq!(
        subject.step(Some(error_keys + keys.len()), &mut fuel),
        ScanOutcome::Finished(Ok(expected(0..6)))
    );
}

/// A batch is offered only once the accumulated rows have crossed the stash threshold, and
/// only to a peek that may use the stash. Rows the scan has not offered stay with it, so a
/// driver that cannot write them is never the one holding them.
#[mz_ore::test]
fn take_batch_yields_only_past_the_stash_threshold() {
    let keys = rows(0..8);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.stash.eligible = true;
    // Crossed by the fourth row, which leaves rows on either side of it.
    subject.stash.threshold_bytes = 3 * row_size();

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
        ScanOutcome::Finished(Ok(RowBatch::new()))
    );
}

/// A scan holding a full batch stops where it stands until the batch is taken. Stepping it
/// again pulls no row and grows no prefix, which is what bounds what one scan retains for a
/// driver that steps in a loop and takes batches on a schedule of its own.
#[mz_ore::test]
fn a_batch_ready_scan_does_not_grow_when_stepped_again() {
    let keys = rows(0..8);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.stash.eligible = true;
    // Crossed by the third row, which leaves rows behind it in the trace.
    subject.stash.threshold_bytes = 2 * row_size();

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

/// Both outcomes that end the ok walk end the scan, and neither of them leaves the cursor at
/// the end of the trace, so a scan that resumed from one would answer the same peek with a
/// different row set.
#[mz_ore::test]
fn both_outcomes_of_the_ok_walk_end_the_scan() {
    let keys = rows(0..10);

    // A finishing without an ordering is answered by the rows in hand, which leaves rows a
    // resumed walk would go on to produce.
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.max_results = Some(2);

    let mut fuel = usize::MAX;
    assert_eq!(
        subject.step(None, &mut fuel),
        ScanOutcome::Finished(Ok(expected(0..2)))
    );
    assert!(subject.ended.is_some(), "the scan must be over");

    // The result-size ceiling fails the peek on the row that crosses it, which is the other
    // way the walk ends short of the trace's end.
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.max_result_size = 3 * row_size();

    let mut fuel = usize::MAX;
    let failure = subject.step(None, &mut fuel);
    assert!(
        matches!(failure, ScanOutcome::Finished(Err(_))),
        "the ceiling must fail the peek: {failure:?}"
    );
    assert_eq!(subject.ended, Some(failure));
}

/// A peek that cannot use the stash is never offered a batch, however large its accumulation
/// grows.
#[mz_ore::test]
fn take_batch_yields_nothing_without_the_stash() {
    let keys = rows(0..8);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    // Every row crosses a zero threshold, so eligibility is the only thing that can keep the
    // scan from offering a batch.
    assert!(!subject.stash.eligible);
    subject.stash.threshold_bytes = 0;

    let mut fuel = usize::MAX;
    assert_eq!(
        subject.step(None, &mut fuel),
        ScanOutcome::Finished(Ok(expected(0..8)))
    );
    assert_eq!(subject.take_batch(), None);
    assert_eq!(
        subject.total_size, 0,
        "the size accounted to rows that have left the scan must not stay behind"
    );
}

/// A finishing that imposes no ordering is answered by any `max_results` rows, so the scan ends
/// as soon as it holds that many rather than walking the rest of the trace.
#[mz_ore::test]
fn unordered_thinning_ends_the_scan_at_the_limit() {
    let keys = rows(0..10);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.max_results = Some(2);

    let mut fuel = usize::MAX;
    assert_eq!(
        subject.step(None, &mut fuel),
        ScanOutcome::Finished(Ok(expected(0..2)))
    );
    assert_eq!(
        subject.rows_processed(),
        2,
        "the scan must stop at the limit rather than walk the trace out"
    );
    assert_eq!(
        subject.total_size, 0,
        "the size accounted to rows that have left the scan must not stay behind"
    );
}

/// The finishing's limit counts a row as often as the answer holds it, so a trace of few rows at
/// a high multiplicity reaches the limit in fewer cursor positions than it has rows.
#[mz_ore::test]
fn the_limit_counts_copies_rather_than_distinct_rows() {
    let keys = rows(0..10);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.oks = ok_iterator_with_copies(&keys, Diff::from(4));
    subject.max_results = Some(6);

    let mut fuel = usize::MAX;
    let ScanOutcome::Finished(Ok(answer)) = subject.step(None, &mut fuel) else {
        panic!("a scan that reaches its limit finishes");
    };
    assert_eq!(
        answer,
        vec![
            (row(0), NonZeroI64::new(4).expect("non-zero")),
            (row(1), NonZeroI64::new(4).expect("non-zero")),
        ],
        "the row that crosses the limit is answered with whole"
    );
    assert_eq!(
        subject.rows_processed(),
        2,
        "eight copies of two rows is past a limit of six, so the walk stops there"
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
    assert_kept(subject.step(None, &mut fuel), [9, 8]);
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
        ScanOutcome::Finished(Err(PeekError::ResultExceedsMaxSize { max_result_size }))
    );
    // The rows it had accumulated are gone: they are the prefix of an answer that will never be
    // given, and holding them would leave `total_size` accounting for rows the scan no longer
    // has. The position the ceiling tripped on is where the count says it is: three rows fit,
    // and the fourth is the one that did not.
    assert!(subject.results.is_empty());
    assert_eq!(subject.take_batch(), None);
    assert_eq!(subject.rows_processed(), 4);
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
    subject.stash.eligible = true;
    subject.stash.threshold_bytes = 2 * row_size();

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
            ScanOutcome::Finished(Ok(rest)) => {
                collected.extend(rest);
                completed = true;
                break;
            }
            ScanOutcome::Finished(Err(error)) => panic!("scan failed: {error:?}"),
        }
    }
    assert!(
        completed,
        "scan did not answer within {RESUMPTION_BOUND} resumptions"
    );

    assert_eq!(collected, expected(0..8));
}

/// Once a batch has left, the ceiling on an inline answer no longer applies to what the scan
/// retains, which the batch size bounds instead and may put above the ceiling.
#[mz_ore::test]
fn a_batch_size_above_the_ceiling_does_not_fail_a_stash_bound_scan() {
    let keys = rows(0..8);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.max_result_size = 3 * row_size();
    subject.stash.eligible = true;
    subject.stash.threshold_bytes = 2 * row_size();
    subject.stash.batch_bytes = 5 * row_size();

    let mut fuel = usize::MAX;
    assert_eq!(subject.step(None, &mut fuel), ScanOutcome::Suspended);
    assert_eq!(subject.take_batch().expect("a full batch").len(), 3);

    // The five rows left fit under the batch size and over the ceiling, so they end the walk in
    // hand rather than failing it.
    let mut fuel = usize::MAX;
    assert_eq!(
        subject.step(None, &mut fuel),
        ScanOutcome::Finished(Ok(expected(3..8)))
    );
}

/// The first batch is cut at the stash threshold, which decides that the answer is not an inline
/// one, and every later batch at the batch size, so the placement decision and the size of a
/// hand-over are set apart.
#[mz_ore::test]
fn later_batches_are_cut_at_the_batch_size_rather_than_the_threshold() {
    let keys = rows(0..12);
    let mut subject = scan(ErrorPhase::Clean, &keys);
    subject.stash.eligible = true;
    subject.stash.threshold_bytes = 2 * row_size();
    subject.stash.batch_bytes = 4 * row_size();

    let mut batches = Vec::new();
    let mut rest = None;
    for _ in 0..RESUMPTION_BOUND {
        let mut fuel = usize::MAX;
        match subject.step(None, &mut fuel) {
            ScanOutcome::Suspended => batches.push(subject.take_batch().expect("a full batch")),
            ScanOutcome::Finished(Ok(rows)) => {
                rest = Some(rows);
                break;
            }
            ScanOutcome::Finished(Err(error)) => panic!("scan failed: {error:?}"),
        }
    }
    let rest = rest.expect("the scan must finish");

    // Three rows cross a two-row threshold, five cross a four-row batch size, and the four that
    // remain cross neither.
    assert_eq!(
        batches.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![3, 5],
        "the first batch is threshold-sized and the next batch-sized"
    );
    assert_eq!(rest.len(), 4);
    let collected: RowBatch = batches.into_iter().flatten().chain(rest).collect();
    assert_eq!(collected, expected(0..12));
}

/// Opens a scan of `peek` over `bundle`, the way the peek path opens one: through
/// [`PeekScan::new`], from the pair of handles [`TraceBundle::oks_errs_mut`] hands out.
fn open(
    bundle: &mut TraceBundle,
    peek: &Peek,
    max_result_size: u64,
    peek_stash_eligible: bool,
    peek_stash_threshold_bytes: usize,
) -> PeekScan<OksHandle, ErrsHandle> {
    let (oks, errs) = bundle.oks_errs_mut();
    PeekScan::new(
        peek,
        errs,
        oks,
        max_result_size,
        StashBounds {
            eligible: peek_stash_eligible,
            threshold_bytes: peek_stash_threshold_bytes,
            batch_bytes: 0,
        },
    )
}

/// Runs `subject` to an answer in slices of `fuel_per_step` units, taking every batch it
/// offers, and returns that answer, the batches taken in order, and the fuel spent in total.
///
/// A scan that has a batch to give away is emptied before it is stepped again, so the fuel
/// this reports is comparable across runs that cross the stash threshold and runs that do
/// not.
fn run_sliced(
    subject: &mut PeekScan<OksHandle, ErrsHandle>,
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
            outcome @ ScanOutcome::Finished(_) => return (outcome, taken, spent),
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
    assert_eq!(unsliced, ScanOutcome::Finished(Err(error.clone())));
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
    assert_eq!(sliced, ScanOutcome::Finished(Err(error.clone())));
    assert_eq!(
        subject.rows_processed(),
        0,
        "slicing the scan must not let it reach the ok trace",
    );
    assert_eq!(
        sliced_fuel, unsliced_fuel,
        "slicing the scan must not repeat or skip an error key",
    );

    assert_eq!(
        subject.ended,
        Some(ScanOutcome::Finished(Err(error))),
        "the error must end the scan rather than leave it resumable",
    );
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
    assert_eq!(unsliced, ScanOutcome::Finished(Ok(expected(0..6))));
    // Every key of the error walk and every position of the ok walk comes out of the one
    // budget the caller supplied, and nothing else does: neither phase charges for the
    // position on which it discovers it is done. A phase that spent a budget of its own would
    // leave the caller's short of this, while still answering the peek and still costing the
    // same when sliced.
    assert_eq!(
        unsliced_fuel,
        error_keys + subject.rows_processed(),
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
        || ScanOutcome::Finished(Err(PeekError::RowIterationLimitExceeded { limit }));

    let mut bundle = trace_bundle(&keys, cancelling_errors(error_keys));
    let mut subject = open(&mut bundle, &peek, u64::MAX, false, usize::MAX);
    let (unsliced, _, unsliced_fuel) = run_sliced(&mut subject, usize::MAX, Some(limit));
    assert_eq!(unsliced, expected_outcome());
    assert_eq!(
        subject.rows_processed(),
        2,
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
    assert!(subject.results.is_empty(), "a failed scan keeps no rows");
    assert_eq!(sliced_fuel, unsliced_fuel);
}

/// A scan offers no batch before its accumulation crosses the stash threshold, offers one
/// once it has, and the batches it gave away followed by the payload of its final
/// [`ScanOutcome::Finished`] are the answer the same peek gets from a single unsliced walk,
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
    assert_eq!(whole, ScanOutcome::Finished(Ok(expected(0..8))));
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
    let ScanOutcome::Finished(Ok(tail)) = rest else {
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
        ScanOutcome::Finished(Ok(expected([2, 5]))),
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
    assert_kept(subject.step(None, &mut fuel), [9, 8]);
}

/// `entry_byte_len` is the per-entry half of `RowCollection::byte_len`, which is what the
/// finishing and the controller measure a result against. Both fast-path peek routes charge with
/// it, so a drift here gives one route a different effective `max_result_size` than the other.
#[mz_ore::test]
fn the_entry_ruler_matches_what_an_answer_costs() {
    let rows = vec![
        Row::pack_slice(&[Datum::Int32(42)]),
        Row::pack_slice(&[Datum::String(&"a".repeat(100))]),
        Row::pack_slice(&[Datum::Int32(1), Datum::String("two")]),
    ];

    let charged: usize = rows.iter().map(entry_byte_len).sum();
    let collection = RowCollection::new(
        rows.into_iter()
            .map(|row| (row, NonZeroUsize::new(1).expect("non-zero")))
            .collect(),
        &[],
    );

    assert_eq!(charged, collection.byte_len());
}
