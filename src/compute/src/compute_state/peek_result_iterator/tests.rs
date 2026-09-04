// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the fueled cursor walk that produces an index peek's rows.

use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::implementations::ord_neu::OrdValBatcher;
use differential_dataflow::trace::{Batcher, Builder, Navigable};
use mz_expr::{EvalError, MirScalarExpr};
use mz_repr::{Datum, ReprScalarType, Row, Timestamp};
use mz_row_spine::{ArcOrdValBuilder, ArcOrdValSpine};
use timely::container::PushInto;
use timely::progress::Antichain;

use super::*;

type TestTrace = ArcOrdValSpine<Row, Row, Timestamp, Diff>;

fn row(value: u8) -> Row {
    Row::pack_slice(&[Datum::UInt8(value)])
}

/// Builds a single-batch trace holding `keys`, and a cursor over it.
fn trace(keys: &[Row]) -> (TraceCursor<TestTrace>, TraceStorage<TestTrace>) {
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
    (cursor, storage)
}

/// A literal list whose entries are mostly absent from the trace costs one seek per absent
/// literal. That walk is fueled: it suspends when the fuel runs out and resumes at the
/// literal it stopped on, rather than running the whole list in one call.
#[mz_ore::test]
fn literal_seek_is_fueled_and_resumable() {
    // The trace holds the smallest row and the largest literal, so seeking to the match
    // costs one `seek_key` per literal, all but the last of them a miss.
    let mut rows: Vec<Row> = (0..=100).map(row).collect();
    rows.sort();
    let (absent, literals) = rows.split_first().expect("non-empty");
    let matching = literals.last().expect("non-empty").clone();
    let literal_count = literals.len();

    let (mut cursor, storage) = trace(&[absent.clone(), matching.clone()]);
    let mut constraints = literals.to_vec();
    let mut subject = Literals::<TestTrace>::new(&mut constraints);

    // Construction does not seek: the cursor still sits on the trace's first key, and the
    // literals have neither landed on a key nor run out.
    assert!(subject.seek_pending());
    assert!(!subject.is_exhausted());
    assert_eq!(subject.peek(), None);
    assert_eq!(cursor.get_key(&storage), Some(absent));

    // Five units of fuel buy five seeks and no more.
    let mut fuel = 5;
    assert!(matches!(
        subject.seek_next_literal_key(&mut cursor, &storage, &mut fuel),
        SeekOutcome::OutOfFuel
    ));
    assert_eq!(fuel, 0);
    assert!(subject.seek_pending());
    assert!(!subject.is_exhausted());
    assert_eq!(subject.peek(), None);

    // Resuming in five-unit slices lands on the matching literal after exactly as many
    // seeks as there are literals. A resume that restarted the walk, or one that skipped
    // the literal it suspended on, would not add up.
    let mut seeks = 5;
    let mut completed = false;
    // Bounded so that a regression which restarts the walk from literal 0 on each resume
    // fails the test instead of hanging it.
    for _ in 0..100 {
        let mut fuel = 5;
        let outcome = subject.seek_next_literal_key(&mut cursor, &storage, &mut fuel);
        seeks += 5 - fuel;
        if let SeekOutcome::Complete = outcome {
            completed = true;
            break;
        }
    }
    assert!(completed, "seek did not complete within 100 resumptions");
    assert_eq!(seeks, literal_count);
    assert_eq!(subject.peek(), Some(&matching));
    assert!(!subject.is_exhausted());

    // The same seek run with unbounded fuel costs the same, so slicing it neither lost nor
    // repeated work.
    let (mut cursor, storage) = trace(&[absent.clone(), matching.clone()]);
    let mut constraints = literals.to_vec();
    let mut subject = Literals::<TestTrace>::new(&mut constraints);
    let mut fuel = usize::MAX;
    assert!(matches!(
        subject.seek_next_literal_key(&mut cursor, &storage, &mut fuel),
        SeekOutcome::Complete
    ));
    assert_eq!(usize::MAX - fuel, literal_count);
    assert_eq!(subject.peek(), Some(&matching));

    // Past the last literal the seek reports exhaustion rather than suspending, even
    // though it spends no fuel doing so.
    let mut fuel = 0;
    assert!(matches!(
        subject.seek_next_literal_key(&mut cursor, &storage, &mut fuel),
        SeekOutcome::Complete
    ));
    assert!(subject.is_exhausted());
    assert_eq!(subject.peek(), None);
}

/// Builds an iterator over `keys`, constrained to `constraints`.
///
/// Mirrors [`PeekResultIterator::new`], which cannot be used here because it takes a trace
/// rather than a cursor over one.
fn iterator(keys: &[Row], constraints: &mut [Row]) -> PeekResultIterator<TestTrace> {
    let (cursor, storage) = trace(keys);
    // The cursor's key and the literal each contribute one datum. The values are empty.
    let map_filter_project = mz_expr::MapFilterProject::new(2)
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan");
    PeekResultIterator {
        target_id: GlobalId::User(1),
        cursor,
        storage,
        map_filter_project,
        peek_timestamp: Timestamp::MIN,
        row_builder: Row::default(),
        datum_vec: DatumVec::new(),
        literals: Some(Literals::new(constraints)),
        rows_processed: 0,
        row_iteration_tracker: PeekRowIterationTracker::new(None, 0),
        exhausted: false,
    }
}

/// Builds an iterator over `keys` with no literal constraints, filtered by `predicate`.
///
/// Mirrors [`iterator`], but omits the literal column so `predicate` can address the key at
/// column 0 directly.
fn iterator_without_literals(
    keys: &[Row],
    predicate: mz_expr::MirScalarExpr,
) -> PeekResultIterator<TestTrace> {
    let (cursor, storage) = trace(keys);
    // The cursor's key is the only datum. There are no literals and the values are empty.
    let map_filter_project = mz_expr::MapFilterProject::new(1)
        .filter([predicate])
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan");
    PeekResultIterator {
        target_id: GlobalId::User(1),
        cursor,
        storage,
        map_filter_project,
        peek_timestamp: Timestamp::MIN,
        row_builder: Row::default(),
        datum_vec: DatumVec::new(),
        literals: None,
        rows_processed: 0,
        row_iteration_tracker: PeekRowIterationTracker::new(None, 0),
        exhausted: false,
    }
}

/// Fuel must be spent walking the rows a `map_filter_project` rejects, not only the rows it
/// returns. Otherwise a highly selective filter over a large arrangement could walk the
/// entire arrangement inside a single fueled step, which is exactly the unbounded work the
/// budget exists to prevent.
#[mz_ore::test]
fn filtered_scan_charges_fuel_for_rejected_rows() {
    let keys: Vec<Row> = (0..10).map(row).collect();
    let accepted = 9u8;
    // Keeps only the last key. Every earlier key is visited and rejected.
    let predicate = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal(Ok(Datum::UInt8(accepted)), ReprScalarType::UInt8),
        mz_expr::func::Gte,
    );
    let mut iterator = iterator_without_literals(&keys, predicate);

    let mut rejected = 0;
    let mut found = None;
    // Bounded so that a step which spends fuel without ever surfacing the accepted row
    // fails the test instead of hanging it.
    for _ in 0..100 {
        let mut fuel = 1;
        match iterator.step(&mut fuel) {
            Step::OutOfFuel => {
                assert_eq!(
                    fuel, 0,
                    "a one-unit slice must spend its unit on the row it visited"
                );
                rejected += 1;
            }
            Step::Row(row) => {
                found = Some(row.expect("no error"));
                break;
            }
            Step::Done => panic!("scan exhausted before reaching the accepted row"),
        }
    }
    let found = found.expect("scan did not reach the accepted row within 100 steps");

    // A step that charged fuel only for rows it returns would reach the accepted row on the
    // very first call, leaving `rejected` at 0 instead of one per skipped key.
    assert_eq!(
        rejected, 9,
        "fuel must be spent on the 9 rejected rows before the accepted one"
    );
    let expected_row = Row::pack_slice(&[Datum::UInt8(accepted)]);
    assert_eq!(found, (expected_row, NonZeroI64::new(1).expect("non-zero")));
}

/// Resuming a filtered scan with fresh fuel after each `OutOfFuel` must continue from where
/// it stopped: it yields the same rows, in the same order, as an unbudgeted walk, and the
/// total fuel spent across every slice equals the fuel an unbudgeted walk spends. Matching
/// final rows alone would not catch a resumption that re-walks rows it already visited, or
/// one that drops a suspended position, since both can still land on the right answer while
/// doing the wrong amount of work.
#[mz_ore::test]
fn filtered_scan_is_fueled_and_resumable() {
    let keys: Vec<Row> = (0..30).map(row).collect();
    let threshold = 20u8;
    let predicate = || {
        MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal(Ok(Datum::UInt8(threshold)), ReprScalarType::UInt8),
            mz_expr::func::Gte,
        )
    };

    let mut unbudgeted = iterator_without_literals(&keys, predicate());
    let mut unbudgeted_rows = Vec::new();
    let mut unbudgeted_fuel = 0;
    let mut unbudgeted_completed = false;
    // Bounded for the same reason the budgeted walk below is. A regression that leaves the
    // cursor parked on a row it already returned never reaches `Done`, and an unbounded loop
    // would hang the suite rather than name the walk that failed to terminate.
    for _ in 0..100 {
        let mut fuel = usize::MAX;
        let step = unbudgeted.step(&mut fuel);
        unbudgeted_fuel += usize::MAX - fuel;
        match step {
            Step::Row(row) => unbudgeted_rows.push(row.expect("no error")),
            Step::Done => {
                unbudgeted_completed = true;
                break;
            }
            Step::OutOfFuel => unreachable!("fuel is unbounded"),
        }
    }
    assert!(
        unbudgeted_completed,
        "the unbudgeted walk did not reach the end of the cursor within 100 steps"
    );
    // Every one of the 30 rows costs one unit to visit, whether accepted or rejected. Finding
    // the cursor exhausted costs nothing, because the walk charges only for positions it goes
    // on to inspect. A charge that skipped rejected rows, or double-charged some subset of
    // rows, would move this total even though it does not depend on how the walk is sliced
    // into fueled steps.
    assert_eq!(unbudgeted_fuel, keys.len());

    let mut budgeted = iterator_without_literals(&keys, predicate());
    let mut budgeted_rows = Vec::new();
    let mut budgeted_fuel = 0;
    let mut completed = false;
    // Bounded so that a regression which restarts the walk on every resume fails the test
    // instead of hanging it.
    for _ in 0..100 {
        let mut fuel = 4;
        let step = budgeted.step(&mut fuel);
        budgeted_fuel += 4 - fuel;
        match step {
            Step::Row(row) => budgeted_rows.push(row.expect("no error")),
            Step::OutOfFuel => {}
            Step::Done => {
                completed = true;
                break;
            }
        }
    }
    assert!(completed, "scan did not finish within 100 resumptions");

    assert_eq!(
        budgeted_rows, unbudgeted_rows,
        "a fueled walk must return the same rows, in the same order, as an unbudgeted one"
    );
    assert_eq!(
        budgeted_fuel, unbudgeted_fuel,
        "slicing the walk into small fuel budgets must not repeat or skip cursor positions"
    );
}

/// A literal-constrained scan returns the rows of the literals the trace holds, whether it
/// is stepped with unbounded fuel or one unit at a time. The initial seek runs on the first
/// fueled step, so "no seek yet" has to stay distinct from "no literals left". Conflating
/// the two would empty out every literal-constrained peek.
#[mz_ore::test]
fn literal_constrained_scan_returns_matching_rows() {
    let keys: Vec<Row> = (0..=5).map(row).collect();
    // Two of the three literals are in the trace. The third is past its end.
    let constraints = vec![row(1), row(4), row(9)];
    let expected: Vec<(Row, NonZeroI64)> = [1, 4]
        .into_iter()
        .map(|value| {
            let row = Row::pack_slice(&[Datum::UInt8(value), Datum::UInt8(value)]);
            (row, NonZeroI64::new(1).expect("non-zero"))
        })
        .collect();

    // Taken one row at a time under a bound rather than collected. A regression that leaves
    // the cursor parked on a row it already returned yields rows forever, and collecting
    // would hang the suite rather than name the walk that failed to terminate.
    let mut unbounded_iterator = iterator(&keys, &mut constraints.clone());
    let mut unbounded = Vec::new();
    for _ in 0..100 {
        match unbounded_iterator.next() {
            Some(row) => unbounded.push(row.expect("no error")),
            None => break,
        }
    }
    assert_eq!(unbounded, expected);

    // The same walk stepped with unbounded fuel, to record what it costs unsliced.
    let mut unbudgeted_iterator = iterator(&keys, &mut constraints.clone());
    let mut unbudgeted_rows = Vec::new();
    let mut unbudgeted_fuel = 0;
    let mut done = false;
    for _ in 0..100 {
        let mut fuel = usize::MAX;
        let step = unbudgeted_iterator.step(&mut fuel);
        unbudgeted_fuel += usize::MAX - fuel;
        match step {
            Step::Row(row) => unbudgeted_rows.push(row.expect("no error")),
            Step::OutOfFuel => unreachable!("fuel is unbounded"),
            Step::Done => {
                done = true;
                break;
            }
        }
    }
    assert!(done, "the unbudgeted walk did not finish");
    assert_eq!(unbudgeted_rows, expected);

    // One unit of fuel per call: every literal seek suspends, so the scan only completes if
    // each resumption picks up where the last stopped.
    let mut iterator = iterator(&keys, &mut constraints.clone());
    let mut fueled = Vec::new();
    let mut fueled_fuel = 0;
    let mut done = false;
    // Bounded so that a step that spends fuel without advancing fails the test instead of
    // hanging it.
    for _ in 0..100 {
        let mut fuel = 1;
        let step = iterator.step(&mut fuel);
        fueled_fuel += 1 - fuel;
        match step {
            Step::Row(row) => fueled.push(row.expect("no error")),
            Step::OutOfFuel => continue,
            Step::Done => {
                done = true;
                break;
            }
        }
    }
    assert!(done, "scan did not finish");
    assert_eq!(fueled, expected);
    // A key transition costs one seek and one position. Charging the position before the seek
    // that has yet to reach it made a sliced walk pay for the position twice, which shows up
    // here and nowhere in the rows.
    assert_eq!(
        fueled_fuel, unbudgeted_fuel,
        "slicing a literal-constrained walk must not repeat or skip cursor positions"
    );
}

/// Bounds `iterator` by a row-iteration limit, the way [`PeekResultIterator::new`] does when the
/// limit is enabled.
fn with_row_iteration_limit(
    mut iterator: PeekResultIterator<TestTrace>,
    limit: usize,
) -> PeekResultIterator<TestTrace> {
    iterator.row_iteration_tracker = PeekRowIterationTracker::new(Some(limit), 0);
    iterator
}

/// An error is the peek's whole answer, so the walk ends on it. The single production consumer
/// sends the error on and does not step again, but nothing in the type stops a caller from
/// stepping, and the values past the error belong to no answer: resuming would return rows for a
/// peek that already failed, and repeating the error forever would never let the caller reach an
/// end.
#[mz_ore::test]
fn an_error_from_the_filter_ends_the_scan() {
    let keys: Vec<Row> = (0..10).map(row).collect();
    let predicate = MirScalarExpr::literal(
        Err(EvalError::Internal("filter failed".into())),
        ReprScalarType::Bool,
    );
    let mut iterator = iterator_without_literals(&keys, predicate);

    let mut fuel = usize::MAX;
    let Step::Row(Err(error)) = iterator.step(&mut fuel) else {
        panic!("the failing filter did not end the walk with its error");
    };
    assert_eq!(
        error,
        PeekError::from(EvalError::Internal("filter failed".into()))
    );
    // The walk stopped on the position that failed rather than stepping past it, so it spent
    // one unit and no more.
    assert_eq!(usize::MAX - fuel, 1);

    // The rows after the error are not part of any answer, and neither is the error a second
    // time.
    let mut fuel = usize::MAX;
    assert!(matches!(iterator.step(&mut fuel), Step::Done));
    assert_eq!(usize::MAX - fuel, 0, "a latched walk costs no fuel");
    assert_eq!(iterator.next(), None);
}

/// The row-iteration limit ends the walk the same way, and it is the path a peek actually takes
/// when an operator has bounded how much scanning one peek may cause.
#[mz_ore::test]
fn the_row_iteration_limit_ends_the_scan() {
    let keys: Vec<Row> = (0..10).map(row).collect();
    let predicate = MirScalarExpr::literal(Ok(Datum::True), ReprScalarType::Bool);
    let limit = 2;
    let mut iterator = with_row_iteration_limit(iterator_without_literals(&keys, predicate), limit);

    // Exactly `limit` rows come back. The walk fails on the row after them.
    let mut fuel = usize::MAX;
    for _ in 0..limit {
        let Step::Row(Ok(_)) = iterator.step(&mut fuel) else {
            panic!("the walk failed before it reached the limit");
        };
    }
    let Step::Row(Err(error)) = iterator.step(&mut fuel) else {
        panic!("the walk did not fail on the row past the limit");
    };
    assert_eq!(error, PeekError::RowIterationLimitExceeded { limit });

    let mut fuel = usize::MAX;
    assert!(matches!(iterator.step(&mut fuel), Step::Done));
    assert_eq!(usize::MAX - fuel, 0, "a latched walk costs no fuel");
}
