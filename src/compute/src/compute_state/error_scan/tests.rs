// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the fueled walk over a peek's error trace.

use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::{Batcher, Builder, Navigable};
use mz_expr::EvalError;
use mz_timely_util::columnation::ColumnationStack;
use timely::container::PushInto;
use timely::progress::Antichain;

use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{ErrBatcher, ErrBuilder};

use super::*;

/// The time at which the peeks in these tests read.
const PEEK_TIMESTAMP: Timestamp = Timestamp::new(1);

/// A distinct error for `index`.
///
/// The order of the serialized form does not follow `index`, so a test that cares where a
/// key falls in the walk sorts the errors and picks by position.
fn error(index: usize) -> DataflowErrorSer {
    DataflowErrorSer::from(EvalError::Internal(format!("error {index}").into()))
}

/// Builds a walk over a single-batch error trace holding `updates`, bounded by
/// `row_iteration_limit`.
fn error_scan(
    updates: Vec<((DataflowErrorSer, ()), Timestamp, Diff)>,
    row_iteration_limit: Option<usize>,
) -> ErrorScan {
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
    ErrorScan {
        cursor,
        storage,
        row_iteration_tracker: PeekRowIterationTracker::new(row_iteration_limit, 0),
        scan_time: Duration::ZERO,
    }
}

/// Updates that put `error` in the trace at a multiplicity that cancels to zero at
/// [`PEEK_TIMESTAMP`].
///
/// The two updates sit at different times so that they survive consolidation: a key whose
/// updates consolidate away is not in the trace at all, and the walk never sees it.
fn cancelling(error: &DataflowErrorSer) -> Vec<((DataflowErrorSer, ()), Timestamp, Diff)> {
    vec![
        ((error.clone(), ()), Timestamp::new(0), Diff::ONE),
        ((error.clone(), ()), PEEK_TIMESTAMP, Diff::MINUS_ONE),
    ]
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
    let expected = || ErrorScanStep::Finished(Err(answering.deserialize().into()));

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

    // Every key cancels, so the walk visits all of them. Learning that the trace is exhausted
    // costs nothing, because the walk charges only for keys it goes on to examine.
    let expected_fuel = errors.len();

    let mut scan = error_scan(updates.clone(), None);
    let mut fuel = usize::MAX;
    let unbudgeted = scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel);
    assert_eq!(unbudgeted, ErrorScanStep::Finished(Ok(errors.len())));
    assert_eq!(usize::MAX - fuel, expected_fuel);

    let mut scan = error_scan(updates, None);
    let (sliced, consumed, calls) = run_sliced(&mut scan, 3);
    assert_eq!(sliced, ErrorScanStep::Finished(Ok(errors.len())));
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
    let expected = || ErrorScanStep::Finished(Err(PeekError::RowIterationLimitExceeded { limit }));
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

/// The limit a walk is bounded by is the one in effect when it steps, not the one that was in
/// effect when it opened. [`ErrorScan::new`] opens without a limit and
/// [`ErrorScan::set_row_iteration_limit`] supplies the current one before each step, so a walk
/// that spans a configuration change adopts the new limit against the keys it has already
/// examined rather than starting its count over.
#[mz_ore::test]
fn a_limit_adopted_mid_walk_counts_the_keys_already_examined() {
    let errors: Vec<DataflowErrorSer> = (0..64).map(error).collect();
    let updates: Vec<_> = errors.iter().flat_map(cancelling).collect();

    // Opens the way `ErrorScan::new` does, with no limit, and walks part of the trace.
    let examined = 10;
    let mut scan = error_scan(updates.clone(), None);
    let mut fuel = examined;
    assert_eq!(
        scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel),
        ErrorScanStep::OutOfFuel
    );
    assert_eq!(fuel, 0);

    // A limit below the count already accrued trips on the very next key. A walk that
    // restarted its count on adopting the limit would keep going for `limit` keys more.
    let limit = examined / 2;
    scan.set_row_iteration_limit(Some(limit));
    let mut fuel = usize::MAX;
    assert_eq!(
        scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel),
        ErrorScanStep::Finished(Err(PeekError::RowIterationLimitExceeded { limit }))
    );
    assert_eq!(usize::MAX - fuel, 1);

    // The same in the other direction: a walk bounded by a limit it is about to exceed runs to
    // the end of the trace once the limit is withdrawn.
    let limit = 3;
    let mut scan = error_scan(updates, Some(limit));
    let mut fuel = limit;
    assert_eq!(
        scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel),
        ErrorScanStep::OutOfFuel
    );
    scan.set_row_iteration_limit(None);
    let mut fuel = usize::MAX;
    assert_eq!(
        scan.step(PEEK_TIMESTAMP, GlobalId::User(1), &mut fuel),
        ErrorScanStep::Finished(Ok(errors.len()))
    );
}
