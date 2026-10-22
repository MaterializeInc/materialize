// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_types::errors::DataflowErrorSer;
use mz_expr::EvalError;
use mz_repr::{Datum, Diff, Row, Timestamp, UpdateCollection};
use timely::progress::Antichain;

use super::{Cutover, CutoverDecision, trim_errors, trim_rows};

fn at(t: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(t))
}

fn accept(lower: u64, trim: bool) -> CutoverDecision {
    CutoverDecision::Accept {
        lower: at(lower),
        trim,
    }
}

#[mz_ore::test]
fn the_first_batch_is_accepted_at_the_start() {
    let mut cutover = Cutover::new(at(0));
    assert_eq!(cutover.process(&at(0), &at(5)), accept(0, false));
    assert_eq!(cutover.accepted(), &at(5));
}

#[mz_ore::test]
fn a_batch_at_or_below_u_is_dropped() {
    let mut cutover = Cutover::new(at(0));
    cutover.process(&at(0), &at(5));
    assert_eq!(cutover.process(&at(0), &at(5)), CutoverDecision::Drop);
    assert_eq!(cutover.process(&at(0), &at(3)), CutoverDecision::Drop);
    assert_eq!(cutover.accepted(), &at(5));
}

#[mz_ore::test]
fn a_straddling_batch_is_accepted_and_trimmed_at_u() {
    let mut cutover = Cutover::new(at(0));
    cutover.process(&at(0), &at(5));
    // The effective lower is U, not the batch's own lower. A strict `lower == U` rule would
    // reject this batch.
    assert_eq!(cutover.process(&at(3), &at(8)), accept(5, true));
}

#[mz_ore::test]
fn a_straddling_batch_moves_u_to_its_upper() {
    let mut cutover = Cutover::new(at(0));
    cutover.process(&at(0), &at(5));
    cutover.process(&at(3), &at(8));
    assert_eq!(cutover.accepted(), &at(8));
    assert_eq!(cutover.process(&at(8), &at(9)), accept(8, false));
}

#[mz_ore::test]
fn a_rerendered_replica_rejoins_after_the_replica_that_set_u_fails() {
    let mut cutover = Cutover::new(at(0));
    // Replica A sends [0,5) and [5,9). Replica B sends [0,7) and [7,12). The batches of B
    // interleave after those of A.
    assert_eq!(cutover.process(&at(0), &at(5)), accept(0, false));
    assert_eq!(cutover.process(&at(5), &at(9)), accept(5, false));
    assert_eq!(cutover.process(&at(0), &at(7)), CutoverDecision::Drop);
    // A fails. B continues and its batch straddles U = 9.
    assert_eq!(cutover.process(&at(7), &at(12)), accept(9, true));
    assert_eq!(cutover.accepted(), &at(12));
}

#[mz_ore::test]
fn a_batch_beyond_u_is_a_gap() {
    let mut cutover = Cutover::new(at(0));
    cutover.process(&at(0), &at(5));
    assert_eq!(cutover.process(&at(6), &at(9)), CutoverDecision::Gap);
    assert_eq!(cutover.accepted(), &at(5));
}

#[mz_ore::test]
fn the_completion_batch_empties_u() {
    let mut cutover = Cutover::new(at(0));
    cutover.process(&at(0), &at(5));
    assert_eq!(cutover.process(&at(5), &Antichain::new()), accept(5, false));
    assert!(cutover.accepted().is_empty());
    // A complete subscribe accepts nothing more, not even a repeated completion.
    assert_eq!(
        cutover.process(&at(5), &Antichain::new()),
        CutoverDecision::Drop
    );
    assert_eq!(cutover.process(&at(5), &at(9)), CutoverDecision::Drop);
}

#[mz_ore::test]
fn trim_rows_keeps_updates_at_or_after_the_trim() {
    let mut builder = UpdateCollection::builder(64, 4);
    for (id, time, diff) in [(0i64, 2u64, 1i64), (1, 5, -1), (2, 7, 2), (3, 4, 3)] {
        let row = Row::pack_slice(&[Datum::Int64(id)]);
        builder.push((row.as_ref(), &Timestamp::from(time), Diff::from(diff)));
    }
    let trimmed = trim_rows(builder.build(), &at(5));
    let kept: Vec<_> = trimmed
        .iter()
        .map(|(r, t, d)| (r.unpack_first().unwrap_int64(), u64::from(*t), d))
        .collect();
    assert_eq!(
        kept,
        vec![(1, 5, Diff::from(-1i64)), (2, 7, Diff::from(2i64))]
    );
}

fn error(name: &str) -> DataflowErrorSer {
    DataflowErrorSer::from(EvalError::Internal(name.into()))
}

#[mz_ore::test]
fn trim_errors_keeps_updates_at_or_after_the_trim() {
    let errors = vec![
        (error("a"), Timestamp::from(2u64), Diff::ONE),
        (error("b"), Timestamp::from(5u64), Diff::MINUS_ONE),
        (error("c"), Timestamp::from(7u64), Diff::ONE),
    ];
    assert_eq!(
        trim_errors(errors, &at(5)),
        vec![
            (error("b"), Timestamp::from(5u64), Diff::MINUS_ONE),
            (error("c"), Timestamp::from(7u64), Diff::ONE),
        ]
    );
}

/// The error updates of one replica's batch `[lower, upper)` of a subscribe whose consolidated
/// error collection is `errors`, rendered at `as_of`: those before `upper`, with the times of the
/// first batch's updates advanced to the as-of, consolidated per `(time, error)`.
fn batch_errors(
    errors: &[(&str, u64, i64)],
    as_of: u64,
    lower: u64,
    upper: u64,
) -> Vec<(DataflowErrorSer, Timestamp, Diff)> {
    let mut updates: Vec<_> = errors
        .iter()
        .filter(|(_, time, _)| *time < upper && (lower == as_of || lower <= *time))
        .map(|(name, time, diff)| {
            let time = std::cmp::max(*time, as_of);
            (error(name), Timestamp::from(time), Diff::from(*diff))
        })
        .collect();
    differential_dataflow::consolidation::consolidate_updates(&mut updates);
    updates
}

/// Runs the batches of replicas through a cutover starting at `start`, and returns the first
/// error the default policy fails the subscribe on: the least `(time, error)` of the first
/// accepted batch that holds an error update after its trim.
fn first_failure(
    start: u64,
    batches: &[(u64, u64, Vec<(DataflowErrorSer, Timestamp, Diff)>)],
) -> Option<(Timestamp, DataflowErrorSer)> {
    let mut cutover = Cutover::new(at(start));
    for (lower, upper, errors) in batches {
        let CutoverDecision::Accept { lower, .. } = cutover.process(&at(*lower), &at(*upper))
        else {
            continue;
        };
        let failure = trim_errors(errors.clone(), &lower)
            .into_iter()
            .map(|(error, time, _)| (time, error))
            .min();
        if failure.is_some() {
            return failure;
        }
    }
    None
}

/// An error inserted and retracted after the start fails the subscribe on its insertion, whether
/// both land in one description or are split across two, and whichever replica serves them.
#[mz_ore::test]
fn the_failure_does_not_depend_on_batch_boundaries() {
    // The subscribe has as-of 4 and no snapshot, so it starts at 5. The error "e" is inserted at
    // 6 and retracted at 8. The error "x" stands before the start and is never retracted.
    let errors = [("x", 2, 1), ("e", 6, 1), ("e", 8, -1)];
    let expected = Some((Timestamp::from(6u64), error("e")));

    let one = vec![(4, 10, batch_errors(&errors, 4, 4, 10))];
    assert_eq!(first_failure(5, &one), expected);

    let split = vec![
        (4, 7, batch_errors(&errors, 4, 4, 7)),
        (7, 10, batch_errors(&errors, 4, 7, 10)),
    ];
    assert_eq!(first_failure(5, &split), expected);

    // A replica re-rendered at as-of 3 sends a batch that straddles the start, after a batch of
    // another replica that holds no error.
    let failover = vec![
        (4, 5, batch_errors(&errors, 4, 4, 5)),
        (3, 10, batch_errors(&errors, 3, 3, 10)),
    ];
    assert_eq!(first_failure(5, &failover), expected);
}

/// Without a snapshot, an error that stands before the start is trimmed, and its retraction at
/// or after the start fails the subscribe, as a plain subscribe fails on any error update it
/// ships.
#[mz_ore::test]
fn a_retraction_after_the_start_fails_the_subscribe() {
    let errors = [("e", 2, 1), ("e", 8, -1)];
    let batches = vec![
        (4, 7, batch_errors(&errors, 4, 4, 7)),
        (7, 10, batch_errors(&errors, 4, 7, 10)),
    ];
    assert_eq!(
        first_failure(5, &batches),
        Some((Timestamp::from(8u64), error("e")))
    );

    // With the snapshot, the standing error fails the subscribe at the as-of.
    assert_eq!(
        first_failure(4, &batches),
        Some((Timestamp::from(4u64), error("e")))
    );
}
