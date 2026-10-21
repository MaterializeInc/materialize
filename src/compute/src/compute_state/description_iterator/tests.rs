// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the fueled scan of held batches over one description.

use std::collections::BTreeSet;

use differential_dataflow::trace::implementations::ord_neu::OrdValBatcher;
use differential_dataflow::trace::{Batcher, Builder, TraceReader};
use mz_expr::{EvalError, MirScalarExpr};
use mz_repr::{Datum, ReprScalarType};
use mz_row_spine::{ArcOrdValBuilder, ArcOrdValSpine};
use timely::container::PushInto;

use super::*;

type ArcOrdValBatch = <ArcOrdValSpine<Row, Row, Timestamp, Diff> as TraceReader>::Batch;

fn row(value: u8) -> Row {
    Row::pack_slice(&[Datum::UInt8(value)])
}

fn ts(time: u64) -> Timestamp {
    Timestamp::from(time)
}

fn frontier(time: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(ts(time))
}

/// An update of key `key` with an empty value.
fn update(key: u8, time: u64, diff: i64) -> ((Row, Row), Timestamp, Diff) {
    ((row(key), Row::default()), ts(time), Diff::from(diff))
}

/// Seals `updates` into one batch per entry of `uppers`, in order, starting at
/// [`Timestamp::MIN`]. Each batch holds the updates between the previous upper and its own.
fn batches(updates: &[((Row, Row), Timestamp, Diff)], uppers: &[Timestamp]) -> Vec<ArcOrdValBatch> {
    let mut batcher = OrdValBatcher::<Row, Row, Timestamp, Diff>::new(None, 0);
    batcher.push_into(updates.to_vec());
    uppers
        .iter()
        .map(|upper| {
            let (mut chain, description) = batcher.seal(Antichain::from_elem(*upper));
            ArcOrdValBuilder::<Row, Row, Timestamp, Diff>::seal(&mut chain, description)
        })
        .collect()
}

/// A plan that passes the key through, filtered by `predicate` on the key at column 0.
fn plan(predicate: MirScalarExpr) -> SafeMfpPlan {
    // The cursor's key is the only datum. The values are empty.
    mz_expr::MapFilterProject::new(1)
        .filter([predicate])
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan")
}

fn pass_through() -> SafeMfpPlan {
    plan(MirScalarExpr::literal(
        Ok(Datum::True),
        ReprScalarType::Bool,
    ))
}

/// Steps `iterator` with `fuel_per_call` until it is done, returning the updates in emission
/// order, the total fuel spent, and how often a call ran out of fuel.
fn drain(
    iterator: &mut DescriptionIterator<ArcOrdValBatch>,
    fuel_per_call: usize,
) -> (Vec<(Row, Timestamp, Diff)>, usize, usize) {
    let mut updates = Vec::new();
    let mut spent = 0;
    let mut suspensions = 0;
    loop {
        let mut fuel = fuel_per_call;
        let step = iterator.step(&mut fuel);
        spent += fuel_per_call - fuel;
        match step {
            DescriptionStep::Updates(batch) => {
                assert!(
                    !batch.is_empty(),
                    "a step never yields an empty update list"
                );
                updates.extend(batch);
            }
            DescriptionStep::OutOfFuel => suspensions += 1,
            DescriptionStep::Done => return (updates, spent, suspensions),
            DescriptionStep::Error(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

fn scan(batches: Vec<ArcOrdValBatch>, range: DescriptionRange) -> Vec<(Row, Timestamp, Diff)> {
    let mut iterator = DescriptionIterator::new(batches, pass_through(), range);
    drain(&mut iterator, usize::MAX).0
}

fn as_set(updates: Vec<(Row, Timestamp, Diff)>) -> BTreeSet<(Row, Timestamp, Diff)> {
    let len = updates.len();
    let set: BTreeSet<_> = updates.into_iter().collect();
    assert_eq!(set.len(), len, "the scan emitted an update twice");
    set
}

#[mz_ore::test]
fn a_later_range_yields_updates_inside_the_description_with_their_times() {
    let held = batches(
        &[
            update(1, 1, 1),
            update(2, 4, 1),
            update(3, 5, 1),
            update(4, 7, 1),
        ],
        &[ts(3), ts(6), ts(9)],
    );
    let updates = scan(
        held,
        DescriptionRange::Later {
            lower: frontier(4),
            upper: frontier(7),
        },
    );
    assert_eq!(
        as_set(updates),
        BTreeSet::from([(row(2), ts(4), Diff::ONE), (row(3), ts(5), Diff::ONE),])
    );
}

#[mz_ore::test]
fn a_first_range_advances_earlier_times_to_the_as_of() {
    let held = batches(
        &[update(1, 1, 1), update(1, 2, 1), update(2, 4, 1)],
        &[ts(3), ts(6)],
    );
    let updates = scan(
        held,
        DescriptionRange::First {
            as_of: frontier(3),
            upper: frontier(5),
        },
    );
    assert_eq!(
        as_set(updates),
        BTreeSet::from([(row(1), ts(3), Diff::from(2)), (row(2), ts(4), Diff::ONE),])
    );
}

#[mz_ore::test]
fn retractions_survive_the_scan() {
    let held = batches(&[update(1, 4, 1), update(1, 5, -1)], &[ts(6)]);
    let updates = scan(
        held,
        DescriptionRange::Later {
            lower: frontier(4),
            upper: frontier(6),
        },
    );
    assert_eq!(
        as_set(updates),
        BTreeSet::from([(row(1), ts(4), Diff::ONE), (row(1), ts(5), Diff::MINUS_ONE),])
    );
}

#[mz_ore::test]
fn updates_that_cancel_at_one_time_are_not_emitted() {
    // One batcher would consolidate the pair away before the scan saw it, so each update is
    // sealed into a batch of its own. The two descriptions overlap, which a spine never
    // produces, but the scan consolidates across whatever batches it is handed.
    let mut held = batches(&[update(1, 4, 1)], &[ts(5)]);
    held.extend(batches(&[update(1, 4, -1)], &[ts(5)]));
    assert_eq!(held.len(), 2);
    let updates = scan(
        held,
        DescriptionRange::Later {
            lower: frontier(4),
            upper: frontier(5),
        },
    );
    assert_eq!(updates, Vec::new());
}

#[mz_ore::test]
fn the_scan_is_fueled_and_resumable() {
    // Keys at even values hold an update inside the range, keys at odd values only one
    // outside it, so single-unit slices also suspend on positions that yield nothing.
    let updates: Vec<_> = (0..20)
        .map(|key| {
            let time = if key % 2 == 0 { 5 } else { 1 };
            update(key, time, 1)
        })
        .collect();
    let range = || DescriptionRange::Later {
        lower: frontier(4),
        upper: frontier(8),
    };
    let held = batches(&updates, &[ts(3), ts(8)]);

    let mut unbounded = DescriptionIterator::new(held.clone(), pass_through(), range());
    let (expected, unbounded_spent, unbounded_suspensions) = drain(&mut unbounded, usize::MAX);
    assert_eq!(expected.len(), 10);
    assert_eq!(unbounded_suspensions, 0);

    let mut sliced = DescriptionIterator::new(held, pass_through(), range());
    let (actual, sliced_spent, sliced_suspensions) = drain(&mut sliced, 1);
    assert_eq!(actual, expected);
    assert!(sliced_suspensions > 0, "single-unit slices never suspended");
    assert_eq!(
        sliced_spent, unbounded_spent,
        "slicing the walk changed what it cost"
    );
    assert_eq!(unbounded_spent, 20, "one unit per cursor position");
}

#[mz_ore::test]
fn a_filter_error_ends_the_scan() {
    let held = batches(&[update(1, 4, 1), update(2, 4, 1)], &[ts(6)]);
    let predicate = MirScalarExpr::literal(
        Err(EvalError::Internal("filter failed".into())),
        ReprScalarType::Bool,
    );
    let mut iterator = DescriptionIterator::new(
        held,
        plan(predicate),
        DescriptionRange::Later {
            lower: frontier(4),
            upper: frontier(6),
        },
    );

    let mut fuel = usize::MAX;
    let DescriptionStep::Error(error) = iterator.step(&mut fuel) else {
        panic!("the failing filter did not end the scan with its error");
    };
    assert_eq!(
        error,
        PeekError::from(EvalError::Internal("filter failed".into()))
    );
    assert_eq!(usize::MAX - fuel, 1);

    let mut fuel = usize::MAX;
    assert_eq!(iterator.step(&mut fuel), DescriptionStep::Done);
    assert_eq!(usize::MAX - fuel, 0, "a latched scan costs no fuel");
}

#[mz_ore::test]
fn select_batches_keeps_the_batches_that_overlap_the_range() {
    let held = batches(&[], &[ts(3), ts(6), ts(9)]);
    let lowers = |selected: Vec<ArcOrdValBatch>| -> Vec<Antichain<Timestamp>> {
        selected
            .iter()
            .map(|batch| batch.description().lower().clone())
            .collect()
    };
    assert_eq!(
        lowers(select_batches(&held, &frontier(4), &frontier(7))),
        vec![frontier(3), frontier(6)]
    );
    assert_eq!(
        lowers(select_batches(&held, &frontier(3), &frontier(6))),
        vec![frontier(3)]
    );
    assert_eq!(
        lowers(select_batches(&held, &frontier(0), &frontier(9))),
        vec![frontier(0), frontier(3), frontier(6)]
    );
    assert_eq!(
        lowers(select_batches(&held, &frontier(9), &Antichain::new())),
        Vec::<Antichain<Timestamp>>::new()
    );
}
