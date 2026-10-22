// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the fueled scan that combines a description's error and ok walks.

use std::collections::BTreeSet;

use differential_dataflow::trace::TraceReader;
use mz_expr::MapFilterProject;

use crate::compute_state::description_iterator::tests::{
    ArcOrdValBatch, batches, failing_on_zero, frontier, row, ts, update,
};
use crate::compute_state::error_scan::tests::{error, error_batch, holding};
use crate::typedefs::ErrSpine;

use super::*;

type ErrBatch = <ErrSpine<Timestamp, Diff> as TraceReader>::Batch;

/// A plan that passes the key through. The values are empty.
fn pass_through() -> SafeMfpPlan {
    MapFilterProject::new(1)
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan")
}

fn later(lower: u64, upper: u64) -> DescriptionRange {
    DescriptionRange::Later {
        lower: frontier(lower),
        upper: frontier(upper),
    }
}

fn description_scan(
    oks: Vec<ArcOrdValBatch>,
    errs: Vec<ErrBatch>,
    map_filter_project: SafeMfpPlan,
    range: DescriptionRange,
    cut_bytes: usize,
) -> DescriptionScan<ArcOrdValBatch, ErrBatch> {
    DescriptionScan::new(
        oks,
        errs,
        map_filter_project,
        range,
        cut_bytes,
        GlobalId::User(1),
    )
}

/// The bytes one update of `row` is accounted at against the cut.
fn update_bytes(row: &Row) -> usize {
    entry_byte_len(row) + size_of::<Timestamp>() + size_of::<Diff>()
}

#[mz_ore::test]
fn a_projection_that_collides_rows_is_consolidated() {
    // Two positions under one key, told apart only by their value column.
    let held = batches(
        &[
            ((row(1), row(10)), ts(2), Diff::ONE),
            ((row(1), row(11)), ts(2), Diff::ONE),
        ],
        &[ts(5)],
    );
    let key_only = MapFilterProject::new(2)
        .project([0])
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan");
    let mut scan = description_scan(held, Vec::new(), key_only, later(0, 5), usize::MAX);

    let mut fuel = usize::MAX;
    assert_eq!(
        scan.step(&mut fuel),
        DescriptionScanOutcome::Finished(Ok(()))
    );
    assert_eq!(
        scan.take_batch(),
        Some(vec![(row(1), ts(2), Diff::from(2))]),
        "the two positions project to one row and consolidate into one update"
    );
    assert_eq!(scan.take_batch(), None, "the tail is handed out once");
}

#[mz_ore::test]
fn a_poisoned_description_finishes_with_its_error() {
    let held = batches(&[update(1, 1, 1), update(2, 2, 1)], &[ts(5)]);
    let poison = error(0);
    let errs = vec![error_batch(holding(&poison))];
    let range = DescriptionRange::First {
        as_of: frontier(1),
        upper: frontier(5),
        start: ts(1),
    };
    let mut scan = description_scan(held, errs, pass_through(), range, usize::MAX);

    let mut fuel = usize::MAX;
    let expected = PeekError::from(poison.deserialize()).to_string();
    assert_eq!(
        scan.step(&mut fuel),
        DescriptionScanOutcome::Finished(Err(expected))
    );
    assert_eq!(
        scan.take_batch(),
        None,
        "a poisoned description hands out no ok updates"
    );
}

#[mz_ore::test]
fn a_first_description_after_the_as_of_ignores_snapshot_errors() {
    // Key 0 fails the plan. Its update and the standing error are placed at the as-of 1.
    let held = batches(&[update(0, 0, 1), update(2, 2, 1)], &[ts(5)]);
    let poison = error(0);
    let range = |start| DescriptionRange::First {
        as_of: frontier(1),
        upper: frontier(5),
        start: ts(start),
    };

    let errs = vec![error_batch(holding(&poison))];
    let mut without_snapshot =
        description_scan(held.clone(), errs, failing_on_zero(), range(2), usize::MAX);
    let mut fuel = usize::MAX;
    assert_eq!(
        without_snapshot.step(&mut fuel),
        DescriptionScanOutcome::Finished(Ok(()))
    );
    assert_eq!(
        without_snapshot.take_batch(),
        Some(vec![(row(2), ts(2), Diff::ONE)])
    );

    // Starting at the as-of, the error collection and then the plan poison the description.
    let errs = vec![error_batch(holding(&poison))];
    let mut with_snapshot =
        description_scan(held.clone(), errs, failing_on_zero(), range(1), usize::MAX);
    let mut fuel = usize::MAX;
    assert_eq!(
        with_snapshot.step(&mut fuel),
        DescriptionScanOutcome::Finished(Err(PeekError::from(poison.deserialize()).to_string()))
    );
    let mut plan_only = description_scan(held, Vec::new(), failing_on_zero(), range(1), usize::MAX);
    let mut fuel = usize::MAX;
    assert_eq!(
        plan_only.step(&mut fuel),
        DescriptionScanOutcome::Finished(Err(
            PeekError::from(mz_expr::EvalError::DivisionByZero).to_string()
        ))
    );
}

#[mz_ore::test]
fn the_scan_suspends_at_its_cut_and_resumes() {
    let keys: Vec<u8> = (1..8).collect();
    let updates: Vec<_> = keys.iter().map(|key| update(*key, 1, 1)).collect();
    let held = batches(&updates, &[ts(5)]);
    // Every row packs to the same size, so the buffer passes the cut at its third update.
    let sizes: BTreeSet<usize> = keys.iter().map(|key| update_bytes(&row(*key))).collect();
    assert_eq!(sizes.len(), 1, "the rows pack to one size");
    let cut_bytes = 2 * update_bytes(&row(1));
    let mut scan = description_scan(held, Vec::new(), pass_through(), later(0, 5), cut_bytes);

    let mut hand_outs = Vec::new();
    let outcome = loop {
        let mut fuel = usize::MAX;
        match scan.step(&mut fuel) {
            DescriptionScanOutcome::Suspended => {
                // A scan holding an untaken batch makes no progress.
                let mut idle_fuel = 10;
                assert_eq!(scan.step(&mut idle_fuel), DescriptionScanOutcome::Suspended);
                assert_eq!(idle_fuel, 10, "a blocked scan spends no fuel");

                let batch = scan.take_batch().expect("a suspension past the cut");
                hand_outs.push(batch);
            }
            finished @ DescriptionScanOutcome::Finished(_) => break finished,
        }
    };
    assert_eq!(outcome, DescriptionScanOutcome::Finished(Ok(())));
    hand_outs.extend(scan.take_batch());

    let sizes: Vec<usize> = hand_outs.iter().map(Vec::len).collect();
    assert_eq!(sizes, [3, 3, 1], "each cut hands out what passed it");
    let mut all: Vec<_> = hand_outs.into_iter().flatten().collect();
    all.sort();
    let expected: Vec<_> = keys
        .iter()
        .map(|key| (row(*key), ts(1), Diff::ONE))
        .collect();
    assert_eq!(all, expected, "resuming neither drops nor repeats updates");
}

/// Fuel exhaustion suspends without handing out a batch, and slicing does not change the result.
#[mz_ore::test]
fn the_scan_suspends_when_fuel_runs_out() {
    let updates: Vec<_> = (0..4).map(|key| update(key, 1, 1)).collect();
    let held = batches(&updates, &[ts(5)]);
    let mut scan = description_scan(held, Vec::new(), pass_through(), later(0, 5), usize::MAX);

    let mut suspensions = 0;
    loop {
        let mut fuel = 1;
        match scan.step(&mut fuel) {
            DescriptionScanOutcome::Suspended => {
                suspensions += 1;
                assert_eq!(scan.take_batch(), None, "the buffer is below its cut");
            }
            DescriptionScanOutcome::Finished(result) => {
                assert_eq!(result, Ok(()));
                break;
            }
        }
    }
    // One position per unit of fuel. The step that visits the last position also finds the
    // end, which costs nothing.
    assert_eq!(suspensions, 3);
    let batch = scan.take_batch().expect("the tail");
    assert_eq!(batch.len(), 4);
}
