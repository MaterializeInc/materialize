// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the batched subscribe sink's operators.

use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchReader;
use mz_expr::EvalError;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_timely_util::columnation::ColumnationChunker;
use timely::dataflow::operators::{Inspect, Probe};
use timely::progress::Antichain;

use crate::extensions::arrange::MzArrange;
use crate::render::errors::DataflowErrorSer;

use super::{Description, Minter};

fn row(value: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(value)])
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn the_sink_arrangement_emits_batches_without_a_trace() {
    let seen = timely::execute_directly(move |worker| {
        // Each received batch as its `(lower, upper, len)`.
        let seen: Rc<RefCell<Vec<(Antichain<Timestamp>, Antichain<Timestamp>, usize)>>> =
            Default::default();
        let captured = Rc::clone(&seen);
        let (mut input, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let Arranged { stream, trace } = collection
                .mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test");
            drop(trace);
            let captured = Rc::clone(&captured);
            let (probe, _) = stream
                .inspect(move |batch| {
                    let description = batch.description();
                    captured.borrow_mut().push((
                        description.lower().clone(),
                        description.upper().clone(),
                        batch.len(),
                    ));
                })
                .probe();
            (input, probe)
        });

        // Each step is the updates at one time, then an advance past it.
        let steps: [&[(i64, Diff)]; 4] = [
            &[(1, Diff::ONE), (2, Diff::ONE)],
            &[(1, Diff::MINUS_ONE)],
            // No update at time 2, so no batch covers it.
            &[],
            &[(3, Diff::ONE)],
        ];
        for (time, updates) in steps.into_iter().enumerate() {
            let time = u64::cast_from(time);
            for (value, diff) in updates {
                input.update((row(*value), Row::default()), *diff);
            }
            let next = Timestamp::from(time + 1);
            input.advance_to(next);
            input.flush();
            worker.step_while(|| probe.less_than(&next));
        }
        seen.take()
    });

    let frontier = |time: u64| Antichain::from_elem(Timestamp::from(time));
    assert_eq!(
        seen,
        vec![
            (frontier(0), frontier(1), 2),
            (frontier(1), frontier(2), 1),
            (frontier(3), frontier(4), 1),
        ],
        "the arrange operator emits one batch per sealed interval holding updates, \
         even with no trace agent alive",
    );
}

fn frontier(time: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(time))
}

fn description(lower: u64, upper: Option<u64>, first: bool, completion: bool) -> Description {
    Description {
        lower: frontier(lower),
        upper: upper.map_or_else(Antichain::new, frontier),
        first,
        completion,
    }
}

fn division_by_zero() -> DataflowErrorSer {
    DataflowErrorSer::from(EvalError::DivisionByZero)
}

#[mz_ore::test]
fn the_first_description_starts_at_the_as_of() {
    let mut minter = Minter::new(frontier(2), Antichain::new());
    assert_eq!(
        minter.mint(frontier(2).borrow()),
        None,
        "no upper beyond the as-of"
    );
    assert_eq!(
        minter.mint(frontier(5).borrow()),
        Some(description(2, Some(5), true, false)),
    );
    assert_eq!(minter.mint(frontier(5).borrow()), None);
    assert_eq!(
        minter.mint(frontier(7).borrow()),
        Some(description(5, Some(7), false, false)),
    );
}

#[mz_ore::test]
fn up_to_clamps_the_upper_and_completes_the_chain() {
    let mut minter = Minter::new(frontier(0), frontier(3));
    assert_eq!(
        minter.mint(frontier(5).borrow()),
        Some(description(0, Some(3), true, false)),
    );
    assert_eq!(
        minter.mint(frontier(5).borrow()),
        Some(description(3, None, false, true)),
    );
    assert_eq!(minter.mint(Antichain::new().borrow()), None);
}

#[mz_ore::test]
fn up_to_at_the_as_of_completes_before_any_input() {
    let mut minter = Minter::new(frontier(3), frontier(2));
    assert_eq!(
        minter.mint(frontier(0).borrow()),
        Some(description(3, None, true, true)),
    );
    assert_eq!(minter.mint(frontier(9).borrow()), None);
}

#[mz_ore::test]
fn an_empty_input_frontier_ends_the_chain() {
    let mut minter = Minter::new(frontier(0), Antichain::new());
    assert_eq!(
        minter.mint(frontier(4).borrow()),
        Some(description(0, Some(4), true, false)),
    );
    assert_eq!(
        minter.mint(Antichain::new().borrow()),
        Some(description(4, None, false, false)),
    );
    assert_eq!(minter.mint(Antichain::new().borrow()), None);
}

#[mz_ore::test]
fn an_error_poisons_its_description_and_nothing_is_minted_after() {
    let mut minter = Minter::new(frontier(0), Antichain::new());
    minter
        .errors
        .push((division_by_zero(), Timestamp::from(4), Diff::ONE));
    assert_eq!(
        minter.mint(frontier(3).borrow()),
        Some(description(0, Some(3), true, false)),
    );
    assert!(!minter.done, "the error lies beyond the first description");
    assert_eq!(
        minter.mint(frontier(5).borrow()),
        Some(description(3, Some(5), false, false)),
    );
    assert!(minter.done);
    assert_eq!(minter.mint(frontier(9).borrow()), None);
}

#[mz_ore::test]
fn errors_that_cancel_within_a_description_poison_nothing() {
    let mut minter = Minter::new(frontier(0), Antichain::new());
    minter.errors.extend([
        (division_by_zero(), Timestamp::from(1), Diff::ONE),
        (division_by_zero(), Timestamp::from(2), Diff::MINUS_ONE),
    ]);
    assert_eq!(
        minter.mint(frontier(3).borrow()),
        Some(description(0, Some(3), true, false)),
    );
    assert!(!minter.done);
    assert!(
        minter.errors.is_empty(),
        "the description consumed its errors"
    );
}
