// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests that the watchdog totals a dataflow's bytes across a replica exactly once.
//!
//! The interesting axis is the worker count. Of the fragment's inputs only the byte deltas are
//! partitioned: every worker logs the same `(operator, dataflow)` address records, because workers
//! build their dataflows in lockstep. A fragment that exchanged that mapping would consolidate it
//! to the worker count, and `join_core` multiplies input diffs, so every byte total would scale
//! with it. That is invisible at one worker and wrong at every other size, which is why these
//! tests assert on the totals at several worker counts rather than just the largest.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use mz_repr::{Diff, Timestamp};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::core::input::Handle;
use timely::dataflow::operators::generic::operator::empty;

use crate::logging::Update;
use crate::logging::watchdog::{Streams, construct};

/// The operator the bytes are attributed to.
const OPERATOR: usize = 1;
/// The dataflow that operator belongs to.
const DATAFLOW: usize = 7;

type Input<D> = Handle<Timestamp, CapacityContainerBuilder<Vec<Update<D>>>>;

/// Runs the watchdog on `workers` workers and returns the totals it recorded.
///
/// Each worker contributes its entry of `bytes_per_worker` for `OPERATOR`, so the dataflow's true
/// replica-wide size is their sum. The result merges every worker's map, which is safe because a
/// dataflow's total lives on exactly one of them.
fn run(workers: usize, bytes_per_worker: &'static [i64]) -> BTreeMap<usize, Diff> {
    assert_eq!(workers, bytes_per_worker.len());

    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let sizes: Rc<RefCell<BTreeMap<usize, Diff>>> = Rc::default();
        let index = worker.index();

        let handles = worker.dataflow(|scope| {
            let mut heap: Input<(usize, ())> = Handle::default();
            let mut operators: Input<(usize, usize)> = Handle::default();

            construct(Streams {
                arrangement_heap_size: heap.to_stream(scope),
                batcher_heap_size: empty(scope),
                operator_to_dataflow: operators.to_stream(scope),
                dataflow_heap_sizes: Rc::clone(&sizes),
            });

            (heap, operators)
        });

        let (mut heap, mut operators) = handles;
        let bytes = bytes_per_worker[index];
        heap.send(((OPERATOR, ()), Timestamp::MIN, Diff::from(bytes)));
        // Replicated on every worker, exactly as the timely address log is.
        operators.send(((OPERATOR, DATAFLOW), Timestamp::MIN, Diff::ONE));

        // Dropping the handles closes the inputs. `step` reports whether any dataflow remains, and
        // progress tracking keeps this one alive until every peer's data is accounted for, so
        // looping on it is the barrier that makes the exchanged totals deterministic.
        drop((heap, operators));
        while worker.step() {}

        let sizes = sizes.borrow().clone();
        sizes
    })
    .expect("timely execution failed");

    guards
        .join()
        .into_iter()
        .flat_map(|result| result.expect("worker panicked"))
        .collect()
}

#[mz_ore::test]
fn totals_bytes_across_workers() {
    // One worker is the case where the replicated mapping has multiplicity 1 anyway, so an
    // over-counting fragment still reads the right total here.
    assert_eq!(
        run(1, &[100]),
        BTreeMap::from([(DATAFLOW, Diff::from(100))])
    );
    // An over-counting fragment reads 800 here, and 6400 at four workers.
    assert_eq!(
        run(2, &[100, 100]),
        BTreeMap::from([(DATAFLOW, Diff::from(200))])
    );
    assert_eq!(
        run(4, &[100, 100, 100, 100]),
        BTreeMap::from([(DATAFLOW, Diff::from(400))])
    );
}

#[mz_ore::test]
fn totals_skewed_bytes() {
    // All the state on one worker, which is what a single-group aggregate produces.
    assert_eq!(
        run(4, &[400, 0, 0, 0]),
        BTreeMap::from([(DATAFLOW, Diff::from(400))])
    );
    // Spread unevenly, which is the ordinary case.
    assert_eq!(
        run(4, &[10, 20, 30, 40]),
        BTreeMap::from([(DATAFLOW, Diff::from(100))])
    );
}

#[mz_ore::test]
fn total_lands_on_exactly_one_worker() {
    let per_worker = timely::execute(timely::Config::process(4), |worker| {
        let sizes: Rc<RefCell<BTreeMap<usize, Diff>>> = Rc::default();

        let handles = worker.dataflow(|scope| {
            let mut heap: Input<(usize, ())> = Handle::default();
            let mut operators: Input<(usize, usize)> = Handle::default();

            construct(Streams {
                arrangement_heap_size: heap.to_stream(scope),
                batcher_heap_size: empty(scope),
                operator_to_dataflow: operators.to_stream(scope),
                dataflow_heap_sizes: Rc::clone(&sizes),
            });

            (heap, operators)
        });

        let (mut heap, mut operators) = handles;
        heap.send(((OPERATOR, ()), Timestamp::MIN, Diff::from(100)));
        operators.send(((OPERATOR, DATAFLOW), Timestamp::MIN, Diff::ONE));

        drop((heap, operators));
        while worker.step() {}

        let sizes = sizes.borrow().clone();
        sizes
    })
    .expect("timely execution failed")
    .join()
    .into_iter()
    .map(|result| result.expect("worker panicked"))
    .collect::<Vec<_>>();

    let holders: Vec<_> = per_worker.iter().filter(|m| !m.is_empty()).collect();
    assert_eq!(
        holders.len(),
        1,
        "expected one worker to own the dataflow, got {per_worker:?}"
    );
    assert_eq!(*holders[0], BTreeMap::from([(DATAFLOW, Diff::from(400))]));
}

#[mz_ore::test]
fn retracts_to_absent_when_the_dataflow_goes_away() {
    let merged = timely::execute(timely::Config::process(2), |worker| {
        let sizes: Rc<RefCell<BTreeMap<usize, Diff>>> = Rc::default();

        let handles = worker.dataflow(|scope| {
            let mut heap: Input<(usize, ())> = Handle::default();
            let mut operators: Input<(usize, usize)> = Handle::default();

            construct(Streams {
                arrangement_heap_size: heap.to_stream(scope),
                batcher_heap_size: empty(scope),
                operator_to_dataflow: operators.to_stream(scope),
                dataflow_heap_sizes: Rc::clone(&sizes),
            });

            (heap, operators)
        });

        let (mut heap, mut operators) = handles;
        heap.send(((OPERATOR, ()), Timestamp::MIN, Diff::from(100)));
        operators.send(((OPERATOR, DATAFLOW), Timestamp::MIN, Diff::ONE));

        // Tear the dataflow down: the arrangement retracts its bytes and the address log retracts
        // the operator. The entry must disappear rather than linger at zero, or a later dataflow
        // reusing the index would inherit a stale total.
        heap.advance_to(Timestamp::MIN.step_forward());
        operators.advance_to(Timestamp::MIN.step_forward());
        heap.send((
            (OPERATOR, ()),
            Timestamp::MIN.step_forward(),
            Diff::from(-100),
        ));
        operators.send((
            (OPERATOR, DATAFLOW),
            Timestamp::MIN.step_forward(),
            Diff::MINUS_ONE,
        ));

        drop((heap, operators));
        while worker.step() {}

        let sizes = sizes.borrow().clone();
        sizes
    })
    .expect("timely execution failed")
    .join()
    .into_iter()
    .flat_map(|result| result.expect("worker panicked"))
    .collect::<BTreeMap<_, _>>();

    assert_eq!(merged, BTreeMap::new());
}
