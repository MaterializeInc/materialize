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
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use differential_dataflow::Hashable;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchReader;
use mz_compute_client::protocol::response::{BatchedSubscribeResponse, Share};
use mz_expr::{EvalError, MapFilterProject, SafeMfpPlan};
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_timely_util::columnation::ColumnationChunker;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::core::Input as _;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{Concat, Inspect, Probe};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::compute_state::peek_budget::InlineBudget;
use crate::extensions::arrange::{KeyCollection, MzArrange, MzArrangeCore};
use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

use super::{Description, Minter, OkBatch, Protocol, ResponseBuffer, Writer, mint, write};

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

/// A response, reduced to what the tests assert on. Updates are `(value, time, diff)` triples of
/// one-column rows, sorted.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Reported {
    Batch {
        lower: Option<u64>,
        upper: Option<u64>,
        updates: Vec<(i64, u64, i64)>,
        error: bool,
    },
    DroppedAt(Option<u64>),
}

fn option(frontier: &Antichain<Timestamp>) -> Option<u64> {
    frontier.as_option().copied().map(u64::from)
}

fn reported(buffer: &ResponseBuffer) -> Vec<Reported> {
    buffer
        .borrow()
        .iter()
        .map(|(_, response)| match response {
            BatchedSubscribeResponse::Batch(batch) => {
                let mut updates = Vec::new();
                for share in &batch.shares {
                    let Share::Inline(collection) = share else {
                        panic!("the sink writes inline shares only");
                    };
                    for (row, time, diff) in collection.iter() {
                        let Datum::Int64(value) = row.unpack_first() else {
                            panic!("one-column bigint rows");
                        };
                        updates.push((value, u64::from(*time), diff.into_inner()));
                    }
                }
                updates.sort();
                Reported::Batch {
                    lower: option(&batch.lower),
                    upper: option(&batch.upper),
                    updates,
                    error: batch.error.is_some(),
                }
            }
            BatchedSubscribeResponse::DroppedAt { frontier, .. } => {
                Reported::DroppedAt(option(frontier))
            }
        })
        .collect()
}

fn batch(lower: u64, upper: Option<u64>, updates: &[(i64, u64, i64)], error: bool) -> Reported {
    Reported::Batch {
        lower: Some(lower),
        upper,
        updates: updates.to_vec(),
        error,
    }
}

fn identity() -> SafeMfpPlan {
    MapFilterProject::new(1)
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan")
}

/// A protocol at `as_of` reporting into a fresh buffer.
fn protocol(sink_id: GlobalId, as_of: u64) -> (Rc<RefCell<Option<Protocol>>>, ResponseBuffer) {
    let buffer = ResponseBuffer::default();
    let protocol = Rc::new(RefCell::new(Some(Protocol {
        sink_id,
        buffer: Some(Rc::clone(&buffer)),
        upper: frontier(as_of),
        poison: None,
    })));
    (protocol, buffer)
}

fn new_writer(sink_id: GlobalId, protocol: &Rc<RefCell<Option<Protocol>>>) -> Writer {
    Writer::new(
        sink_id,
        identity(),
        InlineBudget::new(&mz_dyncfgs::all_dyncfgs()),
        Rc::clone(protocol),
    )
}

/// Runs `writer` with the given input frontiers until it no longer asks to be reactivated.
fn work(writer: &mut Writer, ok: &Antichain<Timestamp>, err: &Antichain<Timestamp>) {
    while writer.work(ok.borrow(), err.borrow()) {}
}

/// Arranges one-column rows as the sink does, advancing the input to each step's time after
/// inserting its `(value, time)` updates, and returns the emitted batches.
fn arranged_batches(steps: Vec<(u64, Vec<(i64, u64)>)>) -> Vec<OkBatch> {
    timely::execute_directly(move |worker| {
        let batches: Rc<RefCell<Vec<OkBatch>>> = Default::default();
        let captured = Rc::clone(&batches);
        let (mut input, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let Arranged { stream, .. } = collection
                .mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test");
            let (probe, _) = stream
                .inspect(move |batch| captured.borrow_mut().push(batch.clone()))
                .probe();
            (input, probe)
        });
        for (advance_to, updates) in steps {
            for (value, time) in updates {
                input.update_at(
                    (row(value), Row::default()),
                    Timestamp::from(time),
                    Diff::ONE,
                );
            }
            let next = Timestamp::from(advance_to);
            input.advance_to(next);
            input.flush();
            worker.step_while(|| probe.less_than(&next));
        }
        batches.take()
    })
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_writer_waits_on_its_frontiers_and_keeps_a_straddling_batch() {
    let sink_id = GlobalId::User(1);
    let held = arranged_batches(vec![(2, vec![(1, 1)]), (7, vec![(3, 3), (6, 6)])]);
    let bounds: Vec<_> = held
        .iter()
        .map(|b| {
            (
                option(b.description().lower()),
                option(b.description().upper()),
            )
        })
        .collect();
    assert_eq!(bounds, vec![(Some(0), Some(2)), (Some(2), Some(7))]);

    let (protocol, buffer) = protocol(sink_id, 0);
    let mut writer = new_writer(sink_id, &protocol);
    writer.held_oks = held;
    writer
        .pending
        .push_back(description(0, Some(5), true, false));

    work(&mut writer, &frontier(4), &frontier(9));
    assert_eq!(
        reported(&buffer),
        vec![],
        "the ok frontier is short of the upper"
    );
    work(&mut writer, &frontier(9), &frontier(4));
    assert_eq!(
        reported(&buffer),
        vec![],
        "the error frontier is short of the upper"
    );

    work(&mut writer, &frontier(5), &frontier(5));
    assert_eq!(
        reported(&buffer),
        vec![batch(0, Some(5), &[(1, 1, 1), (3, 3, 1)], false)],
    );
    let kept: Vec<_> = writer
        .held_oks
        .iter()
        .map(|b| option(b.description().upper()))
        .collect();
    assert_eq!(
        kept,
        vec![Some(7)],
        "the covered batch goes, the straddling one stays"
    );

    writer
        .pending
        .push_back(description(5, Some(7), false, false));
    work(&mut writer, &frontier(7), &frontier(7));
    assert_eq!(reported(&buffer)[1], batch(5, Some(7), &[(6, 6, 1)], false));
    assert!(writer.held_oks.is_empty(), "every held batch is covered");

    writer.pending.push_back(description(7, None, false, true));
    work(&mut writer, &frontier(7), &frontier(7));
    assert_eq!(reported(&buffer)[2], batch(7, None, &[], false));

    drop(protocol.borrow_mut().take());
    assert_eq!(
        reported(&buffer).len(),
        3,
        "a completed subscribe reports no drop"
    );
}

#[mz_ore::test]
fn dropping_an_incomplete_sink_reports_the_last_upper() {
    let sink_id = GlobalId::User(1);
    let (protocol, buffer) = protocol(sink_id, 2);
    let mut writer = new_writer(sink_id, &protocol);
    writer
        .pending
        .push_back(description(2, Some(4), true, false));
    work(&mut writer, &frontier(4), &frontier(4));
    drop(protocol.borrow_mut().take());
    assert_eq!(
        reported(&buffer),
        vec![batch(2, Some(4), &[], false), Reported::DroppedAt(Some(4))],
    );
}

/// The sink operators rendered on one worker, with handles to drive them.
struct Rig {
    oks: InputSession<Timestamp, (Row, Row), Diff>,
    errs: InputSession<Timestamp, DataflowErrorSer, Diff>,
    /// Gates the ok batches the write operator reads, but not those the mint operator reads.
    gate: Option<timely::dataflow::InputHandle<Timestamp, CapacityContainerBuilder<Vec<OkBatch>>>>,
    /// Probes the broadcast descriptions. Its frontier passes the input frontier only once the mint
    /// holds no capability below it, that is once it has minted up to there or stopped.
    descriptions: ProbeHandle<Timestamp>,
    buffer: ResponseBuffer,
    protocol: Rc<RefCell<Option<Protocol>>>,
}

impl Rig {
    /// Renders the mint and write operators as the sink does, except that each worker arranges
    /// its own inputs, and that the write operator's ok batches pass a gate the test holds.
    ///
    /// Timely frontiers are global, so a worker cannot lag on an input it shares with the mint
    /// worker. The gate holds back the write operators' frontier while the mint operator reads
    /// the ungated one, which is the situation of a writer whose frontier lags the mint's.
    fn new(
        worker: &mut Worker,
        sink_id: GlobalId,
        as_of: u64,
        up_to: Antichain<Timestamp>,
    ) -> Self {
        let (protocol, buffer) = protocol(sink_id, as_of);
        let writer = new_writer(sink_id, &protocol);
        let (oks, errs, gate, descriptions) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, ok_collection) = scope.new_collection::<(Row, Row), Diff>();
            let (errs, err_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let (gate, gate_stream) = scope.new_input::<Vec<OkBatch>>();
            let Arranged {
                stream: ok_batches,
                ..
            } = ok_collection.mz_arrange_core::<
                _,
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >(Pipeline, "oks");
            let Arranged {
                stream: err_batches,
                ..
            } = KeyCollection::from(err_collection.clone()).mz_arrange_core::<
                _,
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >(Pipeline, "errs");
            let minted = mint(
                sink_id,
                frontier(as_of),
                up_to,
                ok_batches.clone(),
                err_collection,
            );
            let (descriptions, broadcast) = minted.broadcast().probe();
            write(
                sink_id,
                broadcast,
                ok_batches.concat(gate_stream),
                err_batches,
                writer,
            );
            (oks, errs, gate, descriptions)
        });
        Rig {
            oks,
            errs,
            gate: Some(gate),
            descriptions,
            buffer,
            protocol,
        }
    }

    /// Advances both inputs to `time`.
    fn advance(&mut self, time: u64) {
        self.oks.advance_to(Timestamp::from(time));
        self.oks.flush();
        self.errs.advance_to(Timestamp::from(time));
        self.errs.flush();
    }

    fn open_gate(&mut self) {
        self.gate = None;
    }
}

/// Steps `worker` until `done` holds, failing after a generous deadline.
fn step_until(worker: &mut Worker, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "the dataflow made no progress");
        worker.step();
    }
}

/// Steps `worker` a fixed number of times, so it handles whatever it has received.
fn settle(worker: &mut Worker) {
    for _ in 0..100 {
        worker.step();
    }
}

/// A sink id whose mint worker, among two, is worker 0.
fn sink_minted_on_worker_zero() -> GlobalId {
    (0..)
        .map(GlobalId::User)
        .find(|id| usize::cast_from(id.hashed()) % 2 == 0)
        .expect("some id hashes to worker 0")
}

/// Runs `logic` on two workers in one process and returns each worker's responses.
fn two_workers<F>(logic: F) -> Vec<Vec<Reported>>
where
    F: Fn(&mut Worker, &Barrier) -> ResponseBuffer + Send + Sync + 'static,
{
    let barrier = Arc::new(Barrier::new(2));
    let guards = timely::execute(timely::Config::process(2), move |worker| {
        let buffer = logic(worker, &barrier);
        reported(&buffer)
    })
    .expect("timely runs");
    guards
        .join()
        .into_iter()
        .map(|result| result.expect("worker succeeds"))
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn both_workers_answer_a_broadcast_description_once_their_frontiers_pass() {
    let sink_id = sink_minted_on_worker_zero();
    let responses = two_workers(move |worker, barrier| {
        let index = worker.index();
        let mut rig = Rig::new(worker, sink_id, 0, frontier(5));
        // Each worker contributes an update, so each holds its own share.
        let value = if index == 0 { 10 } else { 20 };
        rig.oks.update_at(
            (row(value), Row::default()),
            Timestamp::from(u64::cast_from(index) + 1),
            Diff::ONE,
        );
        rig.advance(7);
        if index == 0 {
            rig.open_gate();
        }

        // The mint cuts `[0, 5)` and the completion `[5, ∅)` from the ungated frontier, then
        // stops. Worker 1 holds the gate, so neither writer may answer yet.
        step_until(worker, || !rig.descriptions.less_than(&Timestamp::from(7)));
        settle(worker);
        assert_eq!(
            reported(&rig.buffer),
            vec![],
            "worker {index} answered early"
        );
        barrier.wait();

        if index == 1 {
            rig.open_gate();
        }
        step_until(worker, || rig.buffer.borrow().len() == 2);
        drop(rig.protocol.borrow_mut().take());
        Rc::clone(&rig.buffer)
    });

    assert_eq!(
        responses,
        vec![
            vec![
                batch(0, Some(5), &[(10, 1, 1)], false),
                batch(5, None, &[], false),
            ],
            vec![
                batch(0, Some(5), &[(20, 2, 1)], false),
                batch(5, None, &[], false),
            ],
        ],
        "each worker reports every description once, in order, with its own share",
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn an_error_on_another_worker_stops_the_mint() {
    let sink_id = sink_minted_on_worker_zero();
    let responses = two_workers(move |worker, barrier| {
        let index = worker.index();
        let mut rig = Rig::new(worker, sink_id, 0, Antichain::new());
        rig.open_gate();
        if index == 0 {
            rig.oks
                .update_at((row(1), Row::default()), Timestamp::from(1), Diff::ONE);
        }
        rig.advance(3);
        step_until(worker, || rig.buffer.borrow().len() == 1);
        barrier.wait();

        // Only worker 1 holds the error. The mint worker learns of it through the exchange.
        if index == 1 {
            rig.errs
                .update_at(division_by_zero(), Timestamp::from(4), Diff::ONE);
        }
        rig.advance(6);
        step_until(worker, || rig.buffer.borrow().len() == 2);
        barrier.wait();

        // A mint that kept going would mint `[6, 9)` here, which both writers could answer.
        rig.advance(9);
        step_until(worker, || !rig.descriptions.less_than(&Timestamp::from(9)));
        settle(worker);
        Rc::clone(&rig.buffer)
    });

    assert_eq!(
        responses,
        vec![
            vec![
                batch(0, Some(3), &[(1, 1, 1)], false),
                batch(3, Some(6), &[], false),
            ],
            vec![batch(0, Some(3), &[], false), batch(3, Some(6), &[], true),],
        ],
        "the worker holding the error poisons its share, and no description follows",
    );
}
