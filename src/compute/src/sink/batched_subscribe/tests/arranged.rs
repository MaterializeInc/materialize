// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the batched subscribe sink over an imported index, and of the connection's plan.

use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::Hashable;
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::{Arranged, ShutdownButton};
use differential_dataflow::trace::{BatchReader, TraceReader};
use mz_compute_client::protocol::response::{BatchedSubscribeResponse, Share};
use mz_expr::func::Gt;
use mz_expr::{
    BinaryFunc, MapFilterProject, MirScalarExpr, SafeMfpPlan, permutation_for_arrangement,
};
use mz_repr::{Datum, Diff, GlobalId, ReprScalarType, Row, RowRef, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_timely_util::columnation::ColumnationChunker;
use timely::dataflow::Stream;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{Enter, Leave, Probe};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::compute_state::peek_budget::InlineBudget;
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::{IndexErrBatch, IndexOkBatch};
use crate::typedefs::{
    ErrAgent, ErrBatcher, ErrBuilder, ErrEnter, ErrSpine, RowRowAgent, RowRowEnter,
};

use super::super::{
    Protocol, ResponseBuffer, WriterParts, arranged_plan, render_arranged, render_collection,
};
use super::{
    Setup, frontier, protocol, responses, run_two_workers, sink_minted_on_worker_zero, step_until,
    worker_metrics,
};

/// The arity of the indexed relation, whose rows are `(k, v)` bigint pairs.
const ARITY: usize = 2;

/// The indexed relation is arranged by its column `v`, so an arrangement's key is `[v]` and its
/// value is `[k]`.
fn index_key() -> Vec<MirScalarExpr> {
    vec![MirScalarExpr::column(1)]
}

/// The plan lowering builds to reconstruct rows `(k, v)` from an arrangement's key and value.
fn from_mfp() -> SafeMfpPlan {
    let (permutation, thinning) = permutation_for_arrangement(&index_key(), ARITY);
    let mut mfp = MapFilterProject::new(ARITY);
    mfp.permute_fn(|c| permutation[c], thinning.len() + index_key().len());
    mfp.into_plan()
        .expect("a permutation is a valid plan")
        .into_nontemporal()
        .expect("a permutation is not temporal")
}

fn int(value: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(value)])
}

fn ints(row: &RowRef) -> Vec<i64> {
    row.iter()
        .map(|datum| match datum {
            Datum::Int64(value) => value,
            other => panic!("bigint rows only, found {other:?}"),
        })
        .collect()
}

/// A description as a worker reported it, with its inline updates as `(columns, time, diff)`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Reported {
    lower: Option<u64>,
    upper: Option<u64>,
    updates: Vec<(Vec<i64>, u64, i64)>,
    error: bool,
}

fn option(frontier: &Antichain<Timestamp>) -> Option<u64> {
    frontier.as_option().copied().map(u64::from)
}

fn reported(responses: Vec<BatchedSubscribeResponse>) -> Vec<Reported> {
    responses
        .into_iter()
        .map(|response| match response {
            BatchedSubscribeResponse::Batch(batch) => {
                let mut updates = Vec::new();
                for share in &batch.shares {
                    let Share::Inline(collection) = share else {
                        panic!("a stashed share where every share should be inline");
                    };
                    for (row, time, diff) in collection.iter() {
                        updates.push((ints(row), u64::from(*time), diff.into_inner()));
                    }
                }
                updates.sort();
                Reported {
                    lower: option(&batch.lower),
                    upper: option(&batch.upper),
                    updates,
                    error: batch.error.is_some(),
                }
            }
            BatchedSubscribeResponse::DroppedAt { frontier, .. } => {
                panic!("the subscribe was dropped at {frontier:?}")
            }
        })
        .collect()
}

/// The updates of every description that `workers` reported, consolidated per `(row, time)`.
fn content(workers: &[Vec<Reported>]) -> Vec<(Vec<i64>, u64, i64)> {
    let mut updates: Vec<_> = workers
        .iter()
        .flatten()
        .flat_map(|reported| reported.updates.iter().cloned())
        .map(|(row, time, diff)| ((row, time), diff))
        .collect();
    consolidate(&mut updates);
    updates
        .into_iter()
        .map(|((row, time), diff)| (row, time, diff))
        .collect()
}

/// An index on one worker of two: a trace the sink imports, and the inputs that feed it.
struct Index {
    oks: InputSession<Timestamp, (Row, Row), Diff>,
    errs: InputSession<Timestamp, DataflowErrorSer, Diff>,
    probe: ProbeHandle<Timestamp>,
    ok_trace: RowRowAgent<Timestamp, Diff>,
    err_trace: ErrAgent<Timestamp, Diff>,
}

/// How a test sink reads the index.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// Over the index's batches, as `export_sink` renders a batched subscribe over an index.
    Arranged,
    /// Over the flattened rows of the index, as `export_sink` renders any other batched
    /// subscribe.
    General,
}

/// A sink rendered over the index, with what keeps its dataflow alive.
struct Subscribed {
    buffer: ResponseBuffer,
    protocol: Rc<RefCell<Option<Protocol>>>,
    _buttons: Vec<ShutdownButton<CapabilitySet<Timestamp>>>,
}

impl Subscribed {
    /// Steps `worker` until the sink has reported its completion, and returns its descriptions.
    fn await_completion(&self, worker: &mut Worker) -> Vec<Reported> {
        step_until(worker, || {
            self.buffer
                .borrow()
                .last()
                .is_some_and(|(_, response)| response.frontier().is_empty())
        });
        drop(self.protocol.borrow_mut().take());
        reported(responses(&self.buffer))
    }
}

impl Index {
    fn new(worker: &mut Worker) -> Self {
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, ok_collection) = scope.new_collection::<(Row, Row), Diff>();
            let (errs, err_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let ok_arranged = ok_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("index oks");
            let err_arranged = KeyCollection::from(err_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("index errs");
            let (probe, _) = ok_arranged.stream.probe();
            Index {
                oks,
                errs,
                probe,
                ok_trace: ok_arranged.trace,
                err_trace: err_arranged.trace,
            }
        })
    }

    /// Inserts the row `(k, v)` at `time` with `diff`, arranged by `v`.
    fn update(&mut self, k: i64, v: i64, time: u64, diff: i64) {
        self.oks
            .update_at((int(v), int(k)), Timestamp::from(time), Diff::from(diff));
    }

    /// Advances the inputs to `time` and steps `worker` until the index has caught up.
    fn advance(&mut self, worker: &mut Worker, time: u64) {
        let time = Timestamp::from(time);
        self.oks.advance_to(time);
        self.oks.flush();
        self.errs.advance_to(time);
        self.errs.flush();
        step_until(worker, || !self.probe.less_than(&time));
    }

    /// Allows the index to compact to `since`.
    fn compact(&mut self, since: u64) {
        let since = frontier(since);
        self.ok_trace.set_logical_compaction(since.borrow());
        self.ok_trace.set_physical_compaction(since.borrow());
        self.err_trace.set_logical_compaction(since.borrow());
        self.err_trace.set_physical_compaction(since.borrow());
    }

    /// Renders a batched subscribe over the index at `as_of`, reading it along `path`, and
    /// applying `mfp` to rows `(k, v)`.
    ///
    /// The index is imported into a region and its batches leave it for the sink's region, as
    /// `import_index` and `export_sink` place them.
    fn subscribe(
        &self,
        worker: &mut Worker,
        sink_id: GlobalId,
        path: Path,
        as_of: u64,
        up_to: u64,
        mfp: Option<SafeMfpPlan>,
    ) -> Subscribed {
        let (protocol, buffer) = protocol(sink_id, as_of);
        let setup = Setup::inline_only();
        let parts = WriterParts {
            start: Timestamp::MIN,
            budget: InlineBudget::new(&setup.config),
            tiers: setup.tiers(&worker_metrics()),
            protocol: Rc::clone(&protocol),
        };
        let as_of = frontier(as_of);
        let up_to = frontier(up_to);
        let mut ok_trace = self.ok_trace.clone();
        let mut err_trace = self.err_trace.clone();
        let buttons = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, ok_button) =
                ok_trace.import_frontier_core(scope, "index", as_of.clone(), Antichain::new());
            let (errs, err_button) =
                err_trace.import_frontier_core(scope, "err index", as_of.clone(), Antichain::new());
            let (oks, errs) = scope.clone().region_named("build", |build| {
                (
                    oks.enter(build).stream.leave(scope),
                    errs.enter(build).stream.leave(scope),
                )
            });
            scope.clone().region_named("sink", |inner| {
                let oks: Stream<_, Vec<IndexOkBatch>> = oks.enter(inner);
                let errs: Stream<_, Vec<IndexErrBatch>> = errs.enter(inner);
                match path {
                    Path::Arranged => render_arranged(
                        sink_id,
                        as_of.clone(),
                        up_to.clone(),
                        oks,
                        errs,
                        arranged_plan(from_mfp(), mfp),
                        parts,
                    ),
                    Path::General => {
                        // The flattening `export_sink` performs: `from_mfp` applied to the key
                        // and value of every update.
                        let rows =
                            Arranged::<RowRowEnter<Timestamp, Diff, Timestamp>>::flat_map_batches(
                                oks,
                                |key, val| {
                                    let (key, val) = (key.to_row(), val.to_row());
                                    let mut row = Row::default();
                                    row.packer().extend(val.iter().chain(key.iter()));
                                    Some(row)
                                },
                            );
                        let errs = Arranged::<ErrEnter<Timestamp, Timestamp>>::flat_map_batches(
                            errs,
                            |error: &DataflowErrorSer, &()| Some(error.clone()),
                        );
                        render_collection(
                            sink_id,
                            as_of.clone(),
                            up_to.clone(),
                            rows,
                            errs,
                            mfp,
                            ARITY,
                            parts,
                        );
                    }
                }
            });
            vec![ok_button, err_button]
        });
        Subscribed {
            buffer,
            protocol,
            _buttons: buttons,
        }
    }
}

/// The plan of a subscribe that keeps the rows with `k > 1` and outputs `(v, k)`.
fn filter_and_swap() -> SafeMfpPlan {
    let k_above_one = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        BinaryFunc::Gt(Gt),
    );
    MapFilterProject::new(ARITY)
        .filter([k_above_one])
        .project([1, 0])
        .into_plan()
        .expect("a valid plan")
        .into_nontemporal()
        .expect("not temporal")
}

/// Runs a batched subscribe at as-of 4 and up to 8 over an index on two workers, reading it
/// along `path` with the connection plan `mfp`, and returns each worker's descriptions.
///
/// The index holds updates before the as-of, which the import replays as its snapshot, and
/// receives more after the subscribe is rendered. Worker 0 writes every update, and the index
/// exchanges them by key.
fn subscribe_to_index(path: Path, mfp: Option<SafeMfpPlan>) -> Vec<Vec<Reported>> {
    let sink_id = sink_minted_on_worker_zero();
    run_two_workers(move |worker, _barrier| {
        let mut index = Index::new(worker);
        let writes = worker.index() == 0;
        if writes {
            index.update(1, 10, 1, 1);
            index.update(2, 20, 1, 1);
            index.update(3, 30, 2, 1);
            index.update(2, 20, 3, -1);
        }
        index.advance(worker, 5);
        let subscribed = index.subscribe(worker, sink_id, path, 4, 8, mfp.clone());
        if writes {
            index.update(4, 40, 5, 1);
            index.update(1, 10, 6, -1);
            index.update(5, 50, 9, 1);
        }
        index.advance(worker, 10);
        let reported = subscribed.await_completion(worker);
        assert_eq!(
            reported.last().map(|r| (r.lower, r.upper)),
            Some((Some(8), None)),
            "the chain completes at `up_to`",
        );
        reported
    })
}

/// The content of [`subscribe_to_index`] with no connection plan.
fn expected_content() -> Vec<(Vec<i64>, u64, i64)> {
    vec![
        (vec![1, 10], 4, 1),
        (vec![1, 10], 6, -1),
        (vec![3, 30], 4, 1),
        (vec![4, 40], 5, 1),
    ]
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_batched_subscribe_over_an_index_reads_its_arrangement() {
    let arranged = subscribe_to_index(Path::Arranged, None);
    let general = subscribe_to_index(Path::General, None);
    assert_eq!(
        content(&arranged),
        expected_content(),
        "the arranged path reports the index's rows, with the snapshot advanced to the as-of",
    );
    assert_eq!(content(&arranged), content(&general));
    assert!(
        arranged.iter().flatten().all(|r| !r.error),
        "no description is poisoned"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_filter_in_the_connection_mfp_applies() {
    let arranged = subscribe_to_index(Path::Arranged, Some(filter_and_swap()));
    let general = subscribe_to_index(Path::General, Some(filter_and_swap()));
    assert_eq!(
        content(&arranged),
        vec![(vec![30, 3], 4, 1), (vec![40, 4], 5, 1)],
        "the connection's plan runs on the rows `from_mfp` rebuilds",
    );
    assert_eq!(content(&arranged), content(&general));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn the_index_import_replays_its_snapshot_to_the_sink() {
    let sink_id = sink_minted_on_worker_zero();
    let reported = run_two_workers(move |worker, _barrier| {
        let mut index = Index::new(worker);
        let writes = worker.index() == 0;
        // Each step seals a batch of its own, and the steps after the compaction merge them
        // into batches whose since is the as-of.
        for k in 0..4 {
            let time = u64::try_from(k).expect("non-negative");
            if writes {
                index.update(k, 7, time, 1);
            }
            index.advance(worker, time + 1);
        }
        index.compact(4);
        for time in 4..12 {
            if writes && time == 4 {
                index.update(1, 7, 4, -1);
            }
            index.advance(worker, time + 1);
        }
        let mut compacted = false;
        index.ok_trace.map_batches(|batch| {
            compacted |= batch.description().since() == &frontier(4);
        });
        let subscribed = index.subscribe(worker, sink_id, Path::Arranged, 4, 14, None);
        index.advance(worker, 14);
        (compacted, subscribed.await_completion(worker))
    });

    assert!(
        reported.iter().any(|(compacted, _)| *compacted),
        "the index holds a batch compacted to the as-of, which the import replays",
    );
    let reported: Vec<_> = reported.into_iter().map(|(_, r)| r).collect();
    assert_eq!(
        content(&reported),
        vec![(vec![0, 7], 4, 1), (vec![2, 7], 4, 1), (vec![3, 7], 4, 1)],
        "the first description holds the snapshot at the as-of",
    );
    for worker in &reported {
        assert_eq!(
            worker.first().map(|r| r.lower),
            Some(Some(4)),
            "the chain starts at the as-of",
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn the_general_path_applies_the_connection_mfp_before_arranging() {
    // Two rows that project to one output row. Arranged by themselves they would land on
    // different workers. Arranging after the plan brings them to one worker, whose share holds
    // their sum.
    let pair = |k: i64| Row::pack_slice(&[Datum::Int64(k), Datum::Int64(10)]);
    let on_worker = |worker: u64| {
        (0..)
            .map(pair)
            .find(|row| row.hashed() % 2 == worker)
            .expect("some row hashes to each worker")
    };
    let rows = [on_worker(0), on_worker(1)];
    let sink_id = sink_minted_on_worker_zero();
    let reported = run_two_workers(move |worker, _barrier| {
        let row = rows[worker.index()].clone();
        let (protocol, buffer) = protocol(sink_id, 0);
        let setup = Setup::inline_only();
        let parts = WriterParts {
            start: Timestamp::MIN,
            budget: InlineBudget::new(&setup.config),
            tiers: setup.tiers(&worker_metrics()),
            protocol: Rc::clone(&protocol),
        };
        let project_v = MapFilterProject::new(ARITY)
            .project([1])
            .into_plan()
            .expect("a valid plan")
            .into_nontemporal()
            .expect("not temporal");
        let (mut oks, mut errs) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, rows) = scope.new_collection::<Row, Diff>();
            let (errs, err_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            render_collection(
                sink_id,
                frontier(0),
                frontier(3),
                rows,
                err_collection,
                Some(project_v),
                ARITY,
                parts,
            );
            (oks, errs)
        });
        oks.update_at(row, Timestamp::from(1), Diff::ONE);
        oks.advance_to(Timestamp::from(5));
        oks.flush();
        errs.advance_to(Timestamp::from(5));
        errs.flush();
        step_until(worker, || buffer.borrow().len() >= 2);
        drop(protocol.borrow_mut().take());
        reported(responses(&buffer))
    });

    let shares: Vec<_> = reported
        .iter()
        .map(|worker| worker[0].updates.clone())
        .collect();
    assert!(
        shares.contains(&vec![(vec![10], 1, 2)]) && shares.contains(&vec![]),
        "one worker holds the consolidated output row, found {shares:?}",
    );
}
