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
use mz_compute_client::protocol::response::{
    BatchedSubscribeBatch, BatchedSubscribeResponse, Share, StashedShare,
};
use mz_compute_types::dyncfgs::{
    BATCHED_SUBSCRIBE_INLINE_MAX_BYTES, BATCHED_SUBSCRIBE_OFFLOAD_MIN_UPDATES,
    INDEX_PEEK_PERMIT_FRACTION, PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
    PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES,
};
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_expr::{EvalError, MapFilterProject, SafeMfpPlan};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::Schemas;
use mz_persist_client::batch::Batch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, SqlScalarType, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_storage_types::sources::SourceData;
use mz_timely_util::columnation::ColumnationChunker;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::core::Input as _;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{Concat, Inspect, Probe};
use timely::progress::Antichain;
use timely::worker::Worker;

use crate::compute_state::PeekPermits;
use crate::compute_state::peek_budget::InlineBudget;
use crate::compute_state::peek_stash::tests::CountedBlob;
use crate::extensions::arrange::{KeyCollection, MzArrange, MzArrangeCore};
use crate::metrics::{ComputeMetrics, WorkerMetrics};
use crate::render::errors::DataflowErrorSer;
use crate::server::ComputeRuntimeRole;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine};

use super::share::{StashSpec, Tiers};
use super::{
    Description, Minter, OkBatch, OwnedShare, Protocol, QueuedResponse, ResponseBuffer, Writer,
    mint, write,
};

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
        .map(|(_, response)| reduce(&inline_view(response), |_, _| unreachable!()))
        .collect()
}

/// The protocol response `queued` is sent as, for a response holding inline shares only.
fn inline_view(queued: &QueuedResponse) -> BatchedSubscribeResponse {
    match queued {
        QueuedResponse::Batch {
            lower,
            upper,
            shares,
            error,
        } => BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
            lower: lower.clone(),
            upper: upper.clone(),
            shares: shares
                .iter()
                .map(|share| match share {
                    OwnedShare::Inline(updates) => Share::Inline(updates.clone()),
                    OwnedShare::Stashed(_) => {
                        panic!("a stashed share where every share should be inline")
                    }
                })
                .collect(),
            error: error.clone(),
        }),
        QueuedResponse::DroppedAt { frontier } => BatchedSubscribeResponse::DroppedAt {
            frontier: frontier.clone(),
            orphans: Vec::new(),
        },
    }
}

/// Reduces `response`, reading the updates of a stashed share, given the lower of its
/// description, with `read_stashed`.
fn reduce(
    response: &BatchedSubscribeResponse,
    mut read_stashed: impl FnMut(&StashedShare, &Antichain<Timestamp>) -> Vec<(i64, u64, i64)>,
) -> Reported {
    match response {
        BatchedSubscribeResponse::Batch(batch) => {
            let mut updates = Vec::new();
            for share in &batch.shares {
                match share {
                    Share::Inline(collection) => {
                        for (row, time, diff) in collection.iter() {
                            updates.push((value(row), u64::from(*time), diff.into_inner()));
                        }
                    }
                    Share::Stashed(stashed) => updates.extend(read_stashed(stashed, &batch.lower)),
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
    }
}

fn value(row: &mz_repr::RowRef) -> i64 {
    let Datum::Int64(value) = row.unpack_first() else {
        panic!("one-column bigint rows");
    };
    value
}

/// The tier of each share of each batch in `responses`.
fn tiers(responses: &[BatchedSubscribeResponse]) -> Vec<Vec<&'static str>> {
    responses
        .iter()
        .filter_map(|response| match response {
            BatchedSubscribeResponse::Batch(batch) => Some(
                batch
                    .shares
                    .iter()
                    .map(|share| match share {
                        Share::Inline(_) => "inline",
                        Share::Stashed(_) => "stashed",
                    })
                    .collect(),
            ),
            BatchedSubscribeResponse::DroppedAt { .. } => None,
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

/// The single bigint column the test rows carry, which stashed shares are written under.
fn key_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("value", SqlScalarType::Int64.nullable(false))
        .finish()
}

/// What a test writer's tiers read, and where its shares are stashed.
#[derive(Clone)]
struct Setup {
    config: ConfigSet,
    /// The persist clients the stash is reached through. `None` keeps every share inline.
    stash: Option<Arc<PersistClientCache>>,
    shard_id: ShardId,
    permits: Arc<PeekPermits>,
    cut_bytes: usize,
}

impl Setup {
    /// The production configuration, with no stash.
    fn inline_only() -> Self {
        Self {
            config: mz_dyncfgs::all_dyncfgs(),
            stash: None,
            shard_id: ShardId::new(),
            permits: Arc::new(PeekPermits::new(1)),
            cut_bytes: super::SCAN_CUT_BYTES,
        }
    }

    /// A configuration stashing through `clients`, with the two thresholds given.
    fn stashing(
        clients: &Arc<PersistClientCache>,
        inline_max_bytes: usize,
        offload_min_updates: usize,
    ) -> Self {
        let setup = Self {
            stash: Some(Arc::clone(clients)),
            ..Self::inline_only()
        };
        setup.with(|updates| {
            updates.add(&BATCHED_SUBSCRIBE_INLINE_MAX_BYTES, inline_max_bytes);
            updates.add(&BATCHED_SUBSCRIBE_OFFLOAD_MIN_UPDATES, offload_min_updates);
            // Lands on the floor of one permit, so a test holding it holds every offloaded scan.
            updates.add(&INDEX_PEEK_PERMIT_FRACTION, 0.0001);
        })
    }

    fn with(self, configure: impl FnOnce(&mut ConfigUpdates)) -> Self {
        let mut updates = ConfigUpdates::default();
        configure(&mut updates);
        updates.apply(&self.config);
        self
    }

    fn tiers(&self, metrics: &WorkerMetrics) -> Tiers {
        let stash = self.stash.as_ref().map(|clients| {
            StashSpec::new(
                Arc::clone(clients),
                PersistLocation::new_in_mem(),
                self.shard_id,
                key_desc(),
            )
        });
        Tiers::new(&self.config, stash, Arc::clone(&self.permits), metrics)
    }
}

fn worker_metrics() -> WorkerMetrics {
    ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo).for_worker(0)
}

fn new_writer(sink_id: GlobalId, protocol: &Rc<RefCell<Option<Protocol>>>) -> Writer {
    new_writer_with(sink_id, protocol, &Setup::inline_only(), &worker_metrics())
}

fn new_writer_with(
    sink_id: GlobalId,
    protocol: &Rc<RefCell<Option<Protocol>>>,
    setup: &Setup,
    metrics: &WorkerMetrics,
) -> Writer {
    let mut writer = Writer::new(
        sink_id,
        identity(),
        InlineBudget::new(&setup.config),
        setup.tiers(metrics),
        Rc::clone(protocol),
    );
    writer.cut_bytes = setup.cut_bytes;
    writer
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
    metrics: WorkerMetrics,
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
        Self::with_setup(worker, sink_id, as_of, up_to, &Setup::inline_only())
    }

    /// The same, with the writer's tiers taken from `setup`.
    fn with_setup(
        worker: &mut Worker,
        sink_id: GlobalId,
        as_of: u64,
        up_to: Antichain<Timestamp>,
        setup: &Setup,
    ) -> Self {
        let (protocol, buffer) = protocol(sink_id, as_of);
        let metrics = worker_metrics();
        let writer = new_writer_with(sink_id, &protocol, setup, &metrics);
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
            metrics,
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
#[track_caller]
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
    run_two_workers(move |worker, barrier| reported(&logic(worker, barrier)))
}

/// Runs `logic` on two workers in one process and returns what each worker returns.
fn run_two_workers<F, R>(logic: F) -> Vec<R>
where
    F: Fn(&mut Worker, &Barrier) -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    let barrier = Arc::new(Barrier::new(2));
    let guards = timely::execute(timely::Config::process(2), move |worker| {
        logic(worker, &barrier)
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

/// A runtime for the tasks shares leave the worker to, which each worker enters.
fn runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("the runtime builds"),
    )
}

/// Takes the responses in `buffer` as the protocol sends them.
fn responses(buffer: &ResponseBuffer) -> Vec<BatchedSubscribeResponse> {
    buffer
        .borrow_mut()
        .drain(..)
        .map(|(_, response)| response.into_response())
        .collect()
}

/// Reads the updates of `share` at times from `lower` on, and deletes its batches.
async fn read_stashed(
    clients: &PersistClientCache,
    share: &StashedShare,
    lower: &Antichain<Timestamp>,
) -> Vec<(i64, u64, i64)> {
    let mut client = clients
        .open(PersistLocation::new_in_mem())
        .await
        .expect("the in-memory location opens");
    let batches: Vec<Batch<SourceData, (), Timestamp, i64>> = share
        .batches
        .iter()
        .map(|batch| client.batch_from_transmittable_batch(&share.shard_id, batch.clone()))
        .collect();
    let read_schemas: Schemas<SourceData, ()> = Schemas {
        id: None,
        key: Arc::new(key_desc()),
        val: Arc::new(UnitSchema),
    };
    let mut cursor = client
        .read_batches_consolidated_from::<_, _, _, i64>(
            share.shard_id,
            lower.clone(),
            read_schemas,
            batches,
            |_stats| true,
            *PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES.default(),
        )
        .await
        .expect("range reads never fail on since");
    let mut updates = Vec::new();
    while let Some(chunk) = cursor.next().await {
        for ((key, _val), time, diff) in chunk {
            let row = key.0.expect("no errors");
            updates.push((value(&row), u64::from(time), diff));
        }
    }
    for batch in cursor.into_lease() {
        batch.delete().await;
    }
    updates
}

/// Reduces each worker's `responses`, reading their stashed shares through `clients`.
fn reduce_all(
    runtime: &tokio::runtime::Runtime,
    clients: &PersistClientCache,
    responses: &[Vec<BatchedSubscribeResponse>],
) -> Vec<Vec<Reported>> {
    responses
        .iter()
        .map(|worker| {
            worker
                .iter()
                .map(|response| {
                    reduce(response, |share, lower| {
                        runtime.block_on(read_stashed(clients, share, lower))
                    })
                })
                .collect()
        })
        .collect()
}

/// Runs a subscribe at as-of 2 and up to 7 on two workers whose writers are set up with
/// `setup`, and returns each worker's responses with the stashed shares they wrote.
///
/// Each worker contributes updates of its own to the descriptions `[2, 5)` and `[5, 7)`. With
/// `hold_permit`, worker 0 holds the only offload permit until both workers have seen the first
/// description without answering it, which an offloaded scan cannot.
fn subscribe_across_two_workers(
    runtime: &Arc<tokio::runtime::Runtime>,
    setup: Setup,
    hold_permit: bool,
) -> Vec<(Vec<BatchedSubscribeResponse>, u64)> {
    let sink_id = sink_minted_on_worker_zero();
    let runtime = Arc::clone(runtime);
    run_two_workers(move |worker, barrier| {
        let _runtime = runtime.enter();
        let index = worker.index();
        let mut rig = Rig::with_setup(worker, sink_id, 2, frontier(7), &setup);
        rig.open_gate();
        let permit = (hold_permit && index == 0).then(|| {
            setup
                .permits
                .try_acquire()
                .expect("no scan holds the permit yet")
        });
        barrier.wait();

        let own = i64::try_from(index).expect("two workers");
        // Before the as-of, so the first description advances it to the as-of.
        let first = [(10 + own, 1, Diff::ONE), (20 + own, 3, Diff::ONE)];
        // At the lower of the second description, so the input frontier moves from there to 8
        // in one step, and the mint cuts one description up to 7.
        let second = [(10 + own, 5, Diff::MINUS_ONE), (30 + own, 5, Diff::ONE)];
        for (value, time, diff) in first {
            rig.oks
                .update_at((row(value), Row::default()), Timestamp::from(time), diff);
        }
        rig.advance(5);
        step_until(worker, || !rig.descriptions.less_than(&Timestamp::from(5)));
        if hold_permit {
            settle(worker);
            assert_eq!(
                rig.buffer.borrow().len(),
                0,
                "worker {index} answered without the permit",
            );
        }
        barrier.wait();
        drop(permit);

        step_until(worker, || rig.buffer.borrow().len() == 1);
        for (value, time, diff) in second {
            rig.oks
                .update_at((row(value), Row::default()), Timestamp::from(time), diff);
        }
        rig.advance(8);
        step_until(worker, || rig.buffer.borrow().len() == 3);
        let stashed_bytes = rig.metrics.batched_subscribe_share_bytes_stashed.get();
        drop(rig.protocol.borrow_mut().take());
        (responses(&rig.buffer), stashed_bytes)
    })
}

/// What every worker of [`subscribe_across_two_workers`] reports.
fn expected_subscribe() -> Vec<Vec<Reported>> {
    (0..2)
        .map(|own| {
            vec![
                batch(2, Some(5), &[(10 + own, 2, 1), (20 + own, 3, 1)], false),
                batch(5, Some(7), &[(10 + own, 5, -1), (30 + own, 5, 1)], false),
                batch(7, None, &[], false),
            ]
        })
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_large_description_travels_stashed_and_reads_back_intact() {
    let runtime = runtime();
    // Persist spawns tasks as its clients are created.
    let _runtime = runtime.enter();
    let clients = Arc::new(PersistClientCache::new_no_metrics());
    let setup = Setup::stashing(&clients, 0, usize::MAX);

    let reported = subscribe_across_two_workers(&runtime, setup, false);
    let responses: Vec<_> = reported.iter().map(|(r, _)| r.clone()).collect();

    assert_eq!(
        responses.iter().map(|r| tiers(r)).collect::<Vec<_>>(),
        vec![vec![vec!["stashed"], vec!["stashed"], vec![]]; 2],
        "every share past the inline bound travels stashed, and the completion carries none",
    );
    assert!(
        reported.iter().all(|(_, stashed_bytes)| *stashed_bytes > 0),
        "the stashed bytes are counted",
    );
    assert_eq!(
        reduce_all(&runtime, &clients, &responses),
        expected_subscribe(),
        "a stashed share holds the updates of its description at their times, retractions \
         included, with the first description's advanced to the as-of",
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn an_offloaded_scan_produces_the_same_content_as_an_inline_one() {
    let runtime = runtime();
    // Persist spawns tasks as its clients are created.
    let _runtime = runtime.enter();
    let clients = Arc::new(PersistClientCache::new_no_metrics());
    let on_worker = Setup::stashing(&clients, usize::MAX, usize::MAX);
    let offloaded = Setup::stashing(&clients, usize::MAX, 0);

    let on_worker = subscribe_across_two_workers(&runtime, on_worker, false);
    let offloaded = subscribe_across_two_workers(&runtime, offloaded, true);
    let on_worker: Vec<_> = on_worker.into_iter().map(|(r, _)| r).collect();
    let offloaded: Vec<_> = offloaded.into_iter().map(|(r, _)| r).collect();

    assert_eq!(
        offloaded.iter().map(|r| tiers(r)).collect::<Vec<_>>(),
        vec![vec![vec!["inline"], vec!["inline"], vec![]]; 2],
        "an offloaded share within the inline bound travels inline",
    );
    let on_worker = reduce_all(&runtime, &clients, &on_worker);
    assert_eq!(on_worker, expected_subscribe());
    assert_eq!(reduce_all(&runtime, &clients, &offloaded), on_worker);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_dropped_subscribe_leaves_no_stashed_parts_on_the_replica() {
    let runtime = runtime();
    let _runtime = runtime.enter();
    // Every update the upload takes becomes a part in blob storage.
    let blob = Arc::new(CountedBlob::new());
    // Every update is a hand-out of its own, so the share leaves the worker with its scan
    // unfinished, and the scan then waits for the permit the test holds.
    let setup = Setup {
        cut_bytes: 0,
        ..Setup::stashing(blob.clients(), 0, usize::MAX)
    }
    .with(|updates| {
        // A merge of runs writes parts of its own and leaves those it merged behind.
        updates.add(&PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, 1024);
    });
    let sink_id = sink_minted_on_worker_zero();

    let rt = Arc::clone(&runtime);
    let counted = Arc::clone(&blob);
    let responses = run_two_workers(move |worker, barrier| {
        let _runtime = rt.enter();
        let index = worker.index();
        let mut rig = Rig::with_setup(worker, sink_id, 0, Antichain::new(), &setup);
        rig.open_gate();
        let permit = (index == 0).then(|| {
            setup
                .permits
                .try_acquire()
                .expect("no scan holds the permit yet")
        });
        if index == 0 {
            for value in 0..4 {
                rig.oks
                    .update_at((row(value), Row::default()), Timestamp::from(1), Diff::ONE);
            }
        }
        rig.advance(3);
        step_until(worker, || counted.written() > 0);
        settle(worker);
        barrier.wait();

        // Dropping the sink token takes the protocol, and the next activation of the write
        // operator drops the share under way.
        drop(rig.protocol.borrow_mut().take());
        rig.advance(4);
        step_until(worker, || counted.deleted() == counted.written());
        drop(permit);
        responses(&rig.buffer)
    });

    let reported: Vec<Vec<Reported>> = responses
        .iter()
        .map(|worker| {
            worker
                .iter()
                .map(|response| reduce(response, |_, _| panic!("no stashed share is reported")))
                .collect()
        })
        .collect();
    assert_eq!(
        reported,
        vec![
            vec![Reported::DroppedAt(Some(0))],
            vec![batch(0, Some(3), &[], false), Reported::DroppedAt(Some(3))],
        ],
        "the worker whose share was under way reports the drop at the as-of",
    );
    assert!(blob.written() > 0);
    assert_eq!(
        blob.deleted(),
        blob.written(),
        "every part written is deleted"
    );
    assert_eq!(blob.deletes_of_nothing(), 0);
}

#[mz_ore::test]
fn a_poisoned_description_still_carries_its_shares() {
    let sink_id = GlobalId::User(1);
    let (protocol, buffer) = protocol(sink_id, 0);
    let mut protocol = protocol.borrow_mut().take().expect("a fresh protocol");
    protocol.poison = Some("poisoned".into());
    let updates = vec![(row(1), Timestamp::from(1), Diff::ONE)];
    let share = super::inline_share(&updates).expect("one update");
    protocol.report(&description(0, Some(3), true, false), vec![share]);
    assert_eq!(
        reported(&buffer),
        vec![batch(0, Some(3), &[(1, 1, 1)], true)],
        "the receiver learns of every share, so it can delete their stash batches",
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn a_stashed_share_of_a_description_with_an_empty_upper_reads_back_intact() {
    let runtime = runtime();
    let _runtime = runtime.enter();
    let clients = Arc::new(PersistClientCache::new_no_metrics());
    let setup = Setup::stashing(&clients, 0, usize::MAX);
    let sink_id = sink_minted_on_worker_zero();

    let rt = Arc::clone(&runtime);
    let responses = run_two_workers(move |worker, _barrier| {
        let _runtime = rt.enter();
        let own = i64::try_from(worker.index()).expect("two workers");
        let rig = Rig::with_setup(worker, sink_id, 0, Antichain::new(), &setup);
        let Rig {
            mut oks,
            mut errs,
            gate,
            buffer,
            protocol,
            ..
        } = rig;
        drop(gate);
        oks.update_at(
            (row(10 + own), Row::default()),
            Timestamp::from(1),
            Diff::ONE,
        );
        oks.advance_to(Timestamp::from(3));
        oks.flush();
        errs.advance_to(Timestamp::from(3));
        errs.flush();
        step_until(worker, || buffer.borrow().len() == 1);

        // Closing the inputs takes their frontier to the empty antichain, so the mint cuts a
        // last description `[3, ∅)`.
        oks.update_at(
            (row(20 + own), Row::default()),
            Timestamp::from(4),
            Diff::ONE,
        );
        oks.close();
        errs.close();
        step_until(worker, || buffer.borrow().len() == 2);
        drop(protocol);
        responses(&buffer)
    });

    assert_eq!(
        responses.iter().map(|r| tiers(r)).collect::<Vec<_>>(),
        vec![vec![vec!["stashed"], vec!["stashed"]]; 2],
    );
    assert_eq!(
        reduce_all(&runtime, &clients, &responses),
        (0..2)
            .map(|own| vec![
                batch(0, Some(3), &[(10 + own, 1, 1)], false),
                batch(3, None, &[(20 + own, 4, 1)], false),
            ])
            .collect::<Vec<_>>(),
        "a stash batch with an empty upper holds the updates of its description",
    );
}
