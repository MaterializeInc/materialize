// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_repr::{Datum, Row};
use mz_row_spine::{DatumSeq, RowRowBatcher};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::progress::Antichain;

use crate::arrangement::manager::{ErrsTrace, OksTrace};
use crate::compute_state::ComputeState;
use crate::compute_state::tests::{
    LivePublisher, allow_compaction, apply_and_bind, create_dataflow, interactive_compute_state,
    maintained_index_dataflow, maintenance_reporter, row, test_persist_clients,
};
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::context::ArrangementFlavor;
use crate::render::errors::DataflowErrorSer;
use crate::server::ResponseSender;
use crate::shared_trace::tests::{SharedReaderExt, drop_dataflows};
use crate::shared_trace::{PeerErrsReader, PeerOksReader, SharedOksHandle, adopt_trace};

/// What a reader of the test publications here compares against their slots.
fn test_check() -> SlotCheck {
    SlotCheck::logging()
}
use crate::typedefs::ErrBatcher;

use super::*;

/// The `as_of` of a dataflow that reads from the minimum time.
fn as_of_min() -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::MIN)
}

/// Builds a tiny dataflow that arranges `rows` into a `RowRow` `oks` arrangement and an empty
/// `errs` arrangement, publishes both, and returns a registry that holds them under `id` as
/// incarnation 0, with the slot that keeps the publication. The dataflow runs to completion inside
/// `execute_directly`. The published chain outlives the worker through its `Arc`s, and nothing
/// compacts the trace, so the snapshot reads below observe the sealed contents even after the
/// publishing worker has torn down.
fn publish_index(id: GlobalId, rows: Vec<(Row, Row)>) -> (ArrangementSharingRegistry, Publication) {
    let registry = ArrangementSharingRegistry::new();
    let token = publish_index_into(&registry, id, 0, rows);
    (registry, token)
}

/// Like `publish_index`, but publishes into the given `registry` instead of a fresh one, as the
/// incarnation `creation`.
///
/// Routes through [`ArrangementSharingRegistry::publish`], the path the maintenance render side
/// uses.
fn publish_index_into(
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    creation: CreationNumber,
    rows: Vec<(Row, Row)>,
) -> Publication {
    let registry_in = registry.clone();
    timely::execute_directly(move |worker| {
        // The trace lives as long as an agent does, and the point closes when it drops, so the
        // agents must outlive the stepping that seals the batches. Production keeps them in the
        // trace manager. `execute_directly` steps only after this closure returns, so step here.
        let (keep, token, publication) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks =
                oks_collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("test oks");

            let (mut errs_input, errs_collection) =
                scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection)
                .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("test errs");

            let token = registry_in.publish(
                id,
                creation,
                test_check(),
                oks.stream.scope(),
                &oks.trace,
                &errs.trace,
            );
            // The publishing dataflow runs to completion and drops its trace, which releases the
            // writer's compaction. A reader's hold is what keeps the published `since` at the
            // minimum after that.
            let slot = registry_in.slot(id, creation).expect("published");
            let minimum = Antichain::from_elem(Timestamp::MIN);
            let holds = (
                slot.oks.shared.reader_at_least(&minimum),
                slot.errs.shared.reader_at_least(&minimum),
            );

            for (k, v) in rows {
                oks_input.update((k, v), Diff::ONE);
            }
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
            (
                (oks.trace.clone(), errs.trace.clone()),
                token,
                Publication {
                    _slot: slot,
                    _holds: holds,
                },
            )
        });
        // The token's replay of the hold link would keep the dataflow running. The slot, which the
        // caller holds, keeps the publication.
        drop(token);
        while worker.step() {}
        drop(keep);
        publication
    })
}

/// A publication that tests read after its publishing worker has torn down: the slot the
/// publisher's token held, and readers' holds at the minimum.
struct Publication {
    _slot: Arc<SharedIndexArrangement>,
    _holds: (SharedOksHandle, SharedErrsHandle),
}

fn test_rows() -> Vec<(Row, Row)> {
    vec![
        (
            Row::pack_slice(&[Datum::Int32(1)]),
            Row::pack_slice(&[Datum::String("a")]),
        ),
        (
            Row::pack_slice(&[Datum::Int32(2)]),
            Row::pack_slice(&[Datum::String("b")]),
        ),
    ]
}

#[mz_ore::test]
fn a_frozen_slot_is_kept_while_a_new_incarnation_gets_its_own() {
    // The publishing worker tears down, so the slot's traces drop and its points freeze. A new
    // incarnation publishes into a slot of its own, and readers of the frozen slot keep its final
    // view.
    let id = GlobalId::User(1);
    let (registry, _old_publication) = publish_index(id, test_rows());
    let old = registry.slot(id, 0).expect("published");
    assert!(old.is_frozen());
    let _new_publication = publish_index_into(&registry, id, 1, vec![]);
    let new = registry.slot(id, 1).expect("published");
    assert!(!Arc::ptr_eq(&old, &new), "a frozen slot is not reused");
    assert!(
        Arc::ptr_eq(&registry.slot(id, 0).expect("held"), &old),
        "the old incarnation keeps its slot"
    );
    assert_eq!(
        read_rows(&old.oks.handle(), Timestamp::from(0_u64)),
        expected_rows(&test_rows()),
        "readers of the frozen slot keep its final view"
    );
}

#[mz_ore::test]
#[should_panic(expected = "published twice")]
fn publishing_a_live_identity_twice_is_a_protocol_violation() {
    let registry = ArrangementSharingRegistry::new();
    let id = GlobalId::User(1);
    timely::execute_directly(move |worker| {
        let (_first, first_traces) = publish_empty(worker, &registry, id, 0);
        let _second = publish_reexport(worker, &registry, id, 0, &first_traces);
    });
}

#[mz_ore::test]
fn a_frozen_slot_reports_no_hold_gap() {
    // A peer holds the index at the minimum while its agents ask for time one, which is a gap.
    // Once the incarnation drops, its slot freezes with that gap in its final view, which the
    // metric does not report, see the NOTE on `hold_gaps`.
    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();
    timely::execute_directly(move |worker| {
        let (dataflow, token, (mut oks, errs, oks_input, errs_input)) =
            render_incarnation(worker, &registry, id, 0, &as_of_min());
        let _peer = registry.peer_bundle_at(id, 0, &as_of_min());
        oks.set_logical_compaction(Antichain::from_elem(Timestamp::from(1_u64)).borrow());
        assert_eq!(registry.hold_gaps(), (1, 1), "the peer holds the oks back");
        drop((token, oks, errs, oks_input, errs_input));
        worker.drop_dataflow(dataflow);
        assert_eq!(registry.hold_gaps(), (0, 0), "a frozen slot reports no gap");
    });
}

#[mz_ore::test]
fn handles_available_while_published_gone_after_unpublish() {
    let id = GlobalId::User(1);
    let (registry, token) = publish_index(id, test_rows());

    // A published index yields handles, and an unknown id none.
    assert!(registry.handles(&id).is_some());
    assert!(registry.handles(&GlobalId::User(2)).is_none());

    drop(token);
    assert!(registry.handles(&id).is_none());
}

#[mz_ore::test]
fn a_reader_keeps_the_slot_after_unpublish() {
    let id = GlobalId::User(1);
    let (registry, token) = publish_index(id, test_rows());
    let reader = ArrangementSharingRegistry::newest_slot(&registry.lock(), &id).expect("published");

    drop(token);
    assert!(
        Arc::ptr_eq(
            &ArrangementSharingRegistry::newest_slot(&registry.lock(), &id).expect("held"),
            &reader
        ),
        "the slot a reader holds is still the registered one"
    );
    let held = Arc::downgrade(&reader);
    drop(reader);
    assert!(
        held.upgrade().is_none(),
        "the registry holds no slot itself"
    );
    assert!(registry.handles(&id).is_none());
}

#[mz_ore::test]
fn a_registry_pairs_one_publishing_with_one_reading_thread() {
    let registry = ArrangementSharingRegistry::new();
    registry.attach_publisher();
    registry.attach_reader();
    // Attaching again from the same threads is a no-op.
    registry.attach_publisher();
    registry.attach_reader();

    let other = registry.clone();
    let second_publisher = thread::spawn(move || other.attach_publisher()).join();
    assert!(
        second_publisher.is_err(),
        "a second publishing thread attached"
    );
    let other = registry.clone();
    let second_reader = thread::spawn(move || other.attach_reader()).join();
    assert!(second_reader.is_err(), "a second reading thread attached");
}

#[mz_ore::test]
fn reexport_publishes_its_own_point_over_the_same_trace() {
    let target = GlobalId::User(1);
    let reexport = GlobalId::User(2);
    let registry = ArrangementSharingRegistry::new();
    registry.attach_reader();
    let registry_in = registry.clone();
    let (target_slot, reexport_slot, _holds) = timely::execute_directly(move |worker| {
        let (oks, errs, mut oks_input, mut errs_input, target_token) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                let (oks_input, oks) = scope.new_collection::<(Row, Row), Diff>();
                let oks = oks.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("test oks");
                let (errs_input, errs) = scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs)
                    .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("test errs");
                let token = registry_in.publish(
                    target,
                    0,
                    test_check(),
                    oks.stream.scope(),
                    &oks.trace,
                    &errs.trace,
                );
                (oks.trace, errs.trace, oks_input, errs_input, token)
            });

        // The re-export's dataflow must build the same graph on every worker, so publishing the
        // shared traces under its id builds only what publishing them under any id builds, the
        // replay of the slot's hold link.
        let before = worker.peek_identifier();
        let other_token = worker.dataflow::<Timestamp, _, _>(|scope| {
            registry_in.publish(GlobalId::User(3), 0, test_check(), scope, &oks, &errs)
        });
        let publication = worker.peek_identifier() - before;
        drop(other_token);
        let before = worker.peek_identifier();
        let reexport_token = worker.dataflow::<Timestamp, _, _>(|scope| {
            registry_in.publish(reexport, 1, test_check(), scope, &oks, &errs)
        });
        assert_eq!(worker.peek_identifier() - before, publication);
        // Readers' holds keep both points readable after the publishing worker has torn down.
        let target_slot = registry_in.slot(target, 0).expect("published");
        let reexport_slot = registry_in.slot(reexport, 1).expect("published");
        let holds = [&target_slot, &reexport_slot].map(|slot| {
            (
                slot.oks.shared.reader_at_least(&as_of_min()),
                slot.errs.shared.reader_at_least(&as_of_min()),
            )
        });
        // The tokens' replays of the hold links would keep the dataflows running. The slots, which
        // the test holds, keep the publications.
        drop((target_token, reexport_token));

        for (k, v) in test_rows() {
            oks_input.update((k, v), Diff::ONE);
        }
        oks_input.advance_to(Timestamp::from(1_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(1_u64));
        errs_input.flush();
        drop((oks_input, errs_input));
        while worker.step() {}
        drop((oks, errs));
        (target_slot, reexport_slot, holds)
    });

    for id in [target, reexport] {
        let (oks, _) = registry.handles(&id).expect("published");
        assert_eq!(
            read_rows(&oks, Timestamp::from(0_u64)),
            expected_rows(&test_rows())
        );
    }

    // The two collections compact independently, so a hold on one point does not reach the other.
    let at = |t: u64| Antichain::from_elem(Timestamp::from(t));
    let target_hold = target_slot.oks.shared.reader_at_least(&at(5));
    let reexport_hold = reexport_slot.oks.shared.reader_at_least(&at(10));
    drop(_holds);
    assert_eq!(registry.published_logical_holds(&target), Some(at(5)));
    assert_eq!(registry.published_logical_holds(&reexport), Some(at(10)));
    drop((target_hold, reexport_hold));

    // Dropping the index the re-export re-exports leaves the re-export's point readable.
    drop(target_slot);
    assert!(registry.handles(&target).is_none());
    assert!(registry.handles(&reexport).is_some());
    drop(reexport_slot);
}

/// Walks a snapshot of `handle` at `at` into a sorted `Vec` of owned (key, value) rows,
/// keeping only entries whose accumulated diff at `at` is nonzero.
///
/// Packs each key/value into an owned `Row`, since the snapshot's `storage` is local to this
/// function.
fn read_rows(handle: &SharedOksHandle, at: Timestamp) -> Vec<(Row, Row)> {
    let snapshot = handle.snapshot_at(&at).expect("snapshot at sealed time");

    let (mut cursor, storage) = snapshot.cursor();
    let mut found: Vec<(Row, Row)> = Vec::new();
    while cursor.key_valid(&storage) {
        while cursor.val_valid(&storage) {
            let key = Row::pack_slice(&cursor.key(&storage).into_iter().collect::<Vec<_>>());
            let val = Row::pack_slice(&cursor.val(&storage).into_iter().collect::<Vec<_>>());
            let mut diff = Diff::ZERO;
            cursor.map_times(&storage, |_t, d| diff += d);
            if !diff.is_zero() {
                found.push((key, val));
            }
            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }
    found.sort();
    found
}

/// The row shape `read_rows` returns for the same `(Row, Row)` pairs given to
/// `publish_index`/`publish_index_into`, so a test can compare the two directly.
fn expected_rows(rows: &[(Row, Row)]) -> Vec<(Row, Row)> {
    let mut expected = rows.to_vec();
    expected.sort();
    expected
}

#[mz_ore::test]
fn minted_handle_snapshots_the_index_rows() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let (registry, _token) = publish_index(id, rows.clone());

    // The rows were written at time 0 and sealed by advancing the input to 1.
    let (oks, _errs) = registry.handles(&id).expect("published");
    assert_eq!(
        read_rows(&oks, Timestamp::from(0_u64)),
        expected_rows(&rows)
    );
}

#[mz_ore::test]
fn cross_runtime_read_sees_published_rows() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let registry = ArrangementSharingRegistry::new();

    // Runtime A: a bare timely cluster that arranges and publishes the rows, then tears down.
    let _token = publish_index_into(&registry, id, 0, rows.clone());

    // Runtime B: read the published rows from a different thread than the one that ran A's
    // dataflow, exercising the `Send` handle across a runtime boundary.
    let reader_registry = registry.clone();
    let found = thread::spawn(move || {
        let (oks, _errs) = reader_registry.handles(&id).expect("published by A");
        read_rows(&oks, Timestamp::from(0_u64))
    })
    .join()
    .expect("reader thread panicked");

    assert_eq!(found, expected_rows(&rows));
}

/// An input update: `(key, value, time, diff)`. Keys and values are single-column rows, so a
/// join emits a three-column `(key, value1, value2)` row that we can compare directly.
type Update = (i64, &'static str, u64, i64);

fn key_row(k: i64) -> Row {
    Row::pack_slice(&[Datum::Int64(k)])
}

fn val_row(v: &str) -> Row {
    Row::pack_slice(&[Datum::String(v)])
}

/// The join of `a` and `b` computed directly, consolidated per `(row, time)`.
///
/// Matches the differential contract: a pair of matching updates produces one output at the
/// lattice join (here, the max) of their times, with the product of their diffs. This is the
/// oracle the imported-arrangement join must reproduce.
fn expected_join(a: &[Update], b: &[Update]) -> Vec<(Row, Timestamp, Diff)> {
    let mut out: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for &(ka, la, ta, da) in a {
        for &(kb, rb, tb, db) in b {
            if ka != kb {
                continue;
            }
            let row = Row::pack_slice(&[Datum::Int64(ka), Datum::String(la), Datum::String(rb)]);
            let time = Timestamp::from(ta.max(tb));
            *out.entry((row, time)).or_insert(Diff::ZERO) += Diff::from(da * db);
        }
    }
    let mut v: Vec<_> = out
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((row, t), d)| (row, t, d))
        .collect();
    v.sort();
    v
}

/// Publishes `updates` as a `RowRow` index under `id`, driving the input across its distinct
/// times and stepping the worker between them so the trace seals several batches. An empty
/// `errs` arrangement is published alongside, as the registry slot requires both halves.
///
/// `seal` is one past the last update time, the frontier at which the last batch closes.
fn publish_join_input(
    registry: &ArrangementSharingRegistry,
    worker: &mut timely::worker::Worker,
    id: GlobalId,
    updates: &[Update],
    seal: u64,
) -> JoinInput {
    let registry_in = registry.clone();
    let updates = updates.to_vec();

    let (mut oks_input, mut errs_input, (oks, errs, token)) =
        worker.dataflow::<Timestamp, _, _>(move |scope| {
            let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks =
                oks_collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("input oks");

            let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection)
                .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("input errs");

            let token = registry_in.publish(
                id,
                0,
                test_check(),
                oks.stream.scope(),
                &oks.trace,
                &errs.trace,
            );
            (
                oks_input,
                errs_input,
                (oks.trace.clone(), errs.trace.clone(), token),
            )
        });

    // Distinct update times in order. Insert each time's updates, then advance and step, so the
    // publisher seals and appends one batch per time.
    let mut times: Vec<u64> = updates.iter().map(|&(_, _, t, _)| t).collect();
    times.sort_unstable();
    times.dedup();

    for &t in &times {
        oks_input.advance_to(Timestamp::from(t));
        for &(k, v, ut, d) in &updates {
            if ut == t {
                oks_input.update((key_row(k), val_row(v)), Diff::from(d));
            }
        }
        oks_input.flush();
        // Step so the arrange operator observes this frontier and the publisher appends the
        // sealed batch to importer queues before the next time is loaded.
        for _ in 0..16 {
            worker.step();
        }
    }
    oks_input.advance_to(Timestamp::from(seal));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(seal));
    errs_input.flush();

    JoinInput {
        oks_input,
        errs_input,
        oks,
        errs,
        _token: token,
        filler: seal,
    }
}

/// A published join input: its inputs, its trace agents, and the filler clock its ticks advance.
///
/// The inputs keep the dataflow from draining to the empty frontier, and the agents keep the trace
/// alive, either of which would close the publication before an importer has read it. Production
/// keeps the agents in the trace manager.
struct JoinInput {
    oks_input: differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
    errs_input: differential_dataflow::input::InputSession<Timestamp, DataflowErrorSer, Diff>,
    oks: crate::typedefs::RowRowAgent<Timestamp, Diff>,
    errs: crate::typedefs::ErrAgent<Timestamp, Diff>,
    /// Keeps the publication for the input's life.
    _token: PublishToken,
    filler: u64,
}

impl JoinInput {
    /// Advances the inputs to a fresh filler time and steps the worker once, so the arrange
    /// operators keep activating the way a live index's do in production. The filler times carry
    /// no updates, so they add empty seal-only batches and advance `upper` without changing any
    /// accumulation. The agents' physical compaction follows the upper, as the trace manager's
    /// maintenance does, so the spines may merge.
    fn tick(&mut self, worker: &mut timely::worker::Worker) {
        self.filler += 1;
        let upper = Antichain::from_elem(Timestamp::from(self.filler));
        self.oks_input.advance_to(Timestamp::from(self.filler));
        self.oks_input.flush();
        self.errs_input.advance_to(Timestamp::from(self.filler));
        self.errs_input.flush();
        worker.step();
        self.oks.set_physical_compaction(upper.borrow());
        self.errs.set_physical_compaction(upper.borrow());
    }

    /// Compacts both arrangements to `frontier` on their own agents, as `handle_allow_compaction`
    /// does through the trace manager.
    fn allow_compaction(&mut self, frontier: &Antichain<Timestamp>) {
        self.oks.set_logical_compaction(frontier.borrow());
        self.errs.set_logical_compaction(frontier.borrow());
    }
}

/// The core spike: a maintenance-published index consumed *as an arrangement* by a join.
///
/// Two `RowRow` indexes are published (each sealing several batches), imported through
/// `SharedReader::import_frontier_core` over the full `[0, seal)` range so every distinct time
/// stays visible, and joined with differential's `join_core`, which drives the same
/// `cursor_through`/`batches_through` boundary as `mz_join_core`. The join runs live alongside
/// the publishers in one worker, so the imported batches arrive incrementally and the join
/// performs incremental `cursor_through` cuts as its acknowledged frontiers advance. The captured
/// output must equal the join computed directly.
///
/// Publisher and importer share a worker only to step in lockstep for a deterministic read. The
/// handle that crosses between them is `Send` and the code exercised (import replay, chain cut)
/// is identical to a true second runtime.
#[mz_ore::test]
fn join_over_imported_arrangements_matches_direct() {
    let id_a = GlobalId::User(1);
    let id_b = GlobalId::User(2);

    // A: key 1 inserted then retracted, plus keys 2 and 3 at later times.
    let a: Vec<Update> = vec![
        (1, "a", 0, 1),
        (2, "b", 0, 1),
        (3, "c", 1, 1),
        (1, "a", 2, -1),
    ];
    // B: one value per key, appearing at staggered times.
    let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
    let seal = 3;

    let expected = expected_join(&a, &b);

    let (capture_tx, capture_rx) = mpsc::channel();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();

        // Maintenance side: publish both indexes, sealing several batches each.
        let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
        let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);

        let (oks_a, _errs_a) = registry.handles(&id_a).expect("A published");
        let (oks_b, _errs_b) = registry.handles(&id_b).expect("B published");

        // Interactive side: import both as arrangements and join them. `as_of = 0` matches the
        // earliest real time in either input, so no update coalesces, and `until = seal` keeps
        // every distinct time in `[0, seal)` visible.
        let as_of = Antichain::from_elem(Timestamp::from(0_u64));
        let until = Antichain::from_elem(Timestamp::from(seal));
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr_a =
                oks_a.import_frontier_core(scope.clone(), "import A", as_of.clone(), until.clone());
            let arr_b = oks_b.import_frontier_core(scope.clone(), "import B", as_of, until);
            let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                let row = Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                Some(row)
            });
            joined
                .inner
                .probe_with(&probe)
                .capture_into(capture_tx.clone());
        });

        // Step until the join has sealed through the seal frontier, keeping the publisher
        // inputs alive so their publication points stay open.
        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while probe.less_than(&seal_ts) {
            keep_a.tick(worker);
            keep_b.tick(worker);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "join did not seal through {seal_ts:?}");
        }
    });

    let mut got: Vec<(Row, Timestamp, Diff)> = capture_rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    // Consolidate the captured stream per `(row, time)` so we compare final deltas.
    got.sort();
    let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for (row, time, diff) in got {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
    }
    let got: Vec<(Row, Timestamp, Diff)> = consolidated
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((row, t), d)| (row, t, d))
        .collect();

    assert_eq!(
        got, expected,
        "join over imported arrangements diverged from the direct join"
    );
}

/// Renders a `RowRow` index that ADOPTS an existing publication point, driving the
/// input across its distinct times and stepping between them so the trace seals several batches.
///
/// Mirrors [`publish_join_input`], but instead of minting a fresh publication and registering it,
/// it installs its publisher into the caller-provided `point` via
/// [`adopt_trace`]. The point may already back live importers (see
/// [`join_over_point_adopted_late_matches_direct`]). Adoption fills their queues from the
/// same publisher iteration. Only the `oks` arrangement is adopted, since the test joins on `oks`.
fn adopt_join_input(
    point: &Published<RowRowSpine<Timestamp, Diff>>,
    worker: &mut timely::worker::Worker,
    updates: &[Update],
    seal: u64,
) -> impl FnMut(&mut timely::worker::Worker) + use<> {
    let updates = updates.to_vec();

    let (mut oks_input, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
        let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("adopt oks");
        // Attach this arrangement's trace to the pre-existing point. Importers already registered against it (built before this call) are seeded now.
        adopt_trace(&oks.trace, oks.stream.scope().worker(), point, || {});
        (oks_input, oks.trace.clone())
    });

    let mut times: Vec<u64> = updates.iter().map(|&(_, _, t, _)| t).collect();
    times.sort_unstable();
    times.dedup();
    for &t in &times {
        oks_input.advance_to(Timestamp::from(t));
        for &(k, v, ut, d) in &updates {
            if ut == t {
                oks_input.update((key_row(k), val_row(v)), Diff::from(d));
            }
        }
        oks_input.flush();
        for _ in 0..16 {
            worker.step();
        }
    }
    oks_input.advance_to(Timestamp::from(seal));
    oks_input.flush();

    move |worker: &mut timely::worker::Worker| {
        let _keep = (&oks_input, &keep);
        worker.step();
    }
}

/// A differential join whose input trace is an unattached point at construction fills correctly
/// once the point is adopted in place.
///
/// This exercises late adoption at the `Published` level. `a` is imported and joined BEFORE any
/// publisher for it exists: the test creates an unattached point, takes a handle, imports it, and
/// builds `join_core` over the EMPTY point, capturing the trace by value at construction. `id_b`
/// is published normally as an already-materialized co-input.
///
/// The test asserts two things:
/// * While `a` is unadopted, the join produces nothing and its frontier stays pinned at the
///   minimum (held by the import at `upper = [0]`).
/// * After the maintenance side renders `a`'s arrangement and ADOPTS the same `Arc` (installing a
///   publisher that fills the already-registered importer queue), the captured output equals the
///   direct join, with correct multiplicities and no doubling.
#[mz_ore::test]
fn join_over_point_adopted_late_matches_direct() {
    let id_b = GlobalId::User(2);

    // Same inputs as `join_over_imported_arrangements_matches_direct`: key 1 inserted then
    // retracted, plus keys 2 and 3, joined against one value per key.
    let a: Vec<Update> = vec![
        (1, "a", 0, 1),
        (2, "b", 0, 1),
        (3, "c", 1, 1),
        (1, "a", 2, -1),
    ];
    let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
    let seal = 3;

    let expected = expected_join(&a, &b);

    let (capture_tx, capture_rx) = mpsc::channel();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();

        // B: published normally, an already-materialized co-input.
        let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);
        let (oks_b, _errs_b) = registry.handles(&id_b).expect("B published");

        // A: an unattached point, created before any publisher exists. Take its reader handle now.
        let point_a: Published<RowRowSpine<Timestamp, Diff>> = Published::new();
        let oks_a = point_a.handle();

        // Import both as arrangements and join them. A is imported over the EMPTY point.
        // `join_core` captures `arr_a.trace` by value here, before A has any publisher. This is
        // exactly the construction-time capture that late-binding import must survive. `as_of = 0`
        // matches the earliest real time in either input, so no update coalesces, and
        // `until = seal` keeps every distinct time in `[0, seal)` visible.
        let as_of = Antichain::from_elem(Timestamp::from(0_u64));
        let until = Antichain::from_elem(Timestamp::from(seal));
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr_a = oks_a.import_frontier_core(
                scope.clone(),
                "import A (unbacked)",
                as_of.clone(),
                until.clone(),
            );
            let arr_b = oks_b.import_frontier_core(scope.clone(), "import B", as_of, until);
            let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                let row = Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                Some(row)
            });
            joined
                .inner
                .probe_with(&probe)
                .capture_into(capture_tx.clone());
        });

        // Step with A still unadopted. The import holds A's frontier at the minimum,
        // so the join frontier cannot pass 0 and no output is produced.
        for _ in 0..64 {
            keep_b.tick(worker);
            worker.step();
        }
        assert!(
            probe.less_than(&Timestamp::from(1_u64)),
            "join advanced past time 0 before A was adopted"
        );

        // Maintenance side: NOW render A's arrangement and ADOPT the same point, feeding
        // A's updates. The join built above must observe the filled chain through its captured
        // handle without being rebuilt.
        let mut keep_a = adopt_join_input(&point_a, worker, &a, seal);

        // Step until the join has sealed through the seal frontier.
        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while probe.less_than(&seal_ts) {
            keep_a(worker);
            keep_b.tick(worker);
            worker.step();
            steps += 1;
            assert!(
                steps < 10_000,
                "join did not seal through {seal_ts:?} after adopt"
            );
        }
    });

    let mut got: Vec<(Row, Timestamp, Diff)> = capture_rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    got.sort();
    let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for (row, time, diff) in got {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
    }
    let got: Vec<(Row, Timestamp, Diff)> = consolidated
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((row, t), d)| (row, t, d))
        .collect();

    assert_eq!(
        got, expected,
        "join over a late-adopted point diverged from the direct join"
    );
}

/// SPIKE (decision 4, hardest risk, assumption 1): a bare `SharedOksHandle` held on a reader
/// thread that runs NO import/replay operator observes the published `upper` advance via
/// `TraceReader::read_upper` as the publisher (a separate thread's worker) seals successive
/// times.
///
/// The publisher and reader are on separate threads, handshaking per sealed time so the check is
/// deterministic: the publisher steps its worker (refreshing the
/// shared chain under the lock), announces the sealed time, and blocks. The reader then reads
/// `read_upper` on its bare handle and must see exactly that frontier before acking. The reader
/// never builds a dataflow, so this proves `read_upper` reflects the publisher-refreshed chain
/// directly, not a locally-drained copy.
#[mz_ore::test]
fn bare_handle_read_upper_advances_cross_thread() {
    use differential_dataflow::trace::TraceReader;

    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();
    let publisher_registry = registry.clone();

    // Handshake channels: publisher -> reader announces each sealed time, and reader -> publisher
    // acks so the publisher advances only after the reader has observed the current seal.
    let (tick_tx, tick_rx) = mpsc::channel::<u64>();
    let (ack_tx, ack_rx) = mpsc::channel::<()>();

    let seals: Vec<u64> = vec![1, 2, 3, 4, 5];
    let publisher_seals = seals.clone();

    // `execute_directly` requires a `Send + Sync` closure, but `mpsc` endpoints are not `Sync`.
    // A `Mutex` makes them `Sync`, and the single publisher worker is the only user.
    let tick_tx = std::sync::Mutex::new(tick_tx);
    let ack_rx = std::sync::Mutex::new(ack_rx);

    let publisher = thread::spawn(move || {
        timely::execute_directly(move |worker| {
            let (mut oks_input, mut errs_input, _keep) =
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection
                        .mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("spike oks");

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection)
                        .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("spike errs");

                    // The token is held here for the publisher's life.
                    let token = publisher_registry.publish(
                        id,
                        0,
                        test_check(),
                        oks.stream.scope(),
                        &oks.trace,
                        &errs.trace,
                    );
                    (
                        oks_input,
                        errs_input,
                        (oks.trace.clone(), errs.trace.clone(), token),
                    )
                });

            for &t in &publisher_seals {
                // Add a row just below the seal time, then advance the frontier to `t` and step
                // so the arrange operator seals the batch and the publisher refreshes the shared
                // chain to `upper = {t}`.
                oks_input.update(
                    (
                        Row::pack_slice(&[Datum::Int64(i64::from(u32::try_from(t).unwrap()))]),
                        Row::pack_slice(&[Datum::String("v")]),
                    ),
                    Diff::ONE,
                );
                oks_input.advance_to(Timestamp::from(t));
                oks_input.flush();
                errs_input.advance_to(Timestamp::from(t));
                errs_input.flush();
                for _ in 0..32 {
                    worker.step();
                }
                tick_tx
                    .lock()
                    .unwrap()
                    .send(t)
                    .expect("reader waits for ticks");
                ack_rx
                    .lock()
                    .unwrap()
                    .recv()
                    .expect("reader acks each tick");
            }
        });
    });

    // Reader thread (the current thread): acquire a BARE handle and drive only `read_upper`.
    let (mut oks, _errs) = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(handles) = registry.handles(&id) {
                break handles;
            }
            assert!(Instant::now() < deadline, "publisher never published id");
            thread::sleep(Duration::from_millis(5));
        }
    };

    let mut observed: Vec<u64> = Vec::new();
    for _ in &seals {
        let t = tick_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("publisher announces each sealed time");
        // Read the bare handle's upper. The publisher already refreshed the shared chain under
        // the lock during its `step` before announcing `t`, so a single read suffices. A short
        // retry only guards against scheduler slack, never masks a missing advance.
        let mut upper = Antichain::new();
        let expected = Timestamp::from(t);
        let read_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            oks.read_upper(&mut upper);
            if upper.elements().first() == Some(&expected) {
                break;
            }
            assert!(
                Instant::now() < read_deadline,
                "read_upper never reached {expected:?}; observed {:?}",
                upper.elements()
            );
            thread::sleep(Duration::from_millis(2));
        }
        observed.push(t);
        ack_tx.send(()).expect("publisher waits for ack");
    }

    publisher.join().expect("publisher thread panicked");

    // The reader saw every seal advance, in order, with no operator of its own: proof that a
    // bare handle's `read_upper` tracks the publisher-driven chain.
    assert_eq!(observed, seals);
}

/// The inputs of a test publication's arrangements.
type TestInputs = (
    differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
    differential_dataflow::input::InputSession<Timestamp, DataflowErrorSer, Diff>,
);

/// Arranges empty `oks` and `errs` on `worker` and publishes them under `id` as incarnation
/// `creation`. Returns the token, and the agents and inputs that keep the traces alive until
/// dropped.
fn publish_empty(
    worker: &mut timely::worker::Worker,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    creation: CreationNumber,
) -> (
    PublishToken,
    (
        RowRowAgent<Timestamp, Diff>,
        ErrAgent<Timestamp, Diff>,
        TestInputs,
    ),
) {
    worker.dataflow::<Timestamp, _, _>(|scope| {
        let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("oks");
        let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection)
            .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("errs");
        let token = registry.publish(id, creation, test_check(), scope, &oks.trace, &errs.trace);
        (
            token,
            (
                oks.trace.clone(),
                errs.trace.clone(),
                (oks_input, errs_input),
            ),
        )
    })
}

/// Publishes `id` as a re-export of `base`'s traces, as incarnation `creation`, from a dataflow of
/// its own.
fn publish_reexport(
    worker: &mut timely::worker::Worker,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    creation: CreationNumber,
    base: &(
        RowRowAgent<Timestamp, Diff>,
        ErrAgent<Timestamp, Diff>,
        TestInputs,
    ),
) -> PublishToken {
    worker.dataflow::<Timestamp, _, _>(|scope| {
        registry.publish(id, creation, test_check(), scope, &base.0, &base.1)
    })
}

/// A reconnect recreates a re-export. The base trace backs the new incarnation's points while a
/// reader of the old incarnation, which outlives the old re-exporting index, keeps reading the old
/// points, and the trace keeps publishing into them.
#[mz_ore::test]
fn a_recreated_reexport_gets_a_fresh_slot_while_readers_hold_the_old_one() {
    let registry = ArrangementSharingRegistry::new();
    let (base, reexport) = (GlobalId::User(1), GlobalId::User(2));
    timely::execute_directly(move |worker| {
        let (_base_token, mut base_traces) = publish_empty(worker, &registry, base, 0);
        let first = publish_reexport(worker, &registry, reexport, 1, &base_traces);
        let old = registry.slot(reexport, 1).expect("published");
        let mut old_reader = old.oks.shared.reader_at_least(&as_of_min());
        drop(first);
        assert!(
            !old.is_frozen(),
            "the base trace still backs the re-export's points after its token dropped"
        );

        let _second = publish_reexport(worker, &registry, reexport, 2, &base_traces);
        let new = registry.slot(reexport, 2).expect("published");
        assert!(
            !Arc::ptr_eq(&old, &new),
            "the new incarnation has its own slot"
        );
        assert!(!new.is_frozen());

        let (oks_input, errs_input) = &mut base_traces.2;
        oks_input.advance_to(Timestamp::from(3_u64));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(3_u64));
        errs_input.flush();
        for _ in 0..16 {
            worker.step();
        }
        let mut upper = Antichain::new();
        old_reader.read_upper(&mut upper);
        assert_eq!(
            upper,
            Antichain::from_elem(Timestamp::from(3_u64)),
            "the old point keeps advancing with the base trace"
        );
        assert_eq!(new.oks.shared.upper(), upper);
        drop((old_reader, old, new));
        drop(base_traces);
    });
}

/// The readers through which the interactive runtime holds and reads incarnation 0 of `id` at
/// `as_of`, see [`ArrangementSharingRegistry::peer_readers_at`].
fn peer_readers(
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    as_of: &Antichain<Timestamp>,
) -> (PeerOksReader, PeerErrsReader) {
    registry.peer_readers_at(id, 0, as_of)
}

/// Consolidates a captured `(Row, Timestamp, Diff)` stream per `(row, time)`, dropping entries
/// whose accumulated diff is zero, and returns them sorted. Shared by the assertions below.
fn consolidate_capture(
    rx: mpsc::Receiver<
        timely::dataflow::operators::capture::Event<Timestamp, Vec<(Row, Timestamp, Diff)>>,
    >,
) -> Vec<(Row, Timestamp, Diff)> {
    let got: Vec<(Row, Timestamp, Diff)> = rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for (row, time, diff) in got {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
    }
    consolidated
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((row, t), d)| (row, t, d))
        .collect()
}

/// Exercises [`ArrangementFlavor::Trace`] over a maintenance-published index imported into the
/// interactive runtime *as an arrangement*.
///
/// Two `RowRow` indexes are published, imported through `OksTrace::import_frontier_core` as a
/// static `as_of` snapshot, entered into a region, and wrapped in `ArrangementFlavor::Trace`,
/// exactly as `import_index` does with its `.enter(self.scope)`. Because the import is a snapshot at `as_of`, every update is coalesced
/// to `as_of`, so key 1's insert and retraction cancel. The flavor is then consumed two ways,
/// standing in for the two downstream operator families that matter:
///
/// * REDUCE input surface: `ArrangementFlavor::flat_map_ok` reconstructs rows through the
///   render's generic arrangement body, the same surface a reduce is fed from. The
///   reconstructed `(key, value)` rows must equal the published rows coalesced at `as_of`.
/// * JOIN surface: the two flavors' arrangements are joined with `join_core`, the differential
///   surface the linear join's `DifferentialDataflow` path calls. The output must equal the
///   direct join.
///
/// Both consume the imported shared arrangement AS an arrangement, never re-deriving it from a
/// collection.
#[mz_ore::test]
fn shared_trace_import_feeds_join_and_reduce() {
    let id_a = GlobalId::User(1);
    let id_b = GlobalId::User(2);

    // Same inputs as `join_over_imported_arrangements_matches_direct`: key 1 inserted then
    // retracted, plus keys 2 and 3, joined against one value per key.
    let a: Vec<Update> = vec![
        (1, "a", 0, 1),
        (2, "b", 0, 1),
        (3, "c", 1, 1),
        (1, "a", 2, -1),
    ];
    let b: Vec<Update> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
    let seal = 3;
    // Read as of `seal - 1`, one tick below the sealed upper `{seal}`: `import_frontier_core`
    // emits only once `upper` is strictly beyond `as_of` (as `snapshot_at` does). All input
    // times (0, 1, 2) are at or below `as_of`, so they coalesce to it and key 1 cancels.
    let as_of_ts = Timestamp::from(seal - 1);

    // The interactive import is a static snapshot at `as_of`, so every update is coalesced to
    // `as_of`: all times advance to `as_of_ts` and cancel there. Key 1's insert and retraction
    // therefore net to zero, so it appears in neither the join nor the reduce output.
    let coalesce_at = |rows: Vec<(Row, Timestamp, Diff)>| -> Vec<(Row, Timestamp, Diff)> {
        let mut out: BTreeMap<Row, Diff> = BTreeMap::new();
        for (row, _time, diff) in rows {
            *out.entry(row).or_insert(Diff::ZERO) += diff;
        }
        let mut v: Vec<_> = out
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|(row, d)| (row, as_of_ts, d))
            .collect();
        v.sort();
        v
    };

    let expected_join_rows = coalesce_at(expected_join(&a, &b));

    // Reduce-surface oracle: `a`'s updates coalesced at `as_of` into the two-column
    // `(key, value)` rows that `flat_map_ok` reconstructs.
    let expected_reduce_rows = coalesce_at(
        a.iter()
            .map(|&(k, v, t, d)| {
                (
                    Row::pack_slice(&[Datum::Int64(k), Datum::String(v)]),
                    Timestamp::from(t),
                    Diff::from(d),
                )
            })
            .collect(),
    );

    let (join_tx, join_rx) = mpsc::channel();
    let (reduce_tx, reduce_rx) = mpsc::channel();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();

        // Maintenance side: publish both indexes, sealing several batches each.
        let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
        let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);

        let (oks_a, errs_a) = peer_readers(&registry, id_a, &Antichain::from_elem(as_of_ts));
        let (oks_b, errs_b) = peer_readers(&registry, id_b, &Antichain::from_elem(as_of_ts));

        let join_probe = ProbeHandle::new();
        let reduce_probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            // Import each index as a static snapshot at `as_of`, with no upper suppression, the
            // interactive single-time read path (`import_index`).
            let as_of = Antichain::from_elem(as_of_ts);
            let until = Antichain::new();
            let (arr_a, _) = OksTrace::Shared(oks_a.clone().into()).import_frontier_core(
                scope.clone(),
                "import A",
                as_of.clone(),
                until.clone(),
            );
            let (err_a, _) = ErrsTrace::Shared(errs_a.clone().into()).import_frontier_core(
                scope.clone(),
                "import A errs",
                as_of.clone(),
                until.clone(),
            );
            let (arr_b, _) = OksTrace::Shared(oks_b.clone().into()).import_frontier_core(
                scope.clone(),
                "import B",
                as_of.clone(),
                until.clone(),
            );
            let (err_b, _) = ErrsTrace::Shared(errs_b.clone().into()).import_frontier_core(
                scope.clone(),
                "import B errs",
                as_of,
                until,
            );

            scope.region_named("SharedImport", |inner| {
                // Enter the region and wrap as `Trace`, mirroring `import_index`.
                let flavor_a =
                    ArrangementFlavor::Trace(id_a, arr_a.enter(inner), err_a.enter(inner));
                let flavor_b =
                    ArrangementFlavor::Trace(id_b, arr_b.enter(inner), err_b.enter(inner));

                // REDUCE surface: reconstruct A's rows through the flavor's generic body.
                let (oks_stream, _errs_coll) = flavor_a.flat_map_ok::<CapacityContainerBuilder<
                    Vec<(Row, Timestamp, Diff)>,
                >, _>(None, usize::MAX, {
                    let mut row_buf = Row::default();
                    move |borrow, time, diff, session| {
                        row_buf.packer().extend(borrow.iter());
                        session.give((row_buf.clone(), time, diff));
                        1
                    }
                });
                oks_stream
                    .probe_with(&reduce_probe)
                    .capture_into(reduce_tx.clone());

                // JOIN surface: join the two flavors' arrangements. Extracting them by matching
                // the variant proves the flavor holds real arrangements the join consumes.
                let (join_a, join_b) = match (&flavor_a, &flavor_b) {
                    (ArrangementFlavor::Trace(_, a, _), ArrangementFlavor::Trace(_, b, _)) => {
                        (a.clone(), b.clone())
                    }
                    _ => unreachable!("both flavors constructed as `Trace` above"),
                };
                let joined = join_a.join_core(join_b, |key, v1, v2| {
                    let row =
                        Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                    Some(row)
                });
                joined
                    .inner
                    .probe_with(&join_probe)
                    .capture_into(join_tx.clone());
            });
        });

        // Step until both operators have sealed through the seal frontier, keeping the
        // publisher inputs alive so their publication points stay open.
        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while join_probe.less_than(&seal_ts) || reduce_probe.less_than(&seal_ts) {
            keep_a.tick(worker);
            keep_b.tick(worker);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "dataflow did not seal through {seal_ts:?}");
        }
        drop_dataflows(worker);
    });

    assert_eq!(
        consolidate_capture(join_rx),
        expected_join_rows,
        "join over the shared import diverged from the direct join"
    );
    assert_eq!(
        consolidate_capture(reduce_rx),
        expected_reduce_rows,
        "flat_map_ok over the shared import diverged from the published rows"
    );
}

/// A join and a reduce over an arrangement imported at a stale `as_of`, where the publisher's
/// spine has folded the history below it into fewer, larger batches.
///
/// The other join and reduce tests publish four updates and read at `as_of = 0`, so their chains are one batch per time and
/// no merge ever precedes the read time. Here sixteen times are published and the controller then
/// allows compaction to the read time, which is what raises the published `since` and lets the
/// spine fold the batches below it together.
///
/// The test asserts both halves of the shape. A merge must have
/// happened, so the import really does seed from a folded chain. And a batch must straddle the
/// `as_of`, because that is the case being covered: an import does not cut at its `as_of`, it is
/// seeded with the whole chain and wrapped in `TraceFrontier`, which advances times instead of
/// cutting. The join and reduce output is the observable, so a straddling batch mishandled would
/// show up as updates at times not before the cut, double counted.
#[mz_ore::test]
fn stale_as_of_import_over_merged_chain_matches_direct() {
    let id_a = GlobalId::User(1);
    let id_b = GlobalId::User(2);

    // Sixteen distinct times, four keys cycling, so every key accumulates several updates and
    // the publisher seals sixteen batches for the spine to merge.
    let times = 16u64;
    let mut a: Vec<Update> = Vec::new();
    let mut b: Vec<Update> = Vec::new();
    for t in 0..times {
        let key = i64::try_from(t % 4).expect("small") + 1;
        a.push((key, "a", t, 1));
        b.push((key, "x", t, 1));
    }
    // Retract key 1's first insert at a time still below `as_of`, so the stale read must
    // coalesce the pair away rather than report both.
    a.push((1, "a", 3, -1));
    let seal = times;
    // Read from the middle of the history, far enough below the seal that the batches around it
    // have been merged over.
    let as_of_ts = Timestamp::from(times / 2);

    // The import advances times at or below `as_of` up to it and leaves later times alone, so
    // the oracle is the direct computation over the same advanced updates.
    let advance = |updates: &[Update]| -> Vec<Update> {
        updates
            .iter()
            .map(|&(k, v, t, d)| (k, v, t.max(u64::from(as_of_ts)), d))
            .collect()
    };
    let a_advanced = advance(&a);
    let b_advanced = advance(&b);

    let expected_join_rows = expected_join(&a_advanced, &b_advanced);

    // Reduce-surface oracle: A's advanced updates as the two-column `(key, value)` rows that
    // `flat_map_ok` reconstructs, consolidated per `(row, time)`.
    let expected_reduce_rows = {
        let mut out: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
        for &(k, v, t, d) in &a_advanced {
            let row = Row::pack_slice(&[Datum::Int64(k), Datum::String(v)]);
            *out.entry((row, Timestamp::from(t))).or_insert(Diff::ZERO) += Diff::from(d);
        }
        let mut v: Vec<_> = out
            .into_iter()
            .filter(|(_, d)| !d.is_zero())
            .map(|((row, t), d)| (row, t, d))
            .collect();
        v.sort();
        v
    };

    let (join_tx, join_rx) = mpsc::channel();
    let (reduce_tx, reduce_rx) = mpsc::channel();

    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();

        // Publish both indexes to completion BEFORE any importer registers.
        let mut keep_a = publish_join_input(&registry, worker, id_a, &a, seal);
        let mut keep_b = publish_join_input(&registry, worker, id_b, &b, seal);
        for _ in 0..64 {
            keep_a.tick(worker);
            keep_b.tick(worker);
        }

        // The controller allows compaction up to the read time, which the writer applies to the
        // arrangements as `handle_allow_compaction` does through the trace manager. That raises the
        // published `since`, so the spine may coalesce the history below the read time. No importer
        // has registered yet, so nothing holds the spines' physical frontiers down and they are free
        // to fold those batches together. The extra ticks give them activations to do so.
        let allow = Antichain::from_elem(as_of_ts);
        // The peers hold at the read time from before the compaction, as the interactive
        // runtime's peer bundles do. Without them the publishers' replayed holds stay at the
        // minimum.
        let (oks_a, errs_a) = peer_readers(&registry, id_a, &allow);
        let (oks_b, errs_b) = peer_readers(&registry, id_b, &allow);
        keep_a.allow_compaction(&allow);
        keep_b.allow_compaction(&allow);
        for _ in 0..64 {
            keep_a.tick(worker);
            keep_b.tick(worker);
        }

        // First half of the premise: the spine folded batches, so the import seeds from a merged
        // chain rather than from the one-batch-per-time shape the other tests cover. Each
        // published time seals its own `[t, t+1)` batch, so a batch spanning more than one time
        // can only come from a merge.
        //
        // Second half: a batch *does* straddle `as_of`, which is the case this fixture exists to
        // cover. An import does not cut at `as_of`, it is seeded with the whole chain and wrapped
        // in `TraceFrontier`, which advances times instead. So a straddling batch is harmless and
        // the observable is the join and reduce output below, which must still match the direct
        // computation. Asserting the straddle rather than its absence keeps this test as the
        // detector for a publisher that holds physical compaction down collectively again.
        let mut merged = false;
        let mut straddles_as_of = false;
        oks_a.map_spans(|span| {
            let lower = span.lower().elements().first().copied();
            let upper = span.upper().elements().first().copied();
            if let (Some(lower), Some(upper)) = (lower, upper) {
                if upper.saturating_sub(lower) > Timestamp::from(1_u64) {
                    merged = true;
                }
                if lower < as_of_ts && as_of_ts < upper {
                    straddles_as_of = true;
                }
            }
        });
        assert!(
            merged,
            "no published batch spans more than one time; the spine did not merge and the test \
             is not exercising the merged-chain cut"
        );
        assert!(
            straddles_as_of,
            "no published batch straddles as_of {as_of_ts:?}, so this fixture is not reaching \
             the case it exists for: an import whose `as_of` falls inside a batch. If this \
             fires, the publisher has gone back to holding physical compaction down to a \
             collective floor such as the published `since`, which stops the spine merging \
             across `as_of` at all"
        );

        let join_probe = ProbeHandle::new();
        let reduce_probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let as_of = Antichain::from_elem(as_of_ts);
            let until = Antichain::new();
            let (arr_a, _) = OksTrace::Shared(oks_a.clone().into()).import_frontier_core(
                scope.clone(),
                "import A",
                as_of.clone(),
                until.clone(),
            );
            let (err_a, _) = ErrsTrace::Shared(errs_a.clone().into()).import_frontier_core(
                scope.clone(),
                "import A errs",
                as_of.clone(),
                until.clone(),
            );
            let (arr_b, _) = OksTrace::Shared(oks_b.clone().into()).import_frontier_core(
                scope.clone(),
                "import B",
                as_of.clone(),
                until.clone(),
            );
            let (err_b, _) = ErrsTrace::Shared(errs_b.clone().into()).import_frontier_core(
                scope.clone(),
                "import B errs",
                as_of,
                until,
            );

            scope.region_named("SharedImport", |inner| {
                let flavor_a =
                    ArrangementFlavor::Trace(id_a, arr_a.enter(inner), err_a.enter(inner));
                let flavor_b =
                    ArrangementFlavor::Trace(id_b, arr_b.enter(inner), err_b.enter(inner));

                let (oks_stream, _errs_coll) = flavor_a.flat_map_ok::<CapacityContainerBuilder<
                    Vec<(Row, Timestamp, Diff)>,
                >, _>(None, usize::MAX, {
                    let mut row_buf = Row::default();
                    move |borrow, time, diff, session| {
                        row_buf.packer().extend(borrow.iter());
                        session.give((row_buf.clone(), time, diff));
                        1
                    }
                });
                oks_stream
                    .probe_with(&reduce_probe)
                    .capture_into(reduce_tx.clone());

                let (join_a, join_b) = match (&flavor_a, &flavor_b) {
                    (ArrangementFlavor::Trace(_, a, _), ArrangementFlavor::Trace(_, b, _)) => {
                        (a.clone(), b.clone())
                    }
                    _ => unreachable!("both flavors constructed as `Trace` above"),
                };
                let joined = join_a.join_core(join_b, |key, v1, v2| {
                    let row =
                        Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                    Some(row)
                });
                joined
                    .inner
                    .probe_with(&join_probe)
                    .capture_into(join_tx.clone());
            });
        });

        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while join_probe.less_than(&seal_ts) || reduce_probe.less_than(&seal_ts) {
            keep_a.tick(worker);
            keep_b.tick(worker);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "dataflow did not seal through {seal_ts:?}");
        }
        drop_dataflows(worker);
    });

    assert_eq!(
        consolidate_capture(join_rx),
        expected_join_rows,
        "join over a merged chain read at a stale as_of diverged from the direct join"
    );
    assert_eq!(
        consolidate_capture(reduce_rx),
        expected_reduce_rows,
        "flat_map_ok over a merged chain read at a stale as_of diverged from the published rows"
    );
}

/// Renders a dataflow on `worker` that arranges empty `oks` and `errs` and publishes them under
/// `id` at `as_of`, as the incarnation of a maintained index that command `creation` created.
/// Returns the dataflow's index, the publication, and agents that keep the traces alive until
/// dropped.
fn render_incarnation(
    worker: &mut timely::worker::Worker,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    creation: CreationNumber,
    as_of: &Antichain<Timestamp>,
) -> (
    usize,
    PublishToken,
    (
        RowRowAgent<Timestamp, Diff>,
        ErrAgent<Timestamp, Diff>,
        differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
        differential_dataflow::input::InputSession<Timestamp, DataflowErrorSer, Diff>,
    ),
) {
    let index = worker.next_dataflow_index();
    let (token, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
        let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("oks");
        let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection)
            .mz_arrange::<ErrBatcher<_, _>, ErrSpine<_, _>>("errs");
        let check = SlotCheck {
            as_of: as_of.clone(),
            index: None,
        };
        let token = registry.publish(id, creation, check, scope, &oks.trace, &errs.trace);
        (
            token,
            (oks.trace.clone(), errs.trace.clone(), oks_input, errs_input),
        )
    });
    for _ in 0..10 {
        worker.step();
    }
    (index, token, keep)
}

/// A reconnect recreates an index at the same `as_of` while the old incarnation is still live, as
/// when the reading runtime reconciles before the publishing runtime dropped it. The new
/// incarnation publishes into a slot of its own, under its own creation number, and a peer of it
/// binds that slot and leaves the old incarnation's alone.
#[mz_ore::test]
fn a_new_peer_binds_its_own_slot_while_the_old_incarnation_is_live() {
    let registry = ArrangementSharingRegistry::new();
    let id = GlobalId::User(1);
    timely::execute_directly(move |worker| {
        let as_of = as_of_min();
        let (_old_dataflow, _old_token, _old_keep) =
            render_incarnation(worker, &registry, id, 0, &as_of);
        let old = registry.slot(id, 0).expect("published");

        let (_new_dataflow, _new_token, _new_keep) =
            render_incarnation(worker, &registry, id, 1, &as_of);
        let fresh = registry.slot(id, 1).expect("published");
        assert!(!old.is_frozen(), "the old incarnation is still live");
        assert!(
            !Arc::ptr_eq(&fresh, &old),
            "the new incarnation published into the old slot"
        );

        let _peer = registry.peer_bundle_at(id, 1, &as_of);
        assert!(
            fresh.take_hold_pusher().is_none(),
            "the new peer took its own slot's hold link"
        );
        assert!(
            old.take_hold_pusher().is_some(),
            "the new peer took the old incarnation's hold link"
        );
        drop((old, fresh));
    });
}

/// The frontier `{t}`.
fn at(t: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(t))
}

/// Steps `worker` enough times for a hold to travel from the interactive runtime through the hold
/// stream to the published `since`.
fn settle(worker: &mut timely::worker::Worker) {
    for _ in 0..16 {
        worker.step();
    }
}

/// A worker hosting the maintenance runtime's publisher of index `id`, which the test seals to
/// `{11}`, and the interactive runtime's compute state, which records `id` as a peer at the
/// minimum and binds it. Runs `test` with both.
fn with_peer_of_live_publisher(
    test: impl FnOnce(
        &mut timely::worker::Worker,
        &ArrangementSharingRegistry,
        &mut LivePublisher,
        &mut ComputeState,
        &mut ResponseSender,
    ) + Send
    + Sync
    + 'static,
) {
    let id = GlobalId::User(1);
    let (_rt, persist_clients) = test_persist_clients();
    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut compute_state =
            interactive_compute_state(worker, persist_clients, registry.clone());
        let mut progress = maintenance_reporter(&registry);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);
        let mut publisher = LivePublisher::new(worker, &registry, id);
        create_dataflow(
            worker,
            &mut compute_state,
            &mut response_tx,
            maintained_index_dataflow(id, GlobalId::User(2), Timestamp::MIN),
            Some(0),
        );
        apply_and_bind(worker, &mut compute_state, &mut progress, 0);
        publisher.seal_to(worker, Timestamp::from(11_u64));
        test(
            worker,
            &registry,
            &mut publisher,
            &mut compute_state,
            &mut response_tx,
        );
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn until_the_peer_holds_it_maintenance_does_not_compact_a_published_index() {
    let id = GlobalId::User(1);
    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut publisher = LivePublisher::new(worker, &registry, id);
        publisher.seal_to(worker, Timestamp::from(11_u64));
        publisher.allow_compaction(at(10));
        settle(worker);
        assert_eq!(
            registry.published_since(&id),
            as_of_min(),
            "maintenance compacted an index its peer has yet to hold"
        );
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_reader_holds_the_published_trace_at_its_as_of_and_releases_when_the_peer_drops() {
    let id = GlobalId::User(1);
    with_peer_of_live_publisher(
        move |worker, registry, publisher, compute_state, response_tx| {
            // An import at 2, whose trace the test keeps as a consumer of the import would.
            let import = worker.next_dataflow_index();
            let trace = {
                let bundle = compute_state.traces.get_mut(&id).expect("a peer bundle");
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (arranged, _) = bundle.oks_mut().import_frontier_core(
                        scope,
                        "import",
                        at(2),
                        Antichain::new(),
                    );
                    arranged.trace
                })
            };
            publisher.allow_compaction(at(10));
            allow_compaction(worker, compute_state, response_tx, id, at(2));
            settle(worker);
            assert_eq!(registry.published_since(&id), at(2), "the readers hold 2");

            drop(trace);
            worker.drop_dataflow(import);
            allow_compaction(worker, compute_state, response_tx, id, at(5));
            settle(worker);
            assert_eq!(registry.published_since(&id), at(5), "the peer holds 5");

            allow_compaction(worker, compute_state, response_tx, id, Antichain::new());
            settle(worker);
            assert_eq!(
                registry.published_since(&id),
                at(10),
                "a dropped peer leaves maintenance's own compaction"
            );
        },
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn the_hold_gap_measures_the_replayed_reader_hold() {
    let id = GlobalId::User(1);
    with_peer_of_live_publisher(
        move |worker, registry, publisher, compute_state, response_tx| {
            publisher.allow_compaction(at(10));
            allow_compaction(worker, compute_state, response_tx, id, at(2));
            settle(worker);
            assert_eq!(
                registry.hold_gaps(),
                (8, 2),
                "the peer holds oks and errs at 2 against 10"
            );

            allow_compaction(worker, compute_state, response_tx, id, Antichain::new());
            settle(worker);
            assert_eq!(registry.hold_gaps(), (0, 0), "a dropped peer holds nothing");
        },
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn the_replayed_reader_hold_does_not_hold_back_merging() {
    let id = GlobalId::User(1);
    with_peer_of_live_publisher(
        move |worker, registry, publisher, _compute_state, _response_tx| {
            let seals = 64_u64;
            for t in 11..11 + seals {
                publisher.insert(vec![(
                    Row::pack_slice(&[Datum::Int64(i64::try_from(t).expect("small"))]),
                    Row::pack_slice(&[Datum::Int64(0)]),
                )]);
                publisher.seal_to(worker, Timestamp::from(t + 1));
                publisher.maintain();
            }
            settle(worker);
            let slot =
                ArrangementSharingRegistry::newest_slot(&registry.lock(), &id).expect("published");
            assert_eq!(
                slot.oks.shared.physical_holds(),
                at(11 + seals),
                "only the writer holds the chain physically"
            );
            let spans = slot.oks.shared.chain().len();
            assert!(
                spans < usize::try_from(seals).expect("small"),
                "the spine merged nothing: {spans} spans for {seals} seals"
            );
        },
    );
}

/// A padded peer bundle reports the minimum as its `since`, as after a reconnect. An import from it
/// at the minimum must hold at the peer's own hold, which the publisher may already have compacted
/// to, and not below it.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_padded_peer_import_below_the_peer_hold_holds_at_the_peer_hold() {
    let id = GlobalId::User(1);
    let rows = vec![(row(1), row(10)), (row(2), row(20))];
    let expected = expected_rows(&rows);
    let (capture_tx, capture_rx) = mpsc::channel();
    let (_rt, persist_clients) = test_persist_clients();
    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut compute_state =
            interactive_compute_state(worker, persist_clients, registry.clone());
        let mut progress = maintenance_reporter(&registry);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut response_tx = ResponseSender::for_test(tx);
        let mut publisher = LivePublisher::new(worker, &registry, id);
        publisher.insert(rows);
        create_dataflow(
            worker,
            &mut compute_state,
            &mut response_tx,
            maintained_index_dataflow(id, GlobalId::User(2), Timestamp::MIN),
            Some(0),
        );
        apply_and_bind(worker, &mut compute_state, &mut progress, 0);
        publisher.seal_to(worker, Timestamp::from(11_u64));
        publisher.allow_compaction(at(10));
        allow_compaction(worker, &mut compute_state, &mut response_tx, id, at(5));
        settle(worker);
        assert_eq!(registry.published_since(&id), at(5), "the peer holds 5");

        let mut padded = compute_state
            .traces
            .get(&id)
            .expect("a peer bundle")
            .clone()
            .into_padded();
        assert_eq!(
            padded.oks_mut().get_logical_compaction(),
            as_of_min().borrow(),
            "a padded bundle reports the minimum"
        );
        let probe = ProbeHandle::new();
        let import = worker.next_dataflow_index();
        let trace = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, _) = padded.oks_mut().import_frontier_core(
                scope,
                "import",
                as_of_min(),
                Antichain::new(),
            );
            Arranged::<TraceFrontier<OksTrace>>::flat_map_batches(
                oks.stream,
                |k: DatumSeq, v: DatumSeq| {
                    let key = Row::pack_slice(&k.into_iter().collect::<Vec<_>>());
                    let val = Row::pack_slice(&v.into_iter().collect::<Vec<_>>());
                    [(key, val)]
                },
            )
            .inner
            .probe_with(&probe)
            .capture_into(capture_tx);
            oks.trace
        });
        drop(padded);
        let mut steps = 0;
        while probe.less_than(&Timestamp::from(11_u64)) {
            worker.step();
            steps += 1;
            assert!(
                steps < 10_000,
                "the import did not reach the published upper"
            );
        }
        let mut held = trace;
        assert_eq!(
            held.get_logical_compaction(),
            at(5).borrow(),
            "the import's trace holds at the peer's hold"
        );
        settle(worker);
        assert_eq!(
            registry.published_since(&id),
            at(5),
            "the import keeps the peer's hold"
        );
        // An import with an open `until` runs for as long as its dataflow lives.
        drop(held);
        worker.drop_dataflow(import);
    });

    let mut consolidated: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for ((k, v), _time, diff) in capture_rx.extract().into_iter().flat_map(|(_, data)| data) {
        *consolidated.entry((k, v)).or_insert(Diff::ZERO) += diff;
    }
    let found: Vec<_> = consolidated
        .into_iter()
        .filter(|(_, diff)| !diff.is_zero())
        .map(|(row, diff)| {
            assert_eq!(diff, Diff::ONE, "{row:?} counted {diff:?} times");
            row
        })
        .collect();
    assert_eq!(found, expected);
}

/// A slot has one peer. A second one, which only a divergence between the runtimes asks for, is
/// reported and reads nothing, instead of reading without a hold.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_second_peer_of_a_slot_stays_unbound() {
    let id = GlobalId::User(1);
    timely::execute_directly(move |worker| {
        let registry = ArrangementSharingRegistry::new();
        let mut publisher = LivePublisher::new(worker, &registry, id);
        publisher.seal_to(worker, Timestamp::from(3_u64));
        let slot = || registry.slot(id, 0).expect("published");
        let mut first = PeerIndex::new(0, slot().check.clone());
        let mut first_bundle = first.bundle();
        first.bind(id, slot());
        let mut upper = Antichain::new();
        first_bundle.oks_mut().read_upper(&mut upper);
        assert_eq!(upper, at(3), "the first peer reads the publication");

        let mut second = PeerIndex::new(0, slot().check.clone());
        let mut second_bundle = second.bundle();
        if mz_ore::assert::soft_assertions_enabled() {
            let bind = mz_ore::panic::catch_unwind_str(std::panic::AssertUnwindSafe(|| {
                second.bind(id, slot())
            }));
            let message = bind.expect_err("a second peer is a soft panic");
            assert!(
                message.contains("a second peer"),
                "unexpected panic: {message}"
            );
        } else {
            second.bind(id, slot());
        }
        assert!(!second.is_bound(), "the second peer bound");
        for (half, trace_upper) in [
            ("oks", {
                second_bundle.oks_mut().read_upper(&mut upper);
                upper.clone()
            }),
            ("errs", {
                second_bundle.errs_mut().read_upper(&mut upper);
                upper.clone()
            }),
        ] {
            assert_eq!(
                trace_upper,
                as_of_min(),
                "the second peer's {half} reads past an unbound binding"
            );
        }
    });
}
