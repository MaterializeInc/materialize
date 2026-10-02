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
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use mz_repr::{Datum, Row};
use mz_row_spine::{RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::progress::Antichain;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::context::ArrangementFlavor;
use crate::render::errors::DataflowErrorSer;
use crate::shared_trace::adopt_trace;
use crate::shared_trace::tests::{SharedReaderExt, drop_dataflows};
use crate::typedefs::{ErrBatcher, ErrBuilder};

use super::*;

/// Builds a tiny dataflow that arranges `rows` into a `RowRow` `oks` arrangement and an empty
/// `errs` arrangement, publishes both, and returns a registry that holds them under `id`, with the
/// token that keeps the publication. The dataflow runs to completion inside `execute_directly`.
/// The published chain outlives the worker through its `Arc`s, and nothing compacts the trace, so
/// the snapshot reads below observe the sealed contents even after the publishing worker has torn
/// down.
fn publish_index(id: GlobalId, rows: Vec<(Row, Row)>) -> (ArrangementSharingRegistry, Publication) {
    let registry = ArrangementSharingRegistry::new();
    let token = publish_index_into(&registry, id, rows);
    (registry, token)
}

/// Like `publish_index`, but publishes into the given `registry` instead of a fresh one, so a
/// caller can hand the same registry to a concurrent reader before publication happens.
///
/// Routes through [`ArrangementSharingRegistry::publish`], the path the maintenance render side
/// uses, so whatever slot already exists for `id` is the one that gets filled.
fn publish_index_into(
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) -> Publication {
    // The publishing dataflow runs to completion and drops its trace, which releases the writer's
    // compaction. A peer's hold is what keeps the published `since` at the minimum after that.
    let slot = registry.get_or_create(id);
    let minimum = Antichain::from_elem(Timestamp::MIN);
    let holds = (
        slot.oks.peer_handle(&minimum),
        slot.errs.peer_handle(&minimum),
    );
    let registry_in = registry.clone();
    let token = timely::execute_directly(move |worker| {
        // The trace lives as long as an agent does, and the point closes when it drops, so the
        // agents must outlive the stepping that seals the batches. Production keeps them in the
        // trace manager. `execute_directly` steps only after this closure returns, so step here.
        let (keep, token) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
            let oks = oks_collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("test oks");

            let (mut errs_input, errs_collection) =
                scope.new_collection::<DataflowErrorSer, Diff>();
            let errs = KeyCollection::from(errs_collection).mz_arrange::<
                ColumnationChunker<_>,
                ErrBatcher<_, _>,
                ErrBuilder<_, _>,
                ErrSpine<_, _>,
            >("test errs");

            let token =
                registry_in.publish(id, oks.stream.scope().worker(), &oks.trace, &errs.trace);

            for (k, v) in rows {
                oks_input.update((k, v), Diff::ONE);
            }
            oks_input.advance_to(Timestamp::from(1_u64));
            oks_input.flush();
            errs_input.advance_to(Timestamp::from(1_u64));
            errs_input.flush();
            ((oks.trace.clone(), errs.trace.clone()), token)
        });
        while worker.step() {}
        drop(keep);
        token
    });
    // Held until the publisher has taken the same slot.
    drop(slot);
    Publication {
        _token: token,
        _holds: holds,
    }
}

/// A publication that tests read after its publishing worker has torn down: the publisher's token
/// and a peer's holds at the minimum.
struct Publication {
    _token: UnpublishToken,
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
fn get_or_create_converges_on_one_slot() {
    // A reader and a publisher both touch the same id. Whichever is first creates the
    // point; the second must observe the same Arc, not a second slot, and after
    // adoption the reader sees the published rows.
    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();

    // Reader creates the point first and mints its handle straight off it.
    let slot = registry.get_or_create(id);
    let oks = slot.oks.handle();

    // A second get_or_create call for the same id must return
    // the SAME Arc, not a second, disconnected slot, before we even get to the adopt below.
    let republished = registry.get_or_create(id);
    assert!(Arc::ptr_eq(&slot, &republished));

    // Publisher adopts the same slot and fills it.
    let _token = publish_index_into(&registry, id, test_rows());

    assert_eq!(
        read_rows(&oks, Timestamp::from(0_u64)),
        expected_rows(&test_rows())
    );
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
    let reader = registry.get_or_create(id);

    drop(token);
    assert!(
        Arc::ptr_eq(&registry.get_or_create(id), &reader),
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
fn reexport_publishes_its_own_point_over_the_same_trace() {
    let target = GlobalId::User(1);
    let reexport = GlobalId::User(2);
    let registry = ArrangementSharingRegistry::new();
    registry.attach_reader();
    // A reader bound the re-export's id first on this worker. Publishing backs its point the same
    // way as a point the publisher creates.
    let _reader_slot = registry.get_or_create(reexport);
    // A peer's holds keep both points readable after the publishing worker has torn down.
    let minimum = Antichain::from_elem(Timestamp::MIN);
    let target_slot = registry.get_or_create(target);
    let _holds = [&target_slot, &_reader_slot].map(|slot| {
        (
            slot.oks.peer_handle(&minimum),
            slot.errs.peer_handle(&minimum),
        )
    });
    let registry_in = registry.clone();
    let (target_token, reexport_token) = timely::execute_directly(move |worker| {
        let (oks, errs, mut oks_input, mut errs_input, target_token) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                let (oks_input, oks) = scope.new_collection::<(Row, Row), Diff>();
                let oks = oks.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test oks");
                let (errs_input, errs) = scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test errs");
                let token = registry_in.publish(
                    target,
                    oks.stream.scope().worker(),
                    &oks.trace,
                    &errs.trace,
                );
                (oks.trace, errs.trace, oks_input, errs_input, token)
            });

        // The re-export's dataflow must build the same graph on every worker, so publishing the
        // shared traces under its id builds nothing.
        let before = worker.peek_identifier();
        worker.dataflow::<Timestamp, _, _>(|_| {});
        let empty = worker.peek_identifier() - before;
        let before = worker.peek_identifier();
        let reexport_token = worker.dataflow::<Timestamp, _, _>(|scope| {
            registry_in.publish(reexport, scope.worker(), &oks, &errs)
        });
        assert_eq!(worker.peek_identifier() - before, empty);

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
        (target_token, reexport_token)
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
    let target_hold = registry.get_or_create(target).oks.peer_handle(&at(5));
    let reexport_hold = registry.get_or_create(reexport).oks.peer_handle(&at(10));
    drop(_holds);
    assert_eq!(registry.published_logical_holds(&target), Some(at(5)));
    assert_eq!(registry.published_logical_holds(&reexport), Some(at(10)));
    drop((target_hold, reexport_hold));

    // Dropping the index the re-export re-exports leaves the re-export's point readable.
    drop((target_token, target_slot));
    assert!(registry.handles(&target).is_none());
    assert!(registry.handles(&reexport).is_some());
    drop(reexport_token);
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
    let _token = publish_index_into(&registry, id, rows.clone());

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
) -> impl FnMut(&mut timely::worker::Worker) + use<> {
    let registry_in = registry.clone();
    let updates = updates.to_vec();

    let (mut oks_input, mut errs_input, keep) = worker.dataflow::<Timestamp, _, _>(move |scope| {
        let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
        let oks = oks_collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >("input oks");

        let (errs_input, errs_collection) = scope.new_collection::<DataflowErrorSer, Diff>();
        let errs = KeyCollection::from(errs_collection).mz_arrange::<
            ColumnationChunker<_>,
            ErrBatcher<_, _>,
            ErrBuilder<_, _>,
            ErrSpine<_, _>,
        >("input errs");

        let token = registry_in.publish(id, oks.stream.scope().worker(), &oks.trace, &errs.trace);
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

    // Return a closure that keeps the input handles and the trace agents alive and continues
    // stepping. Dropping the handles would drop the inputs and let the dataflow drain to the empty
    // frontier, and dropping the agents would drop the trace, either closing the publication before
    // the importer has read it.
    //
    // Each call also advances the inputs to a fresh filler time, so the arrange operator keeps
    // activating the way a live index's does in production. The filler times carry no updates, so
    // they add empty seal-only batches and advance `upper` without changing any accumulation.
    let mut filler = seal;
    move |worker: &mut timely::worker::Worker| {
        let _keep = &keep;
        filler += 1;
        oks_input.advance_to(Timestamp::from(filler));
        oks_input.flush();
        errs_input.advance_to(Timestamp::from(filler));
        errs_input.flush();
        worker.step();
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
        // earliest real time in either input, so no update coalesces; `until = seal` keeps every
        // distinct time in `[0, seal)` visible.
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
            keep_a(worker);
            keep_b(worker);
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
        let oks = oks_collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >("adopt oks");
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

/// A differential join whose input trace is a PLACEHOLDER at construction fills correctly once
/// the point is adopted in place.
///
/// This reproduces the command-arrival-order hazard directly. `id_a` is imported and joined
/// BEFORE any publisher for it exists: the interactive side mints a point, takes a handle,
/// imports it, and builds `join_core` over the EMPTY point, capturing the trace by value at
/// construction. `id_b` is published normally as an already-materialized co-input.
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

        // A: a PLACEHOLDER, created before any publisher exists. Mint its reader handle now.
        let point_a: Published<RowRowSpine<Timestamp, Diff>> = Published::new();
        let oks_a = point_a.handle();

        // Interactive side: import both as arrangements and join them. A is imported over the
        // EMPTY point. `join_core` captures `arr_a.trace` by value here, before A has any
        // publisher. This is exactly the construction-time capture that late-binding import must
        // survive. `as_of = 0` matches the earliest real time in either input, so no update
        // coalesces; `until = seal` keeps every distinct time in `[0, seal)` visible.
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
            keep_b(worker);
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
            keep_b(worker);
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
/// shared chain under the lock), announces the sealed time, and blocks; the reader then reads
/// `read_upper` on its bare handle and must see exactly that frontier before acking. The reader
/// never builds a dataflow, so this proves `read_upper` reflects the publisher-refreshed chain
/// directly, not a locally-drained copy.
#[mz_ore::test]
fn bare_handle_read_upper_advances_cross_thread() {
    use differential_dataflow::trace::TraceReader;

    let id = GlobalId::User(1);
    let registry = ArrangementSharingRegistry::new();
    let publisher_registry = registry.clone();

    // Handshake channels: publisher -> reader announces each sealed time; reader -> publisher
    // acks so the publisher advances only after the reader has observed the current seal.
    let (tick_tx, tick_rx) = mpsc::channel::<u64>();
    let (ack_tx, ack_rx) = mpsc::channel::<()>();

    let seals: Vec<u64> = vec![1, 2, 3, 4, 5];
    let publisher_seals = seals.clone();

    // `execute_directly` requires a `Send + Sync` closure, but `mpsc` endpoints are not `Sync`.
    // A `Mutex` makes them `Sync`; the single publisher worker is the only user.
    let tick_tx = std::sync::Mutex::new(tick_tx);
    let ack_rx = std::sync::Mutex::new(ack_rx);

    let publisher = thread::spawn(move || {
        timely::execute_directly(move |worker| {
            let (mut oks_input, mut errs_input, _keep) =
                worker.dataflow::<Timestamp, _, _>(|scope| {
                    let (oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                    let oks = oks_collection.mz_arrange::<
                        ColumnationChunker<_>,
                        RowRowBatcher<_, _>,
                        RowRowBuilder<_, _>,
                        RowRowSpine<_, _>,
                    >("spike oks");

                    let (errs_input, errs_collection) =
                        scope.new_collection::<DataflowErrorSer, Diff>();
                    let errs = KeyCollection::from(errs_collection).mz_arrange::<
                        ColumnationChunker<_>,
                        ErrBatcher<_, _>,
                        ErrBuilder<_, _>,
                        ErrSpine<_, _>,
                    >("spike errs");

                    let slot = publisher_registry.get_or_create(id);
                    adopt_trace(&oks.trace, oks.stream.scope().worker(), &slot.oks, || {});
                    adopt_trace(&errs.trace, errs.stream.scope().worker(), &slot.errs, || {});
                    publisher_registry.notify();
                    // The slot is held here for the publisher's life, as `publish`'s token does.
                    (
                        oks_input,
                        errs_input,
                        (oks.trace.clone(), errs.trace.clone(), slot),
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
        // the lock during its `step` before announcing `t`, so a single read suffices; a short
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

/// Exercises [`ArrangementFlavor::SharedTrace`], the render variant that carries a
/// maintenance-published index imported into the interactive runtime *as an arrangement*.
///
/// Two `RowRow` indexes are published, imported through `SharedTraceHandle::import_snapshot_at`
/// as a static `as_of` snapshot, entered into a region, and wrapped in
/// `ArrangementFlavor::SharedTrace`, exactly as `import_index_shared` does with its
/// `.enter(self.scope)`. Because the import is a snapshot at `as_of`, every update is coalesced
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
/// collection. That is the property the `SharedTrace` variant exists to preserve, and the
/// property the prior `CollectionBundle::from_collections` degradation broke.
#[mz_ore::test]
fn shared_trace_flavor_feeds_join_and_reduce() {
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
    // Read as of `seal - 1`, one tick below the sealed upper `{seal}`: `import_snapshot_at`
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

        let (oks_a, errs_a) = registry.handles(&id_a).expect("A published");
        let (oks_b, errs_b) = registry.handles(&id_b).expect("B published");

        let join_probe = ProbeHandle::new();
        let reduce_probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            // Import each index as a static snapshot at `as_of`, with no upper suppression, the
            // interactive single-time read path (`import_index_shared`).
            let as_of = Antichain::from_elem(as_of_ts);
            let until = Antichain::new();
            let arr_a =
                oks_a.import_snapshot_at(scope.clone(), "import A", as_of.clone(), until.clone());
            let err_a = errs_a.import_snapshot_at(
                scope.clone(),
                "import A errs",
                as_of.clone(),
                until.clone(),
            );
            let arr_b =
                oks_b.import_snapshot_at(scope.clone(), "import B", as_of.clone(), until.clone());
            let err_b = errs_b.import_snapshot_at(scope.clone(), "import B errs", as_of, until);

            scope.region_named("SharedTraceFlavor", |inner| {
                // Enter the region and wrap as `SharedTrace`, mirroring `import_index_shared`.
                let flavor_a =
                    ArrangementFlavor::SharedTrace(id_a, arr_a.enter(inner), err_a.enter(inner));
                let flavor_b =
                    ArrangementFlavor::SharedTrace(id_b, arr_b.enter(inner), err_b.enter(inner));

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
                    (
                        ArrangementFlavor::SharedTrace(_, a, _),
                        ArrangementFlavor::SharedTrace(_, b, _),
                    ) => (a.clone(), b.clone()),
                    _ => unreachable!("both flavors constructed as SharedTrace above"),
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
            keep_a(worker);
            keep_b(worker);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "dataflow did not seal through {seal_ts:?}");
        }
        drop_dataflows(worker);
    });

    assert_eq!(
        consolidate_capture(join_rx),
        expected_join_rows,
        "join over SharedTrace flavor diverged from the direct join"
    );
    assert_eq!(
        consolidate_capture(reduce_rx),
        expected_reduce_rows,
        "flat_map_ok over SharedTrace flavor diverged from the published rows"
    );
}

/// A join and a reduce over an arrangement imported at a stale `as_of`, where the publisher's
/// spine has folded the history below it into fewer, larger batches.
///
/// This is the regime production reads in and no other test reaches. The other join and reduce
/// tests publish four updates and read at `as_of = 0`, so their chains are one batch per time and
/// no merge ever precedes the read time. Here sixteen times are published and the controller then
/// allows compaction to the read time, which is what raises the published `since` and lets the
/// spine fold the batches below it together.
///
/// The test asserts both halves of the shape rather than assuming them. A merge must have
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
            keep_a(worker);
            keep_b(worker);
        }

        // The controller allows compaction up to the read time, exactly as
        // `handle_allow_compaction` does in production. That raises the published `since`, so the
        // spine may coalesce the history below the read time. No importer has registered yet, so
        // the publisher's physical target is the chain coverage and the spine is free to fold
        // those batches together. The extra ticks give it activations to do so.
        let allow = Antichain::from_elem(as_of_ts);
        registry.note_allow_compaction(id_a, 0, &allow);
        registry.note_allow_compaction(id_b, 0, &allow);
        for _ in 0..64 {
            keep_a(worker);
            keep_b(worker);
        }

        let (oks_a, errs_a) = registry.handles(&id_a).expect("A published");
        let (oks_b, errs_b) = registry.handles(&id_b).expect("B published");

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
        oks_a.map_batches(|batch| {
            let lower = batch.lower().elements().first().copied();
            let upper = batch.upper().elements().first().copied();
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
            let arr_a =
                oks_a.import_snapshot_at(scope.clone(), "import A", as_of.clone(), until.clone());
            let err_a = errs_a.import_snapshot_at(
                scope.clone(),
                "import A errs",
                as_of.clone(),
                until.clone(),
            );
            let arr_b =
                oks_b.import_snapshot_at(scope.clone(), "import B", as_of.clone(), until.clone());
            let err_b = errs_b.import_snapshot_at(scope.clone(), "import B errs", as_of, until);

            scope.region_named("SharedTraceFlavor", |inner| {
                let flavor_a =
                    ArrangementFlavor::SharedTrace(id_a, arr_a.enter(inner), err_a.enter(inner));
                let flavor_b =
                    ArrangementFlavor::SharedTrace(id_b, arr_b.enter(inner), err_b.enter(inner));

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
                    (
                        ArrangementFlavor::SharedTrace(_, a, _),
                        ArrangementFlavor::SharedTrace(_, b, _),
                    ) => (a.clone(), b.clone()),
                    _ => unreachable!("both flavors constructed as SharedTrace above"),
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
            keep_a(worker);
            keep_b(worker);
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
