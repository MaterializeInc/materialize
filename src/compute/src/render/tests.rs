// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::mpsc;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_row_spine::{DatumSeq, RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::Antichain;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::shared_trace::PublishArrangement;
use crate::shared_trace::SharedOksFrontier;
use crate::sharing::ArrangementSharingRegistry;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowAgent, RowRowSpine};

use super::import_shared_index;
use crate::server::ComputeRuntimeRole;

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

/// Publishes `rows` as a `(RowRow oks, Err errs)` index into `registry` under `id` on worker 0
/// of `scope`. The updates are written at time 0 and sealed by advancing the inputs to 1.
///
/// The `InputSession` handles drop at the end of this call, buffering the sealed updates for the
/// worker to process on later steps, mirroring `sharing.rs`'s `publish_index_into`.
fn publish_index(
    scope: Scope<'_, Timestamp>,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) {
    let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
    let oks = oks_collection.mz_arrange::<
        ColumnationChunker<_>,
        RowRowBatcher<_, _>,
        RowRowBuilder<_, _>,
        RowRowSpine<_, _>,
    >("test oks");

    let (mut errs_input, errs_collection) =
        scope.new_collection::<crate::render::errors::DataflowErrorSer, Diff>();
    let errs = KeyCollection::from(errs_collection)
        .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
            "test errs",
        );

    let slot = registry.get_or_create_placeholder(id, 0, 1);
    PublishArrangement::adopt(&oks, &slot.oks, || {});
    PublishArrangement::adopt(&errs, &slot.errs, || {});
    registry.notify(id, 0);

    for (k, v) in rows {
        oks_input.update((k, v), Diff::ONE);
    }
    oks_input.advance_to(Timestamp::from(1_u64));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(1_u64));
    errs_input.flush();
}

/// The interactive import path imports a maintenance-published arrangement into a second
/// dataflow as a static `as_of` snapshot via `SharedTraceHandle::import_snapshot_at`,
/// reconstructing the same rows, and registers a read hold at the importing dataflow's `as_of`.
#[mz_ore::test]
fn interactive_import_replays_rows_and_holds_at_as_of() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let mut expected: Vec<(Row, Row)> = rows.clone();
    expected.sort();

    // `as_of` beyond the publish-time `since` (0), so a correct hold advance is observable: the
    // freshly minted handle's hold starts at `since` (0) and must be advanced to `as_of` (1).
    let as_of = Antichain::from_elem(Timestamp::from(1_u64));
    let registry = ArrangementSharingRegistry::new();

    let (capture_tx, capture_rx) = mpsc::channel();
    let registry_in = registry.clone();
    let as_of_in = as_of.clone();

    timely::execute_directly(move |worker| {
        // Maintenance runtime: publish the index into the shared registry.
        worker.dataflow::<Timestamp, _, _>(|scope| {
            publish_index(scope, &registry_in, id, rows.clone());
        });

        // Interactive runtime: a temporary dataflow imports the published arrangement via the
        // new path and captures the reconstructed rows.
        let probe = ProbeHandle::new();
        let (mut oks_trace, mut errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            // `until` empty: no upper suppression, so the whole snapshot at `as_of` flows.
            let (oks_arranged, errs_arranged, _slot) = import_shared_index(
                scope.clone(),
                &registry_in,
                id,
                "Index",
                &as_of_in,
                &Antichain::new(),
            );

            let collected = Arranged::<SharedOksFrontier>::flat_map_batches(
                oks_arranged.stream,
                |k: DatumSeq, v: DatumSeq| {
                    let key = Row::pack_slice(&k.into_iter().collect::<Vec<_>>());
                    let val = Row::pack_slice(&v.into_iter().collect::<Vec<_>>());
                    [(key, val)]
                },
            );
            collected.inner.probe_with(&probe).capture_into(capture_tx);
            (oks_arranged.trace, errs_arranged.trace)
        });

        // The read hold is the `Arranged`'s own trace, and it sits at the dataflow's `as_of`, not
        // the publish-time `since`.
        assert_eq!(oks_trace.get_logical_compaction(), as_of_in.borrow());
        assert_eq!(errs_trace.get_logical_compaction(), as_of_in.borrow());

        // Drive both dataflows until the imported-and-reconstructed output has sealed time 0.
        while probe.less_than(&Timestamp::from(1_u64)) {
            worker.step();
        }
    });

    let mut found: Vec<(Row, Row)> = capture_rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .filter(|(_, _, diff)| diff.is_positive())
        .map(|((k, v), _, _)| (k, v))
        .collect();
    found.sort();
    assert_eq!(found, expected);
}

/// Like [`publish_index`], but also returns the writer-side `oks` `InputSession` and a plain
/// `TraceAgent` clone of the `oks` trace (not a `SharedTraceHandle`), so a test can keep
/// publishing after the initial seal and force compaction directly on the writer. Mirrors the
/// `writer` handle in the differential-dataflow primitive's own `import_hold_pins_then_releases`
/// (`differential-dataflow/tests/sharing.rs`), which drives the writer side of the identical
/// pin-then-release scenario one layer down.
fn publish_index_with_writer(
    scope: Scope<'_, Timestamp>,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) -> (
    InputSession<Timestamp, (Row, Row), Diff>,
    InputSession<Timestamp, crate::render::errors::DataflowErrorSer, Diff>,
    RowRowAgent<Timestamp, Diff>,
) {
    let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
    let oks = oks_collection.mz_arrange::<
        ColumnationChunker<_>,
        RowRowBatcher<_, _>,
        RowRowBuilder<_, _>,
        RowRowSpine<_, _>,
    >("test oks");
    let oks_writer = oks.trace.clone();

    let (mut errs_input, errs_collection) =
        scope.new_collection::<crate::render::errors::DataflowErrorSer, Diff>();
    let errs = KeyCollection::from(errs_collection)
        .mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, ErrSpine<_, _>>(
            "test errs",
        );

    let slot = registry.get_or_create_placeholder(id, 0, 1);
    PublishArrangement::adopt(&oks, &slot.oks, || {});
    PublishArrangement::adopt(&errs, &slot.errs, || {});
    registry.notify(id, 0);

    for (k, v) in rows {
        oks_input.update((k, v), Diff::ONE);
    }
    oks_input.advance_to(Timestamp::from(1_u64));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(1_u64));
    errs_input.flush();

    (oks_input, errs_input, oks_writer)
}

/// Feeds `oks_input` a filler update at `at`, advances it to `next`, and steps `worker` a few
/// times, mirroring the `tick` helper in `differential-dataflow`'s own `sharing.rs` test suite.
/// The publisher operator only recomputes its forwarded compaction when a batch runs through
/// it, so a bare `set_logical_compaction`/`set_physical_compaction` call on a writer handle is
/// invisible to the published `since` until the next such tick.
fn tick(
    worker: &mut timely::worker::Worker,
    oks_input: &mut InputSession<Timestamp, (Row, Row), Diff>,
    at: Timestamp,
    next: Timestamp,
) {
    oks_input.advance_to(at);
    oks_input.update(
        (
            Row::pack_slice(&[Datum::Int32(-1)]),
            Row::pack_slice(&[Datum::String("tick")]),
        ),
        Diff::ONE,
    );
    oks_input.advance_to(next);
    oks_input.flush();
    for _ in 0..20 {
        worker.step();
    }
}

/// The interactive import's read hold pins the maintenance trace at `as_of` only while it is
/// alive: once the importing dataflow drops, and with it every registration the import made, the
/// trace is free to compact past `as_of`, which it could not do before the drop.
///
/// Mirrors the differential-dataflow primitive's own `import_hold_pins_then_releases`
/// (`differential-dataflow/tests/sharing.rs`), which demonstrates the identical pin-then-release
/// contract one layer down, directly on a bare `SharedTraceHandle` with no compute-level
/// wrapping. This test drives the same `import_shared_index` primitive that
/// `import_index_shared` calls in production, rather than re-deriving the contract from
/// scratch.
///
/// Staging this end-to-end through the real `ComputeState`/`TraceManager`, as the maintenance
/// `import_index` path would, is not practical in this harness: there is no controller driving
/// frontier advancement, so nothing would ever request compaction past `as_of` for real (the
/// same limitation that keeps the since-gate tests elsewhere in this crate on
/// `execute_directly` plus a directly-driven writer, rather than a full coordinator). The
/// closest observable proxy is used instead: a writer-side compaction request advanced directly
/// on the published trace, exactly as `import_hold_pins_then_releases` does, with the assertion
/// made through `SharedTraceHandle::snapshot_at` (a real read against the shared trace's actual
/// `since`, not a count or a flag).
#[mz_ore::test]
fn interactive_import_hold_releases_on_drop() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of_time = Timestamp::from(1_u64);
    let as_of = Antichain::from_elem(as_of_time);
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        // Maintenance runtime: publish the index, keeping the `oks` `InputSession` (so we can
        // tick the dataflow afterward) and a plain writer trace handle (so we can request
        // compaction on it directly, as a controller would) alive across the whole closure.
        let (mut oks_input, _errs_input, mut oks_writer) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // Interactive runtime: import at `as_of`, exactly as `import_index_shared` does. The read
        // hold is each `Arranged`'s own `trace`, so those are what is kept here. Production keeps
        // them the same way, inside the `CollectionBundle` the import is bound into, which is what
        // lets a consumer downgrade the hold as its frontier advances. The `stream`s are dropped,
        // as a consumer that only needs the trace would.
        let (oks_trace, errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, errs_arranged, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
            (oks_arranged.trace, errs_arranged.trace)
        });

        // The controller requests compaction well past `as_of`, and both runtimes apply it:
        // `note_allow_compaction` forwards the writer floor into the published slot and
        // `note_standing_hold` advances the importing runtime's own position, exactly as
        // `handle_allow_compaction` does on each side. The writer handle advances too so the trace
        // can physically compact. A filler tick reactivates the publisher so it recomputes its
        // forwarded `since` (still pinned to `as_of` here by the live reader hold).
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        registry.note_allow_compaction(id, 0, &target);
        registry.note_standing_hold(id, 0, &target);
        oks_writer.set_logical_compaction(target.borrow());
        oks_writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(5_u64),
            Timestamp::from(6_u64),
        );

        // The live interactive-import hold still pins the trace at `as_of`: a read there still
        // succeeds despite the writer's request. The probe handle is minted only to read and is
        // dropped immediately, so the hold it registers at the current `since` cannot outlive this
        // scope and confound the release assertion below.
        {
            let (probe_oks, _probe_errs) = registry.handles(&id, 0).expect("still published");
            assert!(
                probe_oks.snapshot_at(&as_of_time).is_some(),
                "the live interactive-import hold must keep `as_of` readable"
            );
        }

        // Drop the import's traces, as happens when the interactive dataflow and the
        // `CollectionBundle` holding its arrangements drop. With no reader hold left, the next tick
        // lets the publisher's forwarded `since` follow the writer's request.
        drop(oks_trace);
        drop(errs_trace);
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );

        // The trace compacted past `as_of`: a fresh handle (minted only now, so it introduces no
        // new hold at `as_of`) can no longer read there.
        let (released_oks, _released_errs) = registry.handles(&id, 0).expect("still published");
        assert!(
            released_oks.snapshot_at(&as_of_time).is_none(),
            "after the hold drops, the trace must be free to compact past `as_of`"
        );
    });
}

/// A stream-only import still holds the shared trace after dataflow construction ends.
///
/// This is the regression that matters for anything long-lived on the interactive runtime. The
/// hold that a consumer keeps is the returned `Arranged`'s own trace, and only `mz_join_core`
/// keeps one: it moves its input traces into its operator. `as_collection` and the reduce path
/// take the stream and drop the handle, and the `CollectionBundle` holding it lives in the
/// build-time `Context`, which dies when `build_compute_dataflow` returns. So without a hold owned
/// by the import's own source operator there is no registration left once the dataflow is built,
/// the publisher falls back to the writer-driven frontier, and it compacts straight past the
/// `as_of` the dataflow is still reading at.
///
/// The assertion is on `Published::logical_holds` rather than on a read, because a read cannot
/// tell "a hold exists at `f`" from "no hold exists and the publisher is forwarding `f` from the
/// fallback". Those two look identical from outside and are the whole difference here.
#[mz_ore::test]
fn interactive_import_holds_after_construction() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    // `as_of` beyond the published seal, so the import cannot acknowledge past it and downgrade
    // the hold away. That keeps the assertion about the hold's existence rather than its value.
    let as_of = Antichain::from_elem(Timestamp::from(5_u64));
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, _oks_writer) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // Build an interactive import whose only consumer is the batch stream, and let every
        // handle it produced go out of scope with the builder, exactly as production does.
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, _errs_arranged, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
            let collected = Arranged::<SharedOksFrontier>::flat_map_batches(
                oks_arranged.stream,
                |k: DatumSeq, _v: DatumSeq| [Row::pack_slice(&k.into_iter().collect::<Vec<_>>())],
            );
            collected.inner.probe_with(&probe);
        });

        // Run both dataflows, so the import registers its queue and drains what is published.
        // `tick` advances the input, so each call needs a fresh, larger time. It stops at 3,
        // leaving the published seal below the `as_of` of 5.
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(1_u64),
            Timestamp::from(2_u64),
        );
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(2_u64),
            Timestamp::from(3_u64),
        );

        let holds = registry
            .published_logical_holds(&id, 0)
            .expect("still published");
        assert!(
            !holds.is_empty(),
            "a built import must leave a read hold behind, else the publisher compacts past its \
             as_of as soon as the controller allows it"
        );
        assert!(
            holds
                .iter()
                .all(|hold| timely::PartialOrder::less_equal(hold, &as_of)),
            "the import's hold must not have released past its own as_of: {holds:?}"
        );
    });
}

/// The published `since` must not chase the readers' own holds.
///
/// Before the controller's first `AllowCompaction` there is no writer-driven floor, and if the
/// publisher falls back to its own agent hold it closes a feedback loop: it drives that hold up
/// from the meet of the reader holds every activation, so the published `since` climbs to wherever
/// the readers are. A later read at an earlier time is then refused, and it is a read the
/// controller has allowed nothing against.
#[mz_ore::test]
fn published_since_does_not_chase_reader_holds() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let high = Antichain::from_elem(Timestamp::from(2_u64));
    let low = Antichain::from_elem(Timestamp::from(1_u64));
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, _w) = worker.dataflow::<Timestamp, _, _>(|scope| {
            publish_index_with_writer(scope, &registry, id, rows.clone())
        });
        // A reader at the higher as_of. Its handles go out of scope with the builder; the
        // import operator's own hold remains.
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (_o, _e, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &high,
                &Antichain::new(),
            );
        });
        for t in 1..4 {
            tick(
                worker,
                &mut oks_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // No `note_allow_compaction` has been called: the controller has allowed nothing, so a
        // read at the lower time is still legal.
        let (probe_oks, _) = registry.handles(&id, 0).expect("published");
        let since = probe_oks.frontiers().0;
        assert!(
            timely::PartialOrder::less_equal(&since, &low),
            "published since {:?} chased the reader's as_of; a legal read at {:?} would be \
             refused even though the controller allowed no compaction",
            since.elements(),
            low.elements()
        );
    });
}

/// An import's reported physical compaction must not lead the published chain's coverage.
///
/// `mz_join_core` asserts exactly this at start-up, against the coverage it derives from
/// `map_batches`, and differential's own `join_core` carries the same assert. An `as_of` may
/// legitimately lead the coverage: an import over a placeholder whose publisher has not adopted it
/// yet sees an empty chain, and a read at a timestamp beyond the index's seal leads it too.
/// Reporting the `as_of` here therefore aborts the worker on a correct import, and under shared
/// fate that takes the process with it.
#[mz_ore::test]
fn import_reports_physical_within_chain_coverage() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of = Antichain::from_elem(Timestamp::from(5_u64));
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, _w) = worker.dataflow::<Timestamp, _, _>(|scope| {
            publish_index_with_writer(scope, &registry, id, rows.clone())
        });
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(1_u64),
            Timestamp::from(2_u64),
        );
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(2_u64),
            Timestamp::from(3_u64),
        );

        let mut trace = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, _e, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
            oks_arranged.trace
        });

        // Exactly `mz_join_core`'s start-up computation.
        use differential_dataflow::trace::BatchReader;
        let mut coverage = Antichain::from_elem(Timestamp::MIN);
        trace.map_batches(|b| coverage.clone_from(b.upper()));
        let physical = trace.get_physical_compaction().to_owned();
        assert!(
            timely::PartialOrder::less_equal(&physical, &coverage),
            "mz_join_core would panic: physical {:?} leads coverage {:?}",
            physical.elements(),
            coverage.elements()
        );
    });
}

/// A live import's hold can be downgraded, so the publisher compacts behind a long-lived reader
/// rather than staying pinned at its `as_of` for the reader's whole life.
///
/// This is what a join on the interactive runtime does: `mz_join_core` calls
/// `set_logical_compaction` on each input trace as the other input's frontier advances, and
/// `set_physical_compaction` as it acknowledges batches. An unbounded interactive dataflow that
/// could not downgrade would pin the maintenance index at the `as_of` it started from, so the
/// publisher could never compact for as long as the dataflow ran.
///
/// The hold has to be the `Arranged`'s own trace for this to work. A separate hold token retained
/// beside it would defeat the downgrade entirely, since the publisher forwards the *meet* of the
/// registered holds and a hold nobody downgrades is a floor under every hold that is.
#[mz_ore::test]
fn interactive_import_hold_downgrades_while_live() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of_time = Timestamp::from(1_u64);
    let as_of = Antichain::from_elem(as_of_time);
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, mut oks_writer) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        let (mut oks_trace, mut errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, errs_arranged, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
            (oks_arranged.trace, errs_arranged.trace)
        });

        // The controller allows compaction well past `as_of`, both runtimes apply it, and the
        // writer applies it to the trace.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        registry.note_allow_compaction(id, 0, &target);
        registry.note_standing_hold(id, 0, &target);
        oks_writer.set_logical_compaction(target.borrow());
        oks_writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(5_u64),
            Timestamp::from(6_u64),
        );

        // Still pinned: the import has not downgraded, so `as_of` stays readable.
        {
            let (probe_oks, _probe_errs) = registry.handles(&id, 0).expect("still published");
            assert!(
                probe_oks.snapshot_at(&as_of_time).is_some(),
                "an import that has not downgraded must keep `as_of` readable"
            );
        }

        // The consumer downgrades, as a join does once its other input has advanced. The traces
        // stay alive throughout, which is the point: this is a downgrade, not a release.
        oks_trace.set_logical_compaction(target.borrow());
        oks_trace.set_physical_compaction(target.borrow());
        errs_trace.set_logical_compaction(target.borrow());
        errs_trace.set_physical_compaction(target.borrow());
        assert_eq!(
            oks_trace.get_logical_compaction(),
            target.borrow(),
            "the downgrade must be reflected in what the handle reports holding"
        );
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );

        // The publisher followed the downgrade: `as_of` is no longer readable even though the
        // import is still live and still holding at the downgraded frontier.
        let (compacted_oks, _compacted_errs) = registry.handles(&id, 0).expect("still published");
        assert!(
            compacted_oks.snapshot_at(&as_of_time).is_none(),
            "after the downgrade, the publisher must compact past the original `as_of`"
        );
        assert!(
            compacted_oks
                .snapshot_at(&Timestamp::from(10_u64))
                .is_some(),
            "the downgraded frontier must still be readable"
        );
        drop((oks_trace, errs_trace));
    });
}

/// A published slot's `since` may already sit above the dataflow's requested `as_of` if the
/// controller offered an unreadable `as_of`, a protocol error: `import_shared_index` must
/// panic rather than let the read silently see coalesced data, mirroring the maintenance
/// path's `compaction_frontier` assert in `import_index`.
///
/// Advances the writer's compaction well past `as_of` with no reader hold registered yet, the
/// same `publish_without_readers_does_not_pin_compaction` scenario `shared_trace.rs` covers,
/// so a freshly minted handle's hold starts at the already-advanced `since`. Importing at
/// `as_of` afterward must panic.
#[mz_ore::test]
#[should_panic(expected = "since")]
fn import_asserts_since_at_most_as_of() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of = Antichain::from_elem(Timestamp::from(1_u64));
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, mut oks_writer) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // The controller advances compaction well past `as_of`, with no reader hold registered
        // yet, and both runtimes apply it. The publisher then advances the published `since` past
        // `as_of` on the next tick: no reader hold pins it, and the standing hold has moved with
        // the writer floor.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        registry.note_allow_compaction(id, 0, &target);
        registry.note_standing_hold(id, 0, &target);
        oks_writer.set_logical_compaction(target.borrow());
        oks_writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(5_u64),
            Timestamp::from(6_u64),
        );

        // Importing at `as_of` now finds a `since` already beyond it: the assert must panic.
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let _ = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
        });
    });
}

/// The standing hold keeps `as_of` importable while the importing runtime is behind.
///
/// This is [`import_asserts_since_at_most_as_of`] with one difference: the importing runtime has
/// not applied the controller's compaction. That is the state the two-runtime split makes
/// reachable, and it is not a protocol error. The controller can create a dataflow at `as_of`,
/// drop it (a cancelled peek releases its read hold), and allow compaction, all before the runtime
/// rendering that dataflow has applied the create. From the controller's side nothing is wrong.
/// The create is still queued, so no reader hold exists to pin the arrangement, and the writer
/// floor alone would let the publisher compact straight past the `as_of` the queued create is
/// about to read at.
///
/// Asserting the import *succeeds* is the point. The sibling test asserts the panic that a genuine
/// protocol error produces, so between them a mechanism that pinned nothing, or one that pinned
/// everything, fails one of the two.
#[mz_ore::test]
fn standing_hold_pins_until_the_importing_runtime_applies() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of_time = Timestamp::from(1_u64);
    let as_of = Antichain::from_elem(as_of_time);
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, mut oks_writer) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // The maintenance runtime applies `AllowCompaction(10)` in full: the writer floor moves and
        // its own trace handle compacts. The interactive runtime has not applied the broadcast copy
        // of that command, so its standing hold does not move.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        registry.note_allow_compaction(id, 0, &target);
        oks_writer.set_logical_compaction(target.borrow());
        oks_writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(5_u64),
            Timestamp::from(6_u64),
        );

        // The queued create is now applied. It must import, and the rows it reads at `as_of` must
        // be the ones a read at `as_of` should see rather than a coalesced history.
        let (oks_trace, errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, errs_arranged, _slot) = import_shared_index(
                scope.clone(),
                &registry,
                id,
                "Index",
                &as_of,
                &Antichain::new(),
            );
            (oks_arranged.trace, errs_arranged.trace)
        });

        // Scoped: the probe registers a hold of its own at the current `since`, which would pin the
        // arrangement at `as_of` and make the release assertion below pass for the wrong reason.
        {
            let (probe_oks, _probe_errs) = registry.handles(&id, 0).expect("still published");
            assert!(
                probe_oks.snapshot_at(&as_of_time).is_some(),
                "the standing hold must keep `as_of` readable while the importing runtime is behind"
            );
        }

        // Once that runtime applies the compaction, the bound lifts. The live import's own hold
        // takes over from here, which is what the sibling hold tests cover.
        registry.note_standing_hold(id, 0, &target);
        drop((oks_trace, errs_trace));
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );
        let (released_oks, _released_errs) = registry.handles(&id, 0).expect("still published");
        assert!(
            released_oks.snapshot_at(&as_of_time).is_none(),
            "with the standing hold advanced and no reader left, the arrangement must compact"
        );
    });
}

/// A two-runtime process's maintenance runtime publishes into the sharing registry. Its
/// interactive peer reads only from the registry, so publication is what keeps interactive
/// peeks from blocking until they time out.
#[mz_ore::test]
fn maintenance_role_publishes() {
    assert!(ComputeRuntimeRole::Maintenance.publishes());
}

/// A two-runtime process's interactive runtime publishes its transient query outputs into the
/// sharing registry, so a result peek served from the registry can read the output and receive
/// its seal notifications.
#[mz_ore::test]
fn interactive_role_publishes() {
    assert!(ComputeRuntimeRole::Interactive.publishes());
}

/// The `Solo` (single-runtime) role has no registry peer, so it does not publish.
#[mz_ore::test]
fn solo_role_does_not_publish() {
    assert!(!ComputeRuntimeRole::Solo.publishes());
}
