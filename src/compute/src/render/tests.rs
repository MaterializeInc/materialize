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
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_row_spine::{DatumSeq, RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;
use timely::PartialOrder;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::Antichain;

use crate::arrangement::manager::{ErrsTrace, OksTrace};
use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::shared_trace::adopt_trace;
use crate::shared_trace::tests::{SharedReaderExt, drop_dataflows};
use crate::sharing::{ArrangementSharingRegistry, SharedIndexArrangement};
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowAgent, RowRowSpine};

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
/// worker to process on later steps, mirroring `sharing.rs`'s `publish_index_into`. Returns the
/// trace agents: the point closes with the trace, and the trace lives as long as an agent does, so
/// the caller keeps them for as long as it reads.
fn publish_index(
    scope: Scope<'_, Timestamp>,
    registry: &ArrangementSharingRegistry,
    id: GlobalId,
    rows: Vec<(Row, Row)>,
) -> (
    RowRowAgent<Timestamp, Diff>,
    (
        crate::typedefs::ErrAgent<Timestamp, Diff>,
        std::sync::Arc<SharedIndexArrangement>,
    ),
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

    let slot = registry.get_or_create(id);
    adopt_trace(&oks.trace, oks.stream.scope().worker(), &slot.oks, || {});
    adopt_trace(&errs.trace, errs.stream.scope().worker(), &slot.errs, || {});
    registry.notify();

    for (k, v) in rows {
        oks_input.update((k, v), Diff::ONE);
    }
    oks_input.advance_to(Timestamp::from(1_u64));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(1_u64));
    errs_input.flush();
    // The slot is returned with the traces, because the publication lasts only while it is held.
    (oks.trace.clone(), (errs.trace.clone(), slot))
}

/// Imports index `id`'s publication into `scope` the way the interactive runtime does, through a
/// peer handle at `as_of`. Returns the slot, which the caller keeps for the life of the import.
fn import_shared<'scope>(
    registry: &ArrangementSharingRegistry,
    scope: Scope<'scope, Timestamp>,
    id: GlobalId,
    as_of: &Antichain<Timestamp>,
    until: &Antichain<Timestamp>,
) -> (
    Arranged<'scope, TraceFrontier<OksTrace>>,
    Arranged<'scope, TraceFrontier<ErrsTrace>>,
    std::sync::Arc<SharedIndexArrangement>,
) {
    let slot = registry.get_or_create(id);
    let (oks, _) = OksTrace::Shared(slot.oks.peer_handle(as_of)).import_frontier_core(
        scope.clone(),
        "Index",
        as_of.clone(),
        until.clone(),
    );
    let (errs, _) = ErrsTrace::Shared(slot.errs.peer_handle(as_of)).import_frontier_core(
        scope,
        "ErrIndex",
        as_of.clone(),
        until.clone(),
    );
    (oks, errs, slot)
}

/// The interactive import path imports a maintenance-published arrangement into a second
/// dataflow as a static `as_of` snapshot via `SharedReader::import_frontier_core`,
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
        let _keep = worker.dataflow::<Timestamp, _, _>(|scope| {
            publish_index(scope, &registry_in, id, rows.clone())
        });

        // Interactive runtime: a temporary dataflow imports the published arrangement via the
        // new path and captures the reconstructed rows.
        let probe = ProbeHandle::new();
        let (mut oks_trace, mut errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            // `until` empty: no upper suppression, so the whole snapshot at `as_of` flows.
            let (oks_arranged, errs_arranged, _slot) = import_shared(
                &registry_in,
                scope.clone(),
                id,
                &as_of_in,
                &Antichain::new(),
            );

            let collected = Arranged::<TraceFrontier<OksTrace>>::flat_map_batches(
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
/// `TraceAgent` clone of the `oks` trace (not a `SharedReader`), so a test can keep
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
    (
        crate::typedefs::ErrAgent<Timestamp, Diff>,
        std::sync::Arc<SharedIndexArrangement>,
    ),
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

    let slot = registry.get_or_create(id);
    adopt_trace(&oks.trace, oks.stream.scope().worker(), &slot.oks, || {});
    adopt_trace(&errs.trace, errs.stream.scope().worker(), &slot.errs, || {});
    registry.notify();

    for (k, v) in rows {
        oks_input.update((k, v), Diff::ONE);
    }
    oks_input.advance_to(Timestamp::from(1_u64));
    oks_input.flush();
    errs_input.advance_to(Timestamp::from(1_u64));
    errs_input.flush();

    // The slot is returned with the traces, because the publication lasts only while it is held.
    (
        oks_input,
        errs_input,
        oks_writer,
        (errs.trace.clone(), slot),
    )
}

/// Feeds `oks_input` a filler update at `at`, advances it to `next`, and steps `worker` a few
/// times, mirroring the `tick` helper in `differential-dataflow`'s own `sharing.rs` test suite.
/// A reader's hold reaches the trace on the arrange operator's next activation, which an idle
/// dataflow never gets, so tests tick after moving one.
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
/// contract one layer down, directly on a bare `SharedReader` with no compute-level
/// wrapping. This test drives the same `ArrangementSharingRegistry::import` primitive that
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
/// made through `SharedReaderExt::snapshot_at` (a real read against the shared trace's actual
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
        let (mut oks_input, _errs_input, mut oks_writer, _errs_keep) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // Interactive runtime: import at `as_of`, exactly as `import_index_shared` does. The read
        // hold is each `Arranged`'s own `trace`, so those are what is kept here. Production keeps
        // them the same way, inside the `CollectionBundle` the import is bound into, which is what
        // lets a consumer downgrade the hold as its frontier advances. The `stream`s are dropped,
        // as a consumer that only needs the trace would.
        let (oks_trace, errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, errs_arranged, _slot) =
                import_shared(&registry, scope.clone(), id, &as_of, &Antichain::new());
            (oks_arranged.trace, errs_arranged.trace)
        });

        // The controller requests compaction well past `as_of`: the writer handle advances, which
        // the trace mirrors into the published `since` at once. The `since` stays pinned to `as_of`
        // here by the live reader hold.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
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
            let (probe_oks, _probe_errs) = registry.handles(&id).expect("still published");
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
        let (released_oks, _released_errs) = registry.handles(&id).expect("still published");
        assert!(
            released_oks.snapshot_at(&as_of_time).is_none(),
            "after the hold drops, the trace must be free to compact past `as_of`"
        );
        drop_dataflows(worker);
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
        let (mut oks_input, _errs_input, _oks_writer, _errs_keep) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        // Build an interactive import whose only consumer is the batch stream, and let every
        // handle it produced go out of scope with the builder, exactly as production does.
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, _errs_arranged, _slot) =
                import_shared(&registry, scope.clone(), id, &as_of, &Antichain::new());
            let collected = Arranged::<TraceFrontier<OksTrace>>::flat_map_batches(
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
            .published_logical_holds(&id)
            .expect("still published");
        assert!(
            !holds.is_empty(),
            "a built import must leave a read hold behind, else the publisher compacts past its \
             as_of as soon as the controller allows it"
        );
        assert!(
            timely::PartialOrder::less_equal(&holds, &as_of),
            "the import's hold must not have released past its own as_of: {holds:?}"
        );
        drop_dataflows(worker);
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
        let (mut oks_input, _errs_input, _w, _errs_keep) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });
        // A reader at the higher as_of. Its handles go out of scope with the builder; the
        // import operator's own hold remains.
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (_o, _e, _slot) =
                import_shared(&registry, scope.clone(), id, &high, &Antichain::new());
        });
        for t in 1..4 {
            tick(
                worker,
                &mut oks_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The writer has compacted nothing, so a read at the lower time is still legal.
        let (probe_oks, _) = registry.handles(&id).expect("published");
        let since = probe_oks.frontiers().0;
        assert!(
            timely::PartialOrder::less_equal(&since, &low),
            "published since {:?} chased the reader's as_of; a legal read at {:?} would be \
             refused even though the controller allowed no compaction",
            since.elements(),
            low.elements()
        );
        drop_dataflows(worker);
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
        let (mut oks_input, _errs_input, _w, _errs_keep) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
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
            let (oks_arranged, _e, _slot) =
                import_shared(&registry, scope.clone(), id, &as_of, &Antichain::new());
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
        drop_dataflows(worker);
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
        let (mut oks_input, _errs_input, mut oks_writer, _errs_keep) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        let (mut oks_trace, mut errs_trace) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks_arranged, errs_arranged, _slot) =
                import_shared(&registry, scope.clone(), id, &as_of, &Antichain::new());
            (oks_arranged.trace, errs_arranged.trace)
        });

        // The controller allows compaction well past `as_of`, and the writer applies it to the
        // trace.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
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
            let (probe_oks, _probe_errs) = registry.handles(&id).expect("still published");
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
        let (compacted_oks, _compacted_errs) = registry.handles(&id).expect("still published");
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
        drop_dataflows(worker);
    });
}

/// A peer handle minted below the published `since` holds at that `since` and does not refuse.
///
/// History reduction can replay an index's create with an `as_of` below its published `since`, and
/// the runtime that only holds the index as a peer must not abort over a dataflow it does not
/// render. The `since <= as_of` protocol check belongs to the dataflow that imports the index.
#[mz_ore::test]
fn a_peer_handle_below_the_since_joins_up_to_it() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of = Antichain::from_elem(Timestamp::from(1_u64));
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, mut oks_writer, _errs_keep) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });

        let target = Antichain::from_elem(Timestamp::from(10_u64));
        oks_writer.set_logical_compaction(target.borrow());
        oks_writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(5_u64),
            Timestamp::from(6_u64),
        );

        let mut bundle = registry.peer_bundle(id, &as_of);
        assert!(
            PartialOrder::less_than(&as_of.borrow(), &bundle.oks_mut().get_logical_compaction()),
            "the hold sits at the published since, above the requested as_of"
        );
        drop(bundle);
        drop_dataflows(worker);
    });
}

/// A peer bundle keeps `as_of` importable while the importing runtime is behind.
///
/// That runtime records the index as a peer from the index's own create, holding at its `as_of`.
/// The controller can then create a dataflow over the index, drop it (a cancelled peek releases its
/// read hold), and allow compaction, all before the importing runtime has applied the create. No
/// reader hold pins the arrangement then, and the writer alone would let the publisher compact
/// straight past the `as_of` the queued create is about to read at.
#[mz_ore::test]
fn a_peer_bundle_pins_until_the_importing_runtime_applies() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of_time = Timestamp::from(1_u64);
    let as_of = Antichain::from_elem(as_of_time);
    let registry = ArrangementSharingRegistry::new();

    timely::execute_directly(move |worker| {
        let (mut oks_input, _errs_input, mut oks_writer, _errs_keep) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                publish_index_with_writer(scope, &registry, id, rows.clone())
            });
        let mut peer = registry.peer_bundle(id, &as_of);

        // The maintenance runtime applies `AllowCompaction(10)` in full: the writer floor moves and
        // its own trace handle compacts. The importing runtime has not applied its copy of that
        // command, so its peer hold does not move.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
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
            let (oks_arranged, errs_arranged, _slot) =
                import_shared(&registry, scope.clone(), id, &as_of, &Antichain::new());
            (oks_arranged.trace, errs_arranged.trace)
        });

        // Scoped: the probe registers a hold of its own at the current `since`, which would pin the
        // arrangement at `as_of` and make the release assertion below pass for the wrong reason.
        {
            let (probe_oks, _probe_errs) = registry.handles(&id).expect("still published");
            assert!(
                probe_oks.snapshot_at(&as_of_time).is_some(),
                "the peer hold must keep `as_of` readable while the importing runtime is behind"
            );
        }

        // Once that runtime applies the compaction, its peer hold moves and the bound lifts.
        peer.oks_mut().set_logical_compaction(target.borrow());
        peer.errs_mut().set_logical_compaction(target.borrow());
        drop((oks_trace, errs_trace));
        tick(
            worker,
            &mut oks_input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );
        let (released_oks, _released_errs) = registry.handles(&id).expect("still published");
        assert!(
            released_oks.snapshot_at(&as_of_time).is_none(),
            "with the peer hold advanced and no reader left, the arrangement must compact"
        );
        drop(peer);
        drop_dataflows(worker);
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn a_peer_bundle_holds_logically_and_its_import_reads_the_rows() {
    let id = GlobalId::User(1);
    let rows = test_rows();
    let as_of = Antichain::from_elem(Timestamp::from(0_u64));
    let registry = ArrangementSharingRegistry::new();
    let (capture_tx, capture_rx) = mpsc::channel();
    let registry_in = registry.clone();

    timely::execute_directly(move |worker| {
        let _keep = worker.dataflow::<Timestamp, _, _>(|scope| {
            publish_index(scope, &registry_in, id, rows.clone())
        });

        let mut bundle = registry_in.peer_bundle(id, &as_of);
        assert_eq!(
            bundle.oks_mut().get_physical_compaction(),
            Antichain::new().borrow(),
            "a peer bundle holds nothing physically, so the publisher keeps merging"
        );
        assert!(
            bundle.local().is_none(),
            "a peer bundle is not a trace this runtime maintains"
        );

        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let (oks, _button) = bundle.oks_mut().import_frontier_core(
                scope.clone(),
                "Index",
                as_of.clone(),
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
            .capture_into(capture_tx.clone());
        });
        let sealed = Timestamp::from(1_u64);
        let mut steps = 0;
        while probe.less_than(&sealed) {
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "the import did not seal");
        }
        drop(bundle);
        drop_dataflows(worker);
    });

    let mut found: Vec<(Row, Row)> = capture_rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .map(|((k, v), _t, _d)| (k, v))
        .collect();
    found.sort();
    let mut expected = test_rows();
    expected.sort();
    assert_eq!(found, expected);
}
