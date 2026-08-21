// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::input::Input;
use differential_dataflow::trace::Cursor;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;

use crate::extensions::arrange::MzArrange;
use crate::typedefs::RowRowSpine;

use super::*;

/// Adopts a freshly rendered arrangement into a new placeholder, the standalone-primitive
/// counterpart to the maintenance placeholder+adopt path. Creates a [`Published::placeholder`]
/// sized to the arrangement's own scope, installs `arranged`'s publisher into it via
/// [`PublishArrangement::adopt`], and returns the now-backed point.
fn adopt_fresh<Tr>(arranged: &Arranged<'_, TraceAgent<Tr>>) -> Published<Tr>
where
    Tr: differential_dataflow::trace::Trace + 'static,
    Tr::Batch: Send + Sync,
    Tr::Time: Lattice + Clone + Send + Sync,
{
    let published = Published::placeholder(arranged.stream.scope().peers());
    PublishArrangement::adopt(arranged, &published, || {});
    published
}

/// Smoke test: arrange two rows into a `RowRow` spine, publish it through the extension trait,
/// mint a `Send` handle, and read the sealed contents back via `snapshot_at`.
///
/// Publisher and reader share one worker stepped to completion inside `execute_directly`. The
/// returned handle keeps the published chain alive through its `Arc`s, so the snapshot observes
/// the sealed rows even after the publishing worker tears down. This is the single-worker
/// publish + snapshot path; the full cross-thread and import-replay coverage lives in
/// `crate::sharing`.
#[mz_ore::test]
fn publish_then_snapshot_reads_rows() {
    let rows: Vec<(Row, Row)> = vec![
        (
            Row::pack_slice(&[Datum::Int32(1)]),
            Row::pack_slice(&[Datum::String("a")]),
        ),
        (
            Row::pack_slice(&[Datum::Int32(2)]),
            Row::pack_slice(&[Datum::String("b")]),
        ),
    ];
    let expected = {
        let mut e = rows.clone();
        e.sort();
        e
    };

    let handle = timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("smoke oks");
            // The extension trait under test.
            let published = adopt_fresh(&arranged);
            (published, input)
        });

        for (k, v) in rows {
            input.update((k, v), Diff::ONE);
        }
        // Seal the batch at time 0 by advancing the input to 1.
        input.advance_to(Timestamp::from(1_u64));
        input.flush();

        // Mint the handle before dropping the input so the publication point stays open, then
        // step the worker to seal and refresh the published chain.
        let handle = published.handle();
        for _ in 0..32 {
            worker.step();
        }
        drop(input);
        handle
    });

    // Read the rows accumulated at time 0 from the `Send` handle.
    let snapshot = handle
        .snapshot_at(&Timestamp::from(0_u64))
        .expect("snapshot at sealed time");
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

    assert_eq!(found, expected);
}

/// Quiet-seal: an arrangement that receives no data but whose input frontier advances still
/// publishes an advancing `upper`.
///
/// A seal-only advance produces an empty batch that the arrange operator writes to the trace
/// without sending it on the output stream. The publisher reads that batch back through
/// `map_batches`, so the published chain and `upper` reach the seal even though no data ever
/// traveled the stream. Here we advance the input to `1` with no updates and assert the
/// published `upper` passes `0`, so `snapshot_at(0)` returns rather than blocking.
#[mz_ore::test]
fn quiet_seal_advances_upper() {
    let (upper, snapshot_is_some) = timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("quiet oks");
            let published = adopt_fresh(&arranged);
            (published, input)
        });

        // No updates at all. Advance the input to 1 to seal the (empty) batch at time 0.
        input.advance_to(Timestamp::from(1_u64));
        input.flush();

        let handle = published.handle();
        for _ in 0..32 {
            worker.step();
        }
        drop(input);

        // Observe the published seal without blocking, then confirm a sealed-time read serves.
        let (_since, upper) = handle.frontiers();
        let snapshot_is_some = handle.snapshot_at(&Timestamp::from(0_u64)).is_some();
        (upper, snapshot_is_some)
    });

    // `upper` advanced past 0: the quiet seal was published, so a read at 0 is complete.
    assert!(
        !upper.less_equal(&Timestamp::from(0_u64)),
        "published upper stayed pinned at its init value: {upper:?}"
    );
    assert!(snapshot_is_some, "snapshot at a sealed time returned None");
}

/// A worker on one thread publishes an arrangement; a separate thread holds a `Send` handle,
/// blocks in `snapshot_at` for the publication frontier to pass a time, and reads the
/// collection at that time.
///
/// Ported from the differential-dataflow primitive's own `tests/sharing.rs`
/// `snapshot_from_another_thread`. Unlike `crate::sharing`'s cross-runtime coverage, which reads
/// only after the publishing worker has already torn down, this keeps the publisher stepping
/// concurrently on its own thread so the reader genuinely waits for a seal that has not happened
/// yet, rather than observing an already-sealed chain.
#[mz_ore::test]
fn snapshot_at_waits_until_upper_passes_time() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let key = |k: i32| Row::pack_slice(&[Datum::Int32(k)]);
    let val = |v: &str| Row::pack_slice(&[Datum::String(v)]);

    let (handle_tx, handle_rx) = mpsc::channel::<SharedTraceHandle<RowRowSpine<Timestamp, Diff>>>();
    // The reader raises this once it has its snapshot, so the publisher knows it can stop
    // stepping. A retained trace handle keeps the dataflow from quiescing, so the publisher
    // never finishes on its own until this fires.
    let done = Arc::new(AtomicBool::new(false));
    let reader_done = Arc::clone(&done);

    let reader = std::thread::spawn(move || {
        let handle = handle_rx.recv().expect("publisher sends its handle");
        let snapshot = handle
            .snapshot_at(&Timestamp::from(2_u64))
            .expect("publisher does not close before sealing time 2");
        reader_done.store(true, Ordering::SeqCst);
        let (mut cursor, storage) = snapshot.cursor();
        let mut found: Vec<(Row, Row)> = Vec::new();
        while cursor.key_valid(&storage) {
            while cursor.val_valid(&storage) {
                let k = Row::pack_slice(&cursor.key(&storage).into_iter().collect::<Vec<_>>());
                let v = Row::pack_slice(&cursor.val(&storage).into_iter().collect::<Vec<_>>());
                let mut diff = Diff::ZERO;
                cursor.map_times(&storage, |_t, d| diff += d);
                if !diff.is_zero() {
                    found.push((k, v));
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
        found.sort();
        found
    });

    timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("cross-thread oks");
            let published = adopt_fresh(&arranged);
            (published, input)
        });
        handle_tx.send(published.handle()).unwrap();

        // Time 0: (1,"a")+1, (2,"b")+1. Time 1: retract (2,"b"). Time 2: (3,"c")+1.
        input.advance_to(Timestamp::from(0_u64));
        input.update((key(1), val("a")), Diff::ONE);
        input.update((key(2), val("b")), Diff::ONE);
        input.advance_to(Timestamp::from(1_u64));
        input.update((key(2), val("b")), -Diff::ONE);
        input.advance_to(Timestamp::from(2_u64));
        input.update((key(3), val("c")), Diff::ONE);
        input.advance_to(Timestamp::from(3_u64));
        input.flush();

        // Step until the reader has taken its snapshot. The publisher advances `upper` as it
        // steps, which unblocks the reader's `snapshot_at`.
        while !done.load(Ordering::SeqCst) {
            worker.step();
        }
    });

    let got = reader.join().expect("reader thread panicked");
    // As of time 2: (1,"a") present, (2,"b") inserted then retracted, (3,"c") inserted at 2.
    assert_eq!(got, vec![(key(1), val("a")), (key(3), val("c"))]);
}

/// Feeds `input` a fresh update at `at`, advances the frontier to `next`, and steps `worker`,
/// so the publisher operator reactivates and republishes its forwarded compaction from the
/// trace. Mirrors the `tick` helper in the differential-dataflow primitive's own
/// `tests/sharing.rs`: the publisher only recomputes its forwarded compaction when a batch runs
/// through it, so a bare `set_logical_compaction`/`set_physical_compaction` call on a writer
/// handle is invisible until the next such tick.
fn tick(
    worker: &mut timely::worker::Worker,
    input: &mut differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
    at: Timestamp,
    next: Timestamp,
) {
    input.advance_to(at);
    input.update(
        (
            Row::pack_slice(&[Datum::Int32(-1)]),
            Row::pack_slice(&[Datum::String("tick")]),
        ),
        Diff::ONE,
    );
    input.advance_to(next);
    input.flush();
    for _ in 0..20 {
        worker.step();
    }
}

/// Publishing must not pin compaction. With no registered reader holds, as the controller
/// advances its logical compaction (forwarded through `note_writer_logical`) the publisher's own
/// forwarded hold must follow, so the trace actually compacts.
///
/// Exercises the publisher's compaction forwarding with the standing hold as the only accumulated
/// hold, which no other test in this crate or `crate::sharing` covers: `crate::render`'s
/// `interactive_import_hold_releases_on_drop` always has a live reader hold present at some point
/// in the scenario.
#[mz_ore::test]
fn publish_without_readers_does_not_pin_compaction() {
    timely::execute_directly(move |worker| {
        // Keep a writer handle (a plain `TraceAgent` clone) alongside the publication, and mint no
        // `SharedTraceHandle` until after compaction: that keeps `logical_holds` empty, so the
        // publisher has zero registered reader holds throughout.
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("no-readers oks");
            let writer = arranged.trace.clone();
            let published = adopt_fresh(&arranged);
            (writer, published, input)
        });

        // Seed some updates and let the publisher settle.
        for t in 0..5 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The controller requests compaction to 10. `note_writer_logical` records the writer's own
        // frontier (the production path is `handle_allow_compaction` via the registry), and the
        // writer handle advances too so the underlying trace can physically compact. A fresh tick
        // reactivates the publisher so it recomputes what it forwards.
        //
        // The standing hold moves with it, as it does in production once the importing runtime
        // applies the same broadcast command. Without it the target stays bounded at the adoption
        // floor, which is what `standing_hold_holds_since_behind_the_writer` covers.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        published.note_writer_logical(&target);
        published.note_standing_hold(&target);
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        // The published `since` followed the writer to 10: a fresh handle cannot snapshot at the
        // compacted time 5, but can at 10.
        let handle = published.handle();
        assert!(
            handle.snapshot_at(&Timestamp::from(5_u64)).is_none(),
            "snapshot at a compacted time must be rejected (since did not advance)"
        );
        assert!(
            handle.snapshot_at(&Timestamp::from(10_u64)).is_some(),
            "snapshot at the compaction frontier must succeed"
        );
    });
}

/// I1c: a compaction the importing runtime has not applied does not advance the published `since`.
///
/// This is the invariant the two-runtime split loses without a standing hold. The controller's
/// `AllowCompaction` reaches both runtimes, but they drain independently, so the owning runtime can
/// realize a frontier the importing one has not. A dataflow whose `CreateDataflow` is still queued
/// there has registered no reader hold yet, so without the standing hold nothing would stop the
/// publisher following the controller past that dataflow's `as_of`, and it would render against
/// compacted data.
///
/// The third phase is what keeps the bound from being a permanent pin: with no standing hold noted
/// at all, compaction still reaches the frontier the arrangement was adopted at.
#[mz_ore::test]
fn standing_hold_holds_since_behind_the_writer() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let mut arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("standing-hold oks");
            let writer = arranged.trace.clone();
            // Adopt at 3, standing in for a dataflow whose `as_of` is 3: the publisher captures
            // that as the floor it may compact to before any command has been applied anywhere.
            // Set on the arrangement's own agent, not on a temporary clone, whose `Drop` would
            // give the hold straight back before `adopt` clones from it.
            let at_three = Antichain::from_elem(Timestamp::from(3_u64));
            arranged.trace.set_logical_compaction(at_three.borrow());
            let published = adopt_fresh(&arranged);
            (writer, published, input)
        });

        for t in 0..5 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        assert_eq!(
            published.standing_hold(),
            Antichain::from_elem(Timestamp::from(3_u64)),
            "adoption seeds the standing hold at the publisher's own compaction frontier"
        );

        // The maintenance runtime applies `AllowCompaction(10)` and its trace really does compact:
        // both the writer handle and the publisher's writer-driven floor move to 10. The importing
        // runtime has not applied it, so its standing hold stays at 3.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        published.note_writer_logical(&target);
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        // A read at 5 is still admitted, and is still accurate: the trace's real compaction is the
        // meet over its agents, and the publisher's own agent is still held at 3.
        assert!(
            published
                .handle()
                .snapshot_at(&Timestamp::from(5_u64))
                .is_some(),
            "the published since advanced past the importing runtime's applied frontier"
        );

        // Once that runtime applies the same command, the bound lifts and the arrangement compacts.
        published.note_standing_hold(&target);
        tick(
            worker,
            &mut input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );
        assert!(
            published
                .handle()
                .snapshot_at(&Timestamp::from(5_u64))
                .is_none(),
            "the standing hold advanced but the arrangement did not compact"
        );
        assert!(
            published
                .handle()
                .snapshot_at(&Timestamp::from(10_u64))
                .is_some(),
            "compaction overshot the frontier both runtimes have applied"
        );
    });
}

/// `Published::handle_at` mints a hold at the requested `as_of`, and refuses when the published
/// `since` has already passed it.
///
/// Refusing is the whole point: a reader that observed `since`, decided it permitted its `as_of`,
/// and only then advanced a hold would be racing the publisher across three separate acquisitions
/// of the state lock. The mint collapses that to one, so a handle it returns holds a frontier the
/// trace can still serve. A refusal is reported, not degraded, because the controller promises an
/// index's `since` never passes the `as_of` of a dataflow importing it.
#[mz_ore::test]
fn handle_at_mints_at_as_of_or_refuses() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("handle-at oks");
            let writer = arranged.trace.clone();
            let published = adopt_fresh(&arranged);
            (writer, published, input)
        });

        for t in 0..5 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // While `since` is still at the minimum, a mint at any time succeeds and the hold sits
        // exactly where it was asked for, not at `since`.
        let at_three = Antichain::from_elem(Timestamp::from(3_u64));
        let mut hold = published
            .handle_at(&at_three)
            .expect("since is still at the minimum");
        assert_eq!(
            hold.get_logical_compaction().to_owned(),
            at_three,
            "the mint must register at the requested as_of"
        );

        // A setter joins rather than overwrites, so a consumer cannot lower its own hold below a
        // frontier the trace was already told it could compact past, and the getter keeps
        // reporting what is actually held.
        hold.set_logical_compaction(Antichain::from_elem(Timestamp::from(1_u64)).borrow());
        assert_eq!(
            hold.get_logical_compaction().to_owned(),
            at_three,
            "a request below the current hold must not lower it"
        );
        hold.set_logical_compaction(Antichain::from_elem(Timestamp::from(4_u64)).borrow());
        assert_eq!(
            hold.get_logical_compaction().to_owned(),
            Antichain::from_elem(Timestamp::from(4_u64)),
            "a request beyond the current hold must advance it"
        );
        drop(hold);

        // The controller allows compaction to 10 and both runtimes apply it, so the publisher
        // forwards a `since` of 10 on its next activation.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        published.note_writer_logical(&target);
        published.note_standing_hold(&target);
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        assert_eq!(
            published.handle_at(&at_three).err(),
            Some(target.clone()),
            "a mint below the published since must be refused, and report it"
        );
        assert!(
            published.handle_at(&target).is_ok(),
            "a mint at the published since must succeed"
        );
    });
}

/// A consumer forwarding an empty input frontier releases its hold rather than recording an
/// empty one.
///
/// The empty antichain permits compaction everywhere, so a handle that reaches it has released and
/// must stop appearing as a hold. The reduce operator forwards exactly this on every dataflow whose
/// input finishes: `upper_limit` is the join of the input frontiers, and empties when the input
/// does.
#[mz_ore::test]
fn empty_logical_request_releases_the_hold() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("f4 oks");
            let writer = arranged.trace.clone();
            let published = adopt_fresh(&arranged);
            (writer, published, input)
        });
        for t in 0..3 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        let target = Antichain::from_elem(Timestamp::from(2_u64));
        published.note_writer_logical(&target);
        writer.set_logical_compaction(target.borrow());

        // A reduce over a finished input does exactly this: `upper_limit` becomes the empty
        // antichain and it forwards that to its source trace.
        let mut hold = published.handle();
        hold.set_logical_compaction(Antichain::new().borrow());

        let holds = published.logical_holds();
        assert!(
            !holds.iter().any(|h| h.is_empty()),
            "an empty request must release the hold, not record an empty one: {holds:?}"
        );
    });
}

/// An accumulation that has emptied does not release the trace.
///
/// A collection's drop empties every hold at once: `AllowCompaction` carries the empty frontier, so
/// the standing hold empties and no reader is left. Forwarding the empty accumulation would tell the
/// agent to compact everything, and its joining setter could never take that back, so the publisher
/// leaves the agent where it stands and lets the dataflow's own drop release the trace. Without that
/// the published `since` empties with it, in step with a trace that has released its contents.
#[mz_ore::test]
fn an_emptied_accumulation_does_not_release_the_trace() {
    timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("emptied oks");
            (adopt_fresh(&arranged), input)
        });
        for t in 0..3 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The controller drops the collection. Both the writer's frontier and the importing
        // runtime's applied frontier become empty, and there is no reader hold.
        let empty = Antichain::new();
        published.note_writer_logical(&empty);
        published.note_standing_hold(&empty);
        tick(
            worker,
            &mut input,
            Timestamp::from(3_u64),
            Timestamp::from(4_u64),
        );

        let (_, standing, since, _) = published.diagnostics();
        assert!(
            standing.is_empty(),
            "the standing hold did not follow the controller's empty frontier: {standing:?}"
        );
        assert_eq!(
            since,
            Antichain::from_elem(Timestamp::MIN),
            "the published since {since:?} emptied: the publisher forwarded an empty accumulation \
             and the agent's joining setter can never take that back"
        );
    });
}

/// A publisher on a two-worker runtime (`peers() == 2`) hands its handle to an importer on a
/// single-threaded runtime (`peers() == 1`). Pairwise import assumes both sides shard keys the
/// same way, which requires equal total peers, so `import_snapshot_at` must assert and panic
/// rather than silently reading the wrong shard.
///
/// Ported from the differential-dataflow primitive's own `tests/sharing.rs`
/// `import_asserts_equal_peers`.
#[mz_ore::test]
#[should_panic(expected = "peers")]
fn import_asserts_equal_peers() {
    use std::sync::mpsc;

    let (handle_tx, handle_rx) = mpsc::channel::<SharedTraceHandle<RowRowSpine<Timestamp, Diff>>>();
    // `execute` requires a `Sync` closure; `mpsc::Sender` is not `Sync`.
    let handle_tx = Mutex::new(handle_tx);

    // Publisher runtime: two worker threads, so the publishing scope's `peers()` is 2. The
    // publisher's `peers` is captured when `adopt_fresh` creates the placeholder, from the
    // scope's `peers()`, so
    // sending the handle before the dataflow ever steps is enough; only worker 0 sends, the
    // others publish redundantly (mirroring real SPMD dataflows) but nobody reads their handles.
    timely::execute(timely::Config::process(2), move |worker| {
        let (published, _input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("peers oks");
            let published = adopt_fresh(&arranged);
            (published, input)
        });
        if worker.index() == 0 {
            handle_tx.lock().unwrap().send(published.handle()).unwrap();
        }
    })
    .expect("publisher runtime failed to start");

    let handle = handle_rx.recv().expect("publisher did not send a handle");

    // Importer runtime: single-threaded (`execute_directly` never spawns worker threads), so
    // `peers()` is 1, mismatching the publisher's 2. `import_snapshot_at` runs on this same
    // thread, so its panic unwinds directly into the test rather than being swallowed at a
    // thread boundary.
    timely::execute_directly(move |worker| {
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let as_of = Antichain::from_elem(Timestamp::from(0_u64));
            let until = Antichain::from_elem(Timestamp::from(1_u64));
            let _imported = handle.import_snapshot_at(scope, "Import", as_of, until);
        });
    });
}

/// Root cause of the delayed-capability panic: within a worker step the trace's `map_batches`
/// upper can run strictly ahead of the arrangement stream's input frontier.
///
/// The trace advances the instant the arrange operator inserts a sealed batch, but the stream's
/// input frontier only reaches the sink after progress propagates, a step later. A publisher
/// that sources the seal frontier from the trace (the buggy two-source feed) can therefore
/// forward a frontier the stream has not caught up to. This records both frontiers on every
/// activation of a sink attached to a real arrangement and asserts the trace upper is observed
/// leading the stream frontier, the desync the fix sidesteps by sourcing `upper` from the
/// stream frontier alone.
#[mz_ore::test]
fn trace_upper_can_lead_stream_frontier() {
    let observed_lead = timely::execute_directly(move |worker| {
        let records: Arc<Mutex<Vec<(Antichain<Timestamp>, Antichain<Timestamp>)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let mut input = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("lead oks");
            let agent = arranged.trace.clone();
            let rec = Arc::clone(&records);
            arranged.stream.clone().sink(
                timely::dataflow::channels::pact::Pipeline,
                "record-frontiers",
                move |(handle_in, frontier)| {
                    handle_in.for_each(|_cap, data| data.drain(..).for_each(drop));
                    let stream_frontier = frontier.frontier().to_owned();
                    // Fold accumulator meaning "no batch observed yet", not a gating or published
                    // frontier, so the empty-frontier convention above does not apply here.
                    let mut trace_upper = Antichain::new();
                    agent.map_batches(|b| trace_upper = b.upper().to_owned());
                    rec.lock().unwrap().push((stream_frontier, trace_upper));
                },
            );
            input
        });

        // Seal several distinct times, stepping once between each so progress lags the trace by
        // a batch on each sealing step.
        for t in 0..6u64 {
            input.advance_to(Timestamp::from(t));
            input.update(
                (
                    Row::pack_slice(&[Datum::Int64(i64::cast_from(u32::try_from(t).unwrap()))]),
                    Row::pack_slice(&[Datum::String("v")]),
                ),
                Diff::ONE,
            );
            input.advance_to(Timestamp::from(t + 1));
            input.flush();
            worker.step();
        }
        drop(input);
        for _ in 0..8 {
            worker.step();
        }

        let records = records.lock().unwrap();
        records.iter().any(|(stream_frontier, trace_upper)| {
            match (
                stream_frontier.elements().first(),
                trace_upper.elements().first(),
            ) {
                // Both single-time here: the trace upper strictly leads when the stream
                // frontier is below it.
                (Some(s), Some(u)) => s < u,
                _ => false,
            }
        })
    });

    assert!(
        observed_lead,
        "trace map_batches upper never observed leading the stream frontier"
    );
}

/// A live reader's cut floor bounds the spine's merging, and with no reader the publisher lets it
/// merge freely.
///
/// Both arms run in one worker over the same tick sequence and the observable is the difference
/// between their chain lengths, which is immune to the spine's particular merge policy. Absolute
/// batch counts are not: "a merge happened" is true even when the publisher forwards the
/// published `since`, because everything below `since` may merge either way. Merging *above*
/// `since` is what separates the two, and neither arm calls `note_allow_compaction`, so `since`
/// stays at the minimum and every merge here is above it.
///
/// Chain length is read off the publication point rather than through a handle, because minting a
/// handle registers a floor and would perturb the arm that is supposed to have no reader.
#[mz_ore::test]
fn reader_floor_bounds_merges_and_no_reader_merges_freely() {
    timely::execute_directly(move |worker| {
        let (held, free, mut held_input, mut free_input) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (held_input, held_collection) = scope.new_collection::<(Row, Row), Diff>();
                let held_arranged = held_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("held oks");
                let (free_input, free_collection) = scope.new_collection::<(Row, Row), Diff>();
                let free_arranged = free_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("free oks");
                (
                    adopt_fresh(&held_arranged),
                    adopt_fresh(&free_arranged),
                    held_input,
                    free_input,
                )
            });

        // Seal a few times on both, then take a handle on `held` only. Its floor pins at the
        // coverage as of now, so every batch sealed after this cannot merge: a merge needs the
        // physical frontier at or beyond the batches' upper, and those uppers are all above the
        // floor. `_reader` must outlive the ticks below, it *is* the floor.
        for t in 0..4u64 {
            tick(
                worker,
                &mut held_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
            tick(
                worker,
                &mut free_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        let mut reader = held.handle();

        for t in 4..20u64 {
            tick(
                worker,
                &mut held_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
            tick(
                worker,
                &mut free_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        let held_len = held.chain_len();
        let free_len = free.chain_len();
        assert!(
            held_len > free_len,
            "held chain {held_len} is not longer than unheld chain {free_len}: the floor a \
             registration installs is not reaching the publisher, so a merge can eat a boundary \
             the reader still cuts at"
        );
        assert!(
            free_len < 16,
            "unheld chain {free_len} did not fold: the publisher is holding physical compaction \
             down even with no reader, which is what stopped every published index from merging"
        );

        // Now the reader advances its own floor, as `mz_join_core` does when its acknowledged
        // frontier moves. That must reach the publication point through the setter and free the
        // batches behind it, which is the half a bare registration does not exercise.
        reader.set_physical_compaction(Antichain::from_elem(Timestamp::from(20_u64)).borrow());
        for t in 20..24u64 {
            tick(
                worker,
                &mut held_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        let raised_len = held.chain_len();
        assert!(
            raised_len < held_len,
            "held chain went {held_len} -> {raised_len} after the reader raised its floor to 20: \
             a `set_physical_compaction` call on a shared handle is not reaching the publication \
             point, so a reader can never release the boundaries it has moved past"
        );
    });
}

/// A clone inherits its source's physical hold, not the weaker frontier its source reports.
///
/// `register`/`register_at` seed the hold at the chain coverage while reporting the published
/// `since`, which is weaker. Registering the reported frontier for a clone would silently install a
/// hold below the coverage the source was seeded with, and since the accumulation is a meet, that
/// clone becomes a floor under every other hold for as long as it lives.
#[mz_ore::test]
fn clone_inherits_the_hold_not_the_reported_frontier() {
    timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("clone oks");
            (adopt_fresh(&arranged), input)
        });
        for t in 0..4u64 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The chain now covers 4, while the published `since` is still the minimum, so the two
        // frontiers a handle carries are distinguishable.
        let handle = published.handle();
        let (since, _upper) = handle.frontiers();
        let coverage = Antichain::from_elem(Timestamp::from(4_u64));
        assert_eq!(
            since,
            Antichain::from_elem(Timestamp::MIN),
            "no compaction was requested, so `since` should still be the minimum"
        );

        let clone = handle.clone();
        drop(handle);
        let holds = published.physical_holds();
        assert_eq!(
            holds,
            vec![coverage],
            "the clone's hold is not the coverage its source was seeded with, so it sits below \
             every boundary the source still needed"
        );
        drop(clone);
    });
}

/// A live import does not pin the published spine's physical compaction.
///
/// The import's own read hold is the only registration a consumer that keeps the stream leaves
/// behind, so if that hold never rises the accumulated physical frontier never rises either, and
/// the spine stops merging for the life of the import. The cost is unbounded rather than constant:
/// one stranded batch per seal, whose retractions never consolidate, and a `CursorList` over all of
/// them on every `cursor_through`.
///
/// Same two-arm shape as [`reader_floor_bounds_merges_and_no_reader_merges_freely`], and for the
/// same reason: the observable is the difference between the arms' chain lengths, which is immune to
/// the spine's particular merge policy.
#[mz_ore::test]
fn live_import_does_not_pin_merging() {
    timely::execute_directly(move |worker| {
        let (imported, control, mut imported_input, mut control_input) = worker
            .dataflow::<Timestamp, _, _>(|scope| {
                let (imported_input, imported_collection) =
                    scope.new_collection::<(Row, Row), Diff>();
                let imported_arranged = imported_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("imported oks");
                let (control_input, control_collection) =
                    scope.new_collection::<(Row, Row), Diff>();
                let control_arranged = control_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("control oks");
                (
                    adopt_fresh(&imported_arranged),
                    adopt_fresh(&control_arranged),
                    imported_input,
                    control_input,
                )
            });

        // Seal a few times so the chain the import seeds from is non-empty.
        for t in 0..4u64 {
            tick(
                worker,
                &mut imported_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
            tick(
                worker,
                &mut control_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // A live import: empty `until`, so it never completes and its read hold lives as long as
        // the dataflow. Keep only the stream and drop the trace, which is what `as_collection` and
        // the reduce path do during construction, leaving the import's own hold as the only
        // registration.
        let handle = imported.handle();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arranged = handle.import_snapshot_at(
                scope.clone(),
                "live import",
                Antichain::from_elem(Timestamp::from(4_u64)),
                Antichain::new(),
            );
            drop(arranged.trace);
        });
        // Drop the minting handle, as `crate::render::import_shared_index` does: the import owns
        // its own clone, and a live mint would pin the floor at its own registration coverage and
        // mask what this test is about.
        drop(handle);

        for t in 4..40u64 {
            tick(
                worker,
                &mut imported_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
            tick(
                worker,
                &mut control_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        let imported_len = imported.chain_len();
        let control_len = control.chain_len();
        assert!(
            imported_len <= control_len + 2,
            "imported chain {imported_len} against unimported {control_len}: the live import's \
             read hold is not following the stream on the physical axis, so the spine cannot merge \
             for as long as the import lives"
        );
    });
}

/// A fresh importer is seeded with the frontier its seeded chain covers, not the stream frontier
/// that lags it.
///
/// [`trace_upper_can_lead_stream_frontier`] establishes that the lag is real. Registration copies
/// the chain from the trace, so seeding the lagging stream frontier alongside it would hand the
/// importer a trace covering times its own stream had not reached.
#[mz_ore::test]
fn seed_frontier_covers_the_chain_not_the_stream_frontier() {
    timely::execute_directly(move |worker| {
        let (agent, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("seed oks");
            (arranged.trace.clone(), input)
        });

        for t in 0..3u64 {
            input.advance_to(Timestamp::from(t));
            input.update(
                (
                    Row::pack_slice(&[Datum::Int64(i64::cast_from(u32::try_from(t).unwrap()))]),
                    Row::pack_slice(&[Datum::String("v")]),
                ),
                Diff::ONE,
            );
            input.advance_to(Timestamp::from(t + 1));
            input.flush();
            worker.step();
        }

        let mut chain = Vec::new();
        agent.map_batches(|batch| chain.push(batch.clone()));
        let coverage = chain.last().expect("sealed batches").upper().to_owned();
        // A stream frontier from before the last seal, the lagging value registration must not
        // seed.
        let lagging = Antichain::from_elem(Timestamp::from(0_u64));
        assert!(
            timely::PartialOrder::less_than(&lagging, &coverage),
            "test needs a stream frontier strictly below the chain coverage"
        );
        assert_eq!(
            seed_frontier::<RowRowSpine<Timestamp, Diff>>(&chain, &lagging),
            coverage,
            "seed must cover the seeded chain"
        );
        assert_eq!(
            seed_frontier::<RowRowSpine<Timestamp, Diff>>(&[], &lagging),
            lagging,
            "an empty chain covers nothing, so the stream frontier stands"
        );
    });
}

/// A live batch the seed already covers is dropped, not replayed under a capability the seed
/// has already moved past.
///
/// The importer seeds from the trace, which can hold a batch the arrangement stream has not
/// delivered yet. The publisher then pushes that same batch as a live instruction on a later
/// activation, with a hint below the frontier the seed already claimed. Replaying it would both
/// double count the batch and panic in `caps.delayed(hint)`, since the capability set no longer
/// has an element at or below the hint.
///
/// Injects exactly that ordering into a real published arrangement's importer queue, using a
/// real non-empty `Arc` batch, so the drain-and-emit loop under test is the production one. An
/// unbounded `until` keeps the capability alive long enough for the injected batch to be
/// reached: with a finite `until` the frontier check would drop the capability first and mask
/// the case. The test passes by running to completion, since the failure mode is a panic on the
/// worker thread.
#[mz_ore::test]
fn live_batch_covered_by_the_seed_is_dropped() {
    timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("hazard oks");
            let published = adopt_fresh(&arranged);
            (published, input)
        });
        input.update(
            (
                Row::pack_slice(&[Datum::Int32(1)]),
                Row::pack_slice(&[Datum::String("a")]),
            ),
            Diff::ONE,
        );
        input.advance_to(Timestamp::from(1_u64));
        input.flush();
        let handle = published.handle();
        for _ in 0..32 {
            worker.step();
        }

        // A real non-empty `Arc` batch from the published chain to replay.
        let real_batch = {
            let state = handle.shared.state.lock().unwrap();
            state
                .chain
                .iter()
                .find(|b| !b.is_empty())
                .expect("a non-empty sealed batch")
                .clone()
        };

        // Register a real importer, then step so its source seeds and drains the current chain,
        // leaving its `CapabilitySet` at the published upper (1). `until` is left unbounded so
        // the injected frontier below cannot trip the early "reached until" exit before the
        // hazardous batch is replayed.
        let as_of = Antichain::from_elem(Timestamp::from(1_u64));
        let until = Antichain::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let _imp = handle.import_snapshot_at(scope, "hazard import", as_of, until);
        });
        for _ in 0..4 {
            worker.step();
        }

        // Inject the hazardous ordering: a `Frontier` at 5 before a `Batch` whose hint is 1
        // (< 5). `Batch(5)` keeps caps at or below 5, `Frontier(5)` downgrades to 5, and
        // `Batch(1)` would then panic in `delayed` if the loop replayed it. Activate the
        // importer so it drains this step.
        {
            let mut state = handle.shared.state.lock().unwrap();
            let queue = state
                .queues
                .values_mut()
                .next_back()
                .expect("importer queue registered");
            queue.instructions.clear();
            queue.instructions.push_back(TraceReplayInstruction::Batch(
                real_batch.clone(),
                Some(Timestamp::from(5_u64)),
            ));
            queue
                .instructions
                .push_back(TraceReplayInstruction::Frontier(Antichain::from_elem(
                    Timestamp::from(5_u64),
                )));
            queue.instructions.push_back(TraceReplayInstruction::Batch(
                real_batch.clone(),
                Some(Timestamp::from(1_u64)),
            ));
            let _ = queue.activator.activate();
        }

        // Keep `input` alive so the publisher does not close and null the importer's caps.
        for _ in 0..8 {
            worker.step();
        }
        drop(input);
    });
}

/// Publishes `updates` as a `RowRow` index, sealing one batch per distinct time, and returns the
/// publication plus its still-open input handle (dropping the handle would close the publisher).
fn publish_updates(
    worker: &mut timely::worker::Worker,
    updates: &[(i64, &'static str, u64, i64)],
    seal: u64,
    name: &'static str,
) -> (
    Published<RowRowSpine<Timestamp, Diff>>,
    differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
) {
    let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
        let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
        let arranged = collection.mz_arrange::<
            ColumnationChunker<_>,
            RowRowBatcher<_, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >(name);
        let published = adopt_fresh(&arranged);
        (published, input)
    });

    let mut times: Vec<u64> = updates.iter().map(|&(_, _, t, _)| t).collect();
    times.sort_unstable();
    times.dedup();
    for &t in &times {
        input.advance_to(Timestamp::from(t));
        for &(k, v, ut, d) in updates {
            if ut == t {
                input.update(
                    (
                        Row::pack_slice(&[Datum::Int64(k)]),
                        Row::pack_slice(&[Datum::String(v)]),
                    ),
                    Diff::from(d),
                );
            }
        }
        input.flush();
        for _ in 0..16 {
            worker.step();
        }
    }
    input.advance_to(Timestamp::from(seal));
    input.flush();
    (published, input)
}

/// A differential join over two single-sourced imports must equal the direct join exactly, with
/// no doubling and correct multiplicities (key 1 is inserted then retracted and must cancel).
///
/// This is the row-doubling regression guard on the fixed publisher: the imported trace's upper
/// (read by the join through `map_batches`) tracks the stream frontier the fix publishes, so the
/// trace never runs ahead of the stream and the join counts each match once.
#[mz_ore::test]
fn join_over_single_sourced_import_matches_direct() {
    use std::sync::mpsc;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};

    // (key, value, time, diff). Key 1 inserted at 0, retracted at 2.
    let a: Vec<(i64, &str, u64, i64)> = vec![
        (1, "a", 0, 1),
        (2, "b", 0, 1),
        (3, "c", 1, 1),
        (1, "a", 2, -1),
    ];
    let b: Vec<(i64, &str, u64, i64)> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
    let seal = 3u64;

    // Direct-join oracle: matching pair emits at the max of their times with the product diff.
    let mut oracle: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for &(ka, la, ta, da) in &a {
        for &(kb, rb, tb, db) in &b {
            if ka != kb {
                continue;
            }
            let row = Row::pack_slice(&[Datum::Int64(ka), Datum::String(la), Datum::String(rb)]);
            let time = Timestamp::from(ta.max(tb));
            *oracle.entry((row, time)).or_insert(Diff::ZERO) += Diff::from(da * db);
        }
    }
    let mut expected: Vec<(Row, Timestamp, Diff)> = oracle
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((r, t), d)| (r, t, d))
        .collect();
    expected.sort();

    let (tx, rx) = mpsc::channel();
    timely::execute_directly(move |worker| {
        let (pub_a, keep_a) = publish_updates(worker, &a, seal, "join A");
        let (pub_b, keep_b) = publish_updates(worker, &b, seal, "join B");
        let ha = pub_a.handle();
        let hb = pub_b.handle();

        // `as_of = 0` matches the earliest real time in either input, so no update coalesces;
        // `until = seal` (open) keeps every distinct time in `[0, seal)` visible, matching what
        // the old live import would have produced over this same run.
        let as_of = Antichain::from_elem(Timestamp::from(0_u64));
        let until = Antichain::from_elem(Timestamp::from(seal));
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr_a =
                ha.import_snapshot_at(scope.clone(), "import A", as_of.clone(), until.clone());
            let arr_b = hb.import_snapshot_at(scope.clone(), "import B", as_of, until);
            let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                let row = Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                Some(row)
            });
            joined.inner.probe_with(&probe).capture_into(tx.clone());
        });

        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while probe.less_than(&seal_ts) {
            let _keep = (&keep_a, &keep_b);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "join did not seal through {seal_ts:?}");
        }
    });

    let got: Vec<(Row, Timestamp, Diff)> = rx.extract().into_iter().flat_map(|(_, d)| d).collect();
    let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for (row, time, diff) in got {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
    }
    let got: Vec<(Row, Timestamp, Diff)> = consolidated
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((r, t), d)| (r, t, d))
        .collect();

    assert_eq!(
        got, expected,
        "join over single-sourced imports diverged from the direct join"
    );
}

/// An empty seal (frontier advance with no data) still advances the imported frontier, so a
/// bounded read reaches its `until` and completes.
///
/// The publisher sources `upper` from the stream frontier, which advances on empty seals because
/// the arrange operator downgrades its output capability on every seal. Data is sealed to `1`,
/// then a quiet advance to `2` seals nothing. A bounded read with `until = {2}` completes only
/// if that empty seal moved the published upper from `1` to `2`, since the only path to `2` is
/// the quiet advance.
#[mz_ore::test]
fn empty_seal_advances_import_frontier_to_completion() {
    use std::sync::mpsc;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};

    let (tx, rx) = mpsc::channel();
    timely::execute_directly(move |worker| {
        let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged = collection.mz_arrange::<
                ColumnationChunker<_>,
                RowRowBatcher<_, _>,
                RowRowBuilder<_, _>,
                RowRowSpine<_, _>,
            >("empty-seal oks");
            let published = adopt_fresh(&arranged);
            (published, input)
        });

        // Data at time 0, sealed to 1.
        input.advance_to(Timestamp::from(0_u64));
        input.update(
            (
                Row::pack_slice(&[Datum::Int64(7)]),
                Row::pack_slice(&[Datum::String("d")]),
            ),
            Diff::ONE,
        );
        input.advance_to(Timestamp::from(1_u64));
        input.flush();
        for _ in 0..16 {
            worker.step();
        }
        // Quiet seal: advance to 2 with no data. The only way the published upper reaches 2.
        input.advance_to(Timestamp::from(2_u64));
        input.flush();
        for _ in 0..16 {
            worker.step();
        }

        let handle = published.handle();
        let until = Timestamp::from(2_u64);
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr = handle.import_snapshot_at(
                scope.clone(),
                "bounded snap",
                Antichain::from_elem(Timestamp::from(0_u64)),
                Antichain::from_elem(until),
            );
            arr.as_collection(|k, v| Row::pack(k.into_iter().chain(v.into_iter())))
                .inner
                .probe_with(&probe)
                .capture_into(tx.clone());
        });

        let mut steps = 0;
        while probe.less_than(&until) {
            let _keep = &input;
            worker.step();
            steps += 1;
            assert!(
                steps < 10_000,
                "empty seal did not drive the bounded read to completion"
            );
        }
        drop(input);
    });

    // The bounded read completed (the loop above did not time out) and observed the row, its
    // times coalesced to `as_of = 0`.
    let rows: Vec<Row> = rx
        .extract()
        .into_iter()
        .flat_map(|(_, d)| d)
        .filter(|(_, _, diff)| !diff.is_zero())
        .map(|(row, _, _)| row)
        .collect();
    assert_eq!(
        rows,
        vec![Row::pack_slice(&[Datum::Int64(7), Datum::String("d")])],
        "bounded read returned the wrong accumulation"
    );
}
