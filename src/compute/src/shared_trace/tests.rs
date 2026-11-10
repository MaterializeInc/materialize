// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::input::Input;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::cursor::{CursorList, cursor_list};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_row_spine::{DatumSeq, RowRowBatcher};
use mz_timely_util::arrange_agent::TraceAgent;
use mz_timely_util::shared_trace::{Binding, BindingReader, HoldAccumulation};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::Navigable;
use differential_dataflow::trace::{Span, Trace, TraceReader};
use timely::progress::Antichain;

use crate::extensions::arrange::MzArrange;
use crate::typedefs::RowRowSpine;

use super::*;

/// How long [`SharedReaderExt::snapshot_at`] waits for a seal before failing the test.
///
/// Generous, because it only has to exceed the time a correct publisher takes to step. A test that
/// hits it has wedged, and the assertion reports which frontier stalled.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);

/// An owned, consistent snapshot of a published arrangement: an immutable chain.
///
/// The result of [`SharedReaderExt::snapshot_at`]. Holding it pins the chain's batches, keeping
/// their memory alive even as the publishing worker merges.
pub(crate) struct TraceSnapshot<T, B> {
    chain: Vec<Span<T, B>>,
}

impl<T, B: Clone> TraceSnapshot<T, B> {
    /// A cursor merging the snapshot's batch cursors, with the batches as its storage.
    pub(crate) fn cursor(&self) -> (CursorList<B::Cursor>, Vec<B>)
    where
        B: Navigable,
    {
        cursor_list(
            self.chain
                .iter()
                .filter_map(|span| span.inner.clone())
                .collect(),
        )
    }
}

/// Observations on a publication point's internals that no reader surface exposes.
impl<Tr: TraceReader> Published<Tr> {
    /// The accumulated logical holds registered against this publication point. Empty when every
    /// hold has released.
    pub(crate) fn logical_holds(&self) -> Antichain<Tr::Time> {
        self.shared.logical_holds()
    }

    /// The accumulated physical holds registered against this publication point. Empty when no
    /// reader is registered.
    pub(crate) fn physical_holds(&self) -> Antichain<Tr::Time> {
        self.shared.physical_holds()
    }

    /// The number of batches in the published chain.
    ///
    /// Counting through a handle would register a physical hold and perturb the merge behaviour
    /// being measured, which is the whole observable here.
    pub(crate) fn chain_len(&self) -> usize {
        self.shared.chain().len()
    }
}

/// Drops every dataflow installed on `worker`, as compute drops a collection's dataflow.
///
/// A live import never completes on its own, because a dropped writer leaves it at the last
/// published `upper`, so a test that ends with one installed must drop it before
/// `execute_directly` waits for its dataflows to finish.
pub(crate) fn drop_dataflows(worker: &mut timely::worker::Worker) {
    for dataflow in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow);
    }
}

/// Test-only observations on a publication point reached through one of its readers.
///
/// `SharedReader` is defined in `mz-timely-util`, so these cannot be inherent methods here.
pub(crate) trait SharedReaderExt<T, B> {
    /// The published arrangement's current `(since, upper)` frontiers, read under the state lock.
    fn frontiers(&self) -> (Antichain<T>, Antichain<T>);

    /// Takes a consistent snapshot of the published arrangement as of `time`, waiting until `upper`
    /// passes `time`.
    fn snapshot_at(&self, time: &T) -> Option<TraceSnapshot<T, B>>
    where
        T: Debug;
}

impl<T: timely::progress::Timestamp + Lattice, B: Clone> SharedReaderExt<T, B>
    for SharedReader<T, B>
{
    fn frontiers(&self) -> (Antichain<T>, Antichain<T>) {
        self.shared().frontiers()
    }

    /// Production reads go through [`SharedReader::import_frontier_core`], which is notification
    /// driven and never parks a worker. This waits by polling, so it must not be called from a
    /// timely worker thread: a worker blocked here cannot step, and on a single-worker test that
    /// includes the publisher it is waiting for.
    ///
    /// Returns `None` when compaction has advanced `since` beyond `time`, so the accumulation at
    /// `time` is no longer accurate. The gate mirrors the single-runtime peek path, which errors
    /// when the compaction frontier is beyond the read time.
    ///
    /// Panics once the wait exceeds [`SNAPSHOT_TIMEOUT`], naming the frontiers it was waiting on.
    fn snapshot_at(&self, time: &T) -> Option<TraceSnapshot<T, B>>
    where
        T: Debug,
    {
        let deadline = Instant::now() + SNAPSHOT_TIMEOUT;
        let shared = self.shared();
        loop {
            let (since, upper) = shared.frontiers();
            // `upper` not less-equal `time` means all updates at `time` are sealed.
            if !upper.less_equal(time) {
                // `since` beyond `time` means times at `time` have been coalesced and a read
                // there would be inaccurate.
                if !since.less_equal(time) {
                    return None;
                }
                return Some(TraceSnapshot {
                    chain: shared.chain(),
                });
            }
            assert!(
                Instant::now() < deadline,
                "snapshot_at({time:?}) timed out waiting for a seal: upper={:?} since={:?}",
                upper.elements(),
                since.elements(),
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Adopts a freshly rendered arrangement into a new point, the standalone-primitive counterpart to
/// the maintenance create-then-adopt path. The publication closes when the trace drops, and the
/// trace lives only as long as an agent does (production keeps one in the trace manager), so callers
/// keep `arranged.trace` or a clone alive for the life of the test. Creates a [`Published::new`] sized to the arrangement's
/// own scope, installs `arranged`'s publisher into it via [`adopt_trace`], and returns
/// the now-backed point.
fn adopt_fresh<'a, Tr>(arranged: &Arranged<'a, TraceAgent<Tr>>) -> Published<Tr>
where
    Tr: Trace + 'static,
    Tr::Time: timely::order::TotalOrder,
{
    let published = Published::new();
    adopt_trace(
        &arranged.trace,
        arranged.stream.scope().worker(),
        &published,
        || {},
    );
    published
}

/// Smoke test: arrange two rows into a `RowRow` spine, publish it through the extension trait,
/// mint a `Send` handle, and read the sealed contents back via `snapshot_at`.
///
/// Publisher and reader share one worker stepped to completion inside `execute_directly`. The
/// returned handle keeps the published chain alive through its `Arc`s, so the snapshot observes
/// the sealed rows even after the publishing worker tears down. This is the single-worker
/// publish + snapshot path. The full cross-thread and import-replay coverage lives in
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
        let (published, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("smoke oks");
            // The extension trait under test.
            let published = adopt_fresh(&arranged);
            (published, input, arranged.trace.clone())
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
/// A seal-only advance produces an update-free span that the arrange operator writes to the trace
/// without sending it on the output stream. The publisher reads that span back through
/// `map_spans`, so the published chain and `upper` reach the seal even though no data ever
/// traveled the stream. Here we advance the input to `1` with no updates and assert the
/// published `upper` passes `0`, so `snapshot_at(0)` returns rather than blocking.
#[mz_ore::test]
fn quiet_seal_advances_upper() {
    let (upper, snapshot_is_some) = timely::execute_directly(move |worker| {
        let (published, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("quiet oks");
            let published = adopt_fresh(&arranged);
            (published, input, arranged.trace.clone())
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

/// A worker on one thread publishes an arrangement. A separate thread holds a `Send` handle,
/// blocks in `snapshot_at` for the publication frontier to pass a time, and reads the
/// collection at that time.
///
/// Ported from the differential-dataflow primitive's own `tests/sharing.rs`
/// `snapshot_from_another_thread`. Unlike `crate::sharing`'s cross-runtime coverage, which reads
/// only after the publishing worker has already torn down, this keeps the publisher stepping
/// concurrently on its own thread so the reader genuinely waits for a seal that has not happened
/// yet.
#[mz_ore::test]
fn snapshot_at_waits_until_upper_passes_time() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let key = |k: i32| Row::pack_slice(&[Datum::Int32(k)]);
    let val = |v: &str| Row::pack_slice(&[Datum::String(v)]);

    let (handle_tx, handle_rx) = mpsc::channel::<SharedOksHandle>();
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
        let (published, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("cross-thread oks");
            let published = adopt_fresh(&arranged);
            (published, input, arranged.trace.clone())
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
        let deadline = Instant::now() + 2 * SNAPSHOT_TIMEOUT;
        while !done.load(Ordering::SeqCst) {
            worker.step();
            assert!(
                Instant::now() < deadline,
                "reader did not finish its snapshot"
            );
        }
    });

    let got = reader.join().expect("reader thread panicked");
    // As of time 2: (1,"a") present, (2,"b") inserted then retracted, (3,"c") inserted at 2.
    assert_eq!(got, vec![(key(1), val("a")), (key(3), val("c"))]);
}

/// Feeds `input` a fresh update at `at`, advances the frontier to `next`, and steps `worker`,
/// so the arrange operator activates and applies any reader hold that moved since the last
/// tick. Mirrors the `tick` helper in the differential-dataflow primitive's own
/// `tests/sharing.rs`.
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
/// advances its logical compaction on the writer's own handle, the published `since` must follow,
/// so the trace actually compacts.
///
/// Exercises the publisher's compaction forwarding with no accumulated hold, which no other test in
/// this crate or `crate::sharing` covers: `crate::render`'s `interactive_import_hold_releases_on_drop`
/// always has a live reader hold present at some point in the scenario.
#[mz_ore::test]
fn publish_without_readers_does_not_pin_compaction() {
    timely::execute_directly(move |worker| {
        // Keep a writer handle (a plain `TraceAgent` clone) alongside the publication, and mint no
        // `SharedReader` until after compaction: that keeps `logical_holds` empty, so the
        // publisher has zero registered reader holds throughout.
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("no-readers oks");
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

        // The controller requests compaction to 10 on the writer's own handle (the production path
        // is `handle_allow_compaction` through the `TraceManager`), which the trace mirrors into the
        // published `since` at once.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
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

#[mz_ore::test]
fn a_point_nothing_references_is_detached() {
    timely::execute_directly(move |worker| {
        let (kept, dropped, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("detach oks");
            let kept = adopt_fresh(&arranged);
            let dropped = adopt_fresh(&arranged);
            (kept, dropped, input, arranged.trace.clone())
        });
        tick(
            worker,
            &mut input,
            Timestamp::from(0_u64),
            Timestamp::from(1_u64),
        );

        let dropped_shared = Arc::downgrade(&dropped.shared);
        drop(dropped);
        tick(
            worker,
            &mut input,
            Timestamp::from(1_u64),
            Timestamp::from(2_u64),
        );
        assert!(
            dropped_shared.upgrade().is_none(),
            "the trace must release a point nothing else references"
        );
        assert_eq!(
            kept.shared.upper(),
            Antichain::from_elem(Timestamp::from(2_u64)),
            "a referenced point keeps receiving the trace"
        );
    });
}

/// A hold registered on a placeholder below the `since` of the trace that later attaches does not
/// rewind that trace: a spine accepts a lower logical frontier without complaint, and would then
/// admit reads at times it has already coalesced.
#[mz_ore::test]
fn a_hold_below_the_attaching_since_does_not_rewind_it() {
    timely::execute_directly(move |worker| {
        let placeholder = Published::new();
        let _peer = placeholder
            .shared
            .reader_at_least(&Antichain::from_elem(Timestamp::from(3_u64)));

        let (mut writer, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("rewind oks");
            (arranged.trace, input)
        });
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        adopt_trace(&writer, worker, &placeholder, || {});
        tick(
            worker,
            &mut input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );
        assert!(
            placeholder
                .handle()
                .snapshot_at(&Timestamp::from(5_u64))
                .is_none(),
            "the trace rewound its since to a hold registered below it"
        );
    });
}

/// `Shared::reader_at` mints a hold at the requested `as_of`, and refuses when the published
/// `since` has already passed it.
///
/// Refusing is the whole point: a reader that observed `since`, decided it permitted its `as_of`,
/// and only then advanced a hold would be racing the publisher across three separate acquisitions
/// of the state lock. The mint collapses that to one, so a handle it returns holds a frontier the
/// trace can still serve. A refusal is reported, not degraded, because the controller promises an
/// index's `since` never passes the `as_of` of a dataflow importing it.
#[mz_ore::test]
fn reader_at_mints_at_as_of_or_refuses() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("handle-at oks");
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
            .shared
            .reader_at(&at_three)
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

        // The controller allows compaction to 10, so the publisher forwards a `since` of 10 on its
        // next activation.
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        assert_eq!(
            published.shared.reader_at(&at_three).err(),
            Some(target.clone()),
            "a mint below the published since must be refused, and report it"
        );
        assert!(
            published.shared.reader_at(&target).is_ok(),
            "a mint at the published since must succeed"
        );
    });
}

/// A point reports both the frontier its writer's agents asked for and the one the holds let it
/// apply.
#[mz_ore::test]
fn logical_frontiers_report_what_the_holds_withhold() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("gap oks");
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

        // The peer holds at four while the agents ask for eight and then ten, so the writer applies
        // four and reports what the agents asked for.
        let held = Antichain::from_elem(Timestamp::from(4_u64));
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        let mut peer = published.shared.reader_at_least(&held);
        let first = Antichain::from_elem(Timestamp::from(8_u64));
        writer.set_logical_compaction(first.borrow());
        assert_eq!(
            published.logical_frontiers(),
            (held.clone(), first),
            "a request the hold caps must report the hold and the request"
        );
        // A further request the hold keeps from moving the applied frontier still moves the
        // requested one.
        writer.set_logical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );
        assert_eq!(
            published.logical_frontiers(),
            (held, target.clone()),
            "the applied frontier must report the hold and the requested one the controller"
        );

        // The peer catches up and the gap closes.
        peer.set_logical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(11_u64),
            Timestamp::from(12_u64),
        );
        assert_eq!(
            published.logical_frontiers(),
            (target.clone(), target),
            "a caught-up importer must leave no gap"
        );
    });
}

/// A consumer forwarding an empty input frontier releases its hold.
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
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("f4 oks");
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
        // The writer's own agent holds in the same accumulation as readers, so the reader is
        // minted while `since` is at the minimum and the writer then holds above it. The
        // accumulation sits at the reader's hold until the reader releases.
        let mut hold = published.handle();
        let target = Antichain::from_elem(Timestamp::from(2_u64));
        writer.set_logical_compaction(target.borrow());
        assert_eq!(
            published.logical_holds(),
            Antichain::from_elem(Timestamp::MIN)
        );

        // A reduce over a finished input does exactly this: `upper_limit` becomes the empty
        // antichain and it forwards that to its source trace.
        hold.set_logical_compaction(Antichain::new().borrow());

        assert_eq!(
            published.logical_holds(),
            target,
            "an empty request must release the hold, leaving only the writer's"
        );
    });
}

/// An accumulation that has emptied does not release the trace.
///
/// A collection's drop empties every hold at once: `AllowCompaction` carries the empty frontier, so
/// the peer's hold empties and no reader is left. Forwarding the empty accumulation would tell the
/// agent to compact everything, and its joining setter could never take that back, so the publisher
/// leaves the agent where it stands and lets the dataflow's own drop release the trace. Without that
/// the published `since` empties with it, in step with a trace that has released its contents.
#[mz_ore::test]
fn an_emptied_accumulation_does_not_release_the_trace() {
    timely::execute_directly(move |worker| {
        let (published, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("emptied oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        for t in 0..3 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The controller drops the collection. The peer's hold becomes empty, and there is no
        // reader hold left.
        let empty = Antichain::new();
        let mut peer = published
            .shared
            .reader_at_least(&Antichain::from_elem(Timestamp::MIN));
        peer.set_logical_compaction(empty.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(3_u64),
            Timestamp::from(4_u64),
        );

        // Only the trace's own agent, at the minimum, still holds.
        assert_eq!(
            published.logical_holds(),
            Antichain::from_elem(Timestamp::MIN)
        );
        let since = published.handle().frontiers().0;
        assert_eq!(
            since,
            Antichain::from_elem(Timestamp::MIN),
            "the published since {since:?} emptied: the publisher forwarded an empty accumulation \
             and the agent's joining setter can never take that back"
        );
    });
}

/// A live reader's cut floor bounds the spine's merging, and with no reader the publisher lets it
/// merge freely.
///
/// Both arms run in one worker over the same tick sequence and the observable is the difference
/// between their chain lengths, which is immune to the spine's particular merge policy. Absolute
/// batch counts are not: "a merge happened" is true even when the publisher forwards the
/// published `since`, because everything below `since` may merge either way. Merging *above*
/// `since` is what separates the two, and neither arm compacts logically, so `since`
/// stays at the minimum and every merge here is above it.
///
/// Chain length is read off the publication point rather than through a handle, because minting a
/// handle registers a floor and would perturb the arm that is supposed to have no reader.
#[mz_ore::test]
fn reader_floor_bounds_merges_and_no_reader_merges_freely() {
    timely::execute_directly(move |worker| {
        let (held, free, mut held_input, mut free_input, (mut held_keep, mut free_keep)) =
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (held_input, held_collection) = scope.new_collection::<(Row, Row), Diff>();
                let held_arranged = held_collection
                    .mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("held oks");
                let (free_input, free_collection) = scope.new_collection::<(Row, Row), Diff>();
                let free_arranged = free_collection
                    .mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("free oks");
                (
                    adopt_fresh(&held_arranged),
                    adopt_fresh(&free_arranged),
                    held_input,
                    free_input,
                    (held_arranged.trace.clone(), free_arranged.trace.clone()),
                )
            });

        // The kept agents stand in for the trace manager, which lets every arrangement's spine merge
        // up to its upper. Logical compaction stays at the minimum, so every merge is above `since`.
        let mut tick_both = |worker: &mut timely::worker::Worker, t: u64| {
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
            let upper = Antichain::from_elem(Timestamp::from(t + 1));
            held_keep.set_physical_compaction(upper.borrow());
            free_keep.set_physical_compaction(upper.borrow());
        };

        // Seal a few times on both, then take a handle on `held` only. Its floor pins at the
        // coverage as of now, so every batch sealed after this cannot merge: a merge needs the
        // physical frontier at or beyond the batches' upper, and those uppers are all above the
        // floor. `reader` must outlive the ticks below, it *is* the floor.
        for t in 0..4u64 {
            tick_both(worker, t);
        }
        let mut reader = held.handle();

        for t in 4..20u64 {
            tick_both(worker, t);
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
            tick_both(worker, t);
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

/// A clone registers at its source's physical frontier, the chain coverage.
///
/// Registering anything lower, such as the published `since`, would put the clone below the boundary
/// its source was seeded with, and since the accumulation is a meet, that clone becomes a floor under
/// every other hold for as long as it lives.
#[mz_ore::test]
fn clone_registers_at_its_sources_physical_frontier() {
    timely::execute_directly(move |worker| {
        let (published, mut input, mut keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("clone oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        for t in 0..4u64 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        // The kept agent holds in the same accumulation as readers. Raising it to the coverage, as
        // the trace manager's maintenance does, leaves the readers' holds as the floor.
        keep.set_physical_compaction(Antichain::from_elem(Timestamp::from(4_u64)).borrow());

        // The chain now covers 4, while the published `since` is still the minimum, so a hold at
        // the coverage is distinguishable from one at `since`.
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
        assert_eq!(
            published.physical_holds(),
            coverage,
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
/// the spine stops merging for the life of the import. The cost is unbounded:
/// one stranded batch per seal, whose retractions never consolidate, and a `CursorList` over all of
/// them on every `cursor_through`.
///
/// Same two-arm shape as [`reader_floor_bounds_merges_and_no_reader_merges_freely`], and for the
/// same reason: the observable is the difference between the arms' chain lengths, which is immune to
/// the spine's particular merge policy.
#[mz_ore::test]
fn live_import_does_not_pin_merging() {
    timely::execute_directly(move |worker| {
        let (
            imported,
            control,
            mut imported_input,
            mut control_input,
            (mut imported_keep, mut control_keep),
        ) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (imported_input, imported_collection) = scope.new_collection::<(Row, Row), Diff>();
            let imported_arranged = imported_collection
                .mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("imported oks");
            let (control_input, control_collection) = scope.new_collection::<(Row, Row), Diff>();
            let control_arranged = control_collection
                .mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("control oks");
            (
                adopt_fresh(&imported_arranged),
                adopt_fresh(&control_arranged),
                imported_input,
                control_input,
                (
                    imported_arranged.trace.clone(),
                    control_arranged.trace.clone(),
                ),
            )
        });

        // The kept agents stand in for the trace manager, which lets every arrangement's spine merge
        // up to its upper.
        let mut tick_both = |worker: &mut timely::worker::Worker, t: u64| {
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
            let upper = Antichain::from_elem(Timestamp::from(t + 1));
            imported_keep.set_physical_compaction(upper.borrow());
            control_keep.set_physical_compaction(upper.borrow());
        };

        // Seal a few times so the chain the import seeds from is non-empty.
        for t in 0..4u64 {
            tick_both(worker, t);
        }

        // A live import: empty `until`, so it never completes and its read hold lives as long as
        // the dataflow. Keep only the stream and drop the trace, which is what `as_collection` and
        // the reduce path do during construction, leaving the import's own hold as the only
        // registration.
        let handle = imported.handle();
        let import_dataflow = worker.next_dataflow_index();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arranged = handle.import_frontier_core(
                scope.clone(),
                "live import",
                Antichain::from_elem(Timestamp::from(4_u64)),
                Antichain::new(),
            );
            drop(arranged.trace);
        });
        // Drop the minting handle, as `ArrangementSharingRegistry::import` does: the import owns its
        // own clone, and a live mint would pin the floor at its own registration coverage and mask
        // what this test is about.
        drop(handle);

        for t in 4..40u64 {
            tick_both(worker, t);
        }

        let imported_len = imported.chain_len();
        let control_len = control.chain_len();
        assert!(
            imported_len <= control_len + 2,
            "imported chain {imported_len} against unimported {control_len}: the live import's \
             read hold is not following the stream on the physical axis, so the spine cannot merge \
             for as long as the import lives"
        );
        // A live import never completes on its own, so drop it the way compute drops a dataflow.
        worker.drop_dataflow(import_dataflow);
    });
}

#[mz_ore::test]
fn dropped_writer_leaves_imports_at_its_last_upper() {
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;

    timely::execute_directly(move |worker| {
        let writer_dataflow = worker.next_dataflow_index();
        let (published, mut input, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("writer oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        tick(
            worker,
            &mut input,
            Timestamp::from(0_u64),
            Timestamp::from(1_u64),
        );

        let handle = published.handle();
        let probe = ProbeHandle::new();
        let import_dataflow = worker.next_dataflow_index();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arranged = handle.import_frontier_core(
                scope.clone(),
                "live import",
                Antichain::from_elem(Timestamp::from(0_u64)),
                Antichain::new(),
            );
            arranged.stream.probe_with(&probe);
        });
        drop(handle);
        let mut steps = 0;
        while probe.less_than(&Timestamp::from(1_u64)) {
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "the live import did not seal time 0");
        }

        // Drop the writer the way `drop_collection` does, the trace handle before the dataflow,
        // while its input is still open at 1.
        drop(keep);
        worker.drop_dataflow(writer_dataflow);
        for _ in 0..100 {
            worker.step();
        }
        probe.with_frontier(|frontier| {
            assert_eq!(
                frontier.to_vec(),
                vec![Timestamp::from(1_u64)],
                "the import must not claim the times past the writer's last upper"
            )
        });

        worker.drop_dataflow(import_dataflow);
        drop(input);
    });
}

#[mz_ore::test]
fn a_writer_dropped_before_its_trace_leaves_imports_at_its_last_upper() {
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;

    timely::execute_directly(move |worker| {
        let writer_dataflow = worker.next_dataflow_index();
        let (published, mut input, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("writer oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        tick(
            worker,
            &mut input,
            Timestamp::from(0_u64),
            Timestamp::from(1_u64),
        );

        let handle = published.handle();
        let probe = ProbeHandle::new();
        let import_dataflow = worker.next_dataflow_index();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arranged = handle.import_frontier_core(
                scope.clone(),
                "live import",
                Antichain::from_elem(Timestamp::from(0_u64)),
                Antichain::new(),
            );
            arranged.stream.probe_with(&probe);
        });
        drop(handle);
        while probe.less_than(&Timestamp::from(1_u64)) {
            worker.step();
        }

        // The dataflow goes first while an agent keeps the trace alive, the order opposite to
        // `dropped_writer_leaves_imports_at_its_last_upper`.
        worker.drop_dataflow(writer_dataflow);
        for _ in 0..100 {
            worker.step();
        }
        let mut local_upper = None;
        keep.map_spans(|span| local_upper = Some(span.upper().clone()));
        assert_eq!(
            local_upper,
            Some(Antichain::new()),
            "the spine takes the writer's closing seal, as differential's does"
        );
        assert_eq!(
            published.shared.upper(),
            Antichain::from_elem(Timestamp::from(1_u64)),
            "the point keeps the writer's last upper"
        );
        probe.with_frontier(|frontier| {
            assert_eq!(
                frontier.to_vec(),
                vec![Timestamp::from(1_u64)],
                "the import must not claim the times past the writer's last upper"
            )
        });

        drop(keep);
        worker.drop_dataflow(import_dataflow);
        drop(input);
    });
}

/// An import registered after the writer dropped is seeded from its frozen view. A later trace
/// attaching to the point would regress the chain below what the import acknowledged, so the
/// attach panics, and the import keeps the frozen view.
#[mz_ore::test]
#[should_panic(expected = "attach to a point that already had a writer")]
fn a_later_trace_cannot_attach_to_a_point_an_import_reads_frozen() {
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;

    timely::execute_directly(move |worker| {
        let old_dataflow = worker.next_dataflow_index();
        let (published, mut old_input, old_keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("old oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        for t in 0..2 {
            tick(
                worker,
                &mut old_input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        drop(old_keep);
        worker.drop_dataflow(old_dataflow);
        for _ in 0..20 {
            worker.step();
        }

        let handle = published.handle();
        let probe = ProbeHandle::new();
        let import_dataflow = worker.next_dataflow_index();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arranged = handle.import_frontier_core(
                scope.clone(),
                "gap import",
                Antichain::from_elem(Timestamp::from(0_u64)),
                Antichain::new(),
            );
            arranged.stream.probe_with(&probe);
        });
        drop(handle);
        while probe.less_than(&Timestamp::from(2_u64)) {
            worker.step();
        }

        let new_dataflow = worker.next_dataflow_index();
        let (_new_input, _new_keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("new oks");
            adopt_trace(&arranged.trace, scope.worker(), &published, || {});
            (input, arranged.trace.clone())
        });
        // Reached only if the attach was accepted. Dropping the dataflows ends the test instead of
        // waiting for an import that the new chain never completes.
        worker.drop_dataflow(new_dataflow);
        worker.drop_dataflow(import_dataflow);
        drop(old_input);
    });
}

/// Publishes `updates` as a `RowRow` index, sealing one batch per distinct time, and returns the
/// publication, its still-open input handle, and an agent that keeps the trace alive.
fn publish_updates(
    worker: &mut timely::worker::Worker,
    updates: &[(i64, &'static str, u64, i64)],
    seal: u64,
    name: &'static str,
) -> (
    Published<RowRowSpine<Timestamp, Diff>>,
    differential_dataflow::input::InputSession<Timestamp, (Row, Row), Diff>,
    TraceAgent<RowRowSpine<Timestamp, Diff>>,
) {
    let (published, mut input, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
        let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
        let arranged = collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>(name);
        let published = adopt_fresh(&arranged);
        (published, input, arranged.trace.clone())
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
    (published, input, keep)
}

/// The direct join of `a` and `b` on their keys: each matching pair emits `(key, a value, b value)`
/// at the later of their times with the product of their diffs, consolidated.
fn direct_join(
    a: &[(i64, &'static str, u64, i64)],
    b: &[(i64, &'static str, u64, i64)],
) -> Vec<(Row, Timestamp, Diff)> {
    let mut pairs = Vec::new();
    for &(ka, la, ta, da) in a {
        for &(kb, rb, tb, db) in b {
            if ka == kb {
                let row =
                    Row::pack_slice(&[Datum::Int64(ka), Datum::String(la), Datum::String(rb)]);
                pairs.push((row, Timestamp::from(ta.max(tb)), Diff::from(da * db)));
            }
        }
    }
    consolidate(pairs)
}

/// `updates` summed per `(row, time)`, without zero entries, sorted.
fn consolidate(
    updates: impl IntoIterator<Item = (Row, Timestamp, Diff)>,
) -> Vec<(Row, Timestamp, Diff)> {
    let mut consolidated: BTreeMap<(Row, Timestamp), Diff> = BTreeMap::new();
    for (row, time, diff) in updates {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += diff;
    }
    consolidated
        .into_iter()
        .filter(|(_, d)| !d.is_zero())
        .map(|((r, t), d)| (r, t, d))
        .collect()
}

/// A differential join over two single-sourced imports must equal the direct join exactly, with
/// no doubling and correct multiplicities (key 1 is inserted then retracted and must cancel).
///
/// This is the row-doubling regression guard on the fixed publisher: the imported trace's upper
/// (read by the join through `map_spans`) tracks the stream frontier the fix publishes, so the
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

    let expected = direct_join(&a, &b);

    let (tx, rx) = mpsc::channel();
    timely::execute_directly(move |worker| {
        let (pub_a, keep_a, _agent_a) = publish_updates(worker, &a, seal, "join A");
        let (pub_b, keep_b, _agent_b) = publish_updates(worker, &b, seal, "join B");
        let ha = pub_a.handle();
        let hb = pub_b.handle();

        // `as_of = 0` matches the earliest real time in either input, so no update coalesces, and
        // `until = seal` (open) keeps every distinct time in `[0, seal)` visible.
        let as_of = Antichain::from_elem(Timestamp::from(0_u64));
        let until = Antichain::from_elem(Timestamp::from(seal));
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr_a =
                ha.import_frontier_core(scope.clone(), "import A", as_of.clone(), until.clone());
            let arr_b = hb.import_frontier_core(scope.clone(), "import B", as_of, until);
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

    let got = consolidate(rx.extract().into_iter().flat_map(|(_, d)| d));

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
        let (published, mut input, _keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("empty-seal oks");
            let published = adopt_fresh(&arranged);
            (published, input, arranged.trace.clone())
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
            let arr = handle.import_frontier_core(
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

/// A [`Binding`] over the batches of a `RowRow` index.
type RowBinding = Binding<Timestamp, <RowRowSpine<Timestamp, Diff> as TraceReader>::Batch>;
/// A reader of a [`RowBinding`].
type RowBindingReader =
    BindingReader<Timestamp, <RowRowSpine<Timestamp, Diff> as TraceReader>::Batch>;

#[mz_ore::test]
fn an_unbound_reader_reads_as_an_empty_chain_at_the_minimum() {
    let holds = HoldAccumulation::new();
    let binding = RowBinding::new(holds.clone());
    let minimum = Antichain::from_elem(Timestamp::MIN);
    let mut reader = binding.reader(Antichain::from_elem(Timestamp::from(3_u64)));
    assert!(!binding.is_bound());
    assert_eq!(
        holds.frontier(),
        Antichain::from_elem(Timestamp::from(3_u64)),
        "the logical hold lives in the accumulation"
    );

    let mut upper = Antichain::new();
    reader.read_upper(&mut upper);
    assert_eq!(upper, minimum);
    let mut spans = 0;
    reader.map_spans(|_| spans += 1);
    assert_eq!(spans, 0);
    assert_eq!(
        reader.get_physical_compaction(),
        minimum.borrow(),
        "an unbound reader holds physically at the minimum, as a trace handle starts"
    );
    assert_eq!(
        reader
            .spans_through(minimum.borrow())
            .map(|spans| spans.len()),
        Some(0)
    );

    let message = mz_ore::panic::catch_unwind_str(std::panic::AssertUnwindSafe(|| {
        reader.spans_through(Antichain::from_elem(Timestamp::from(1_u64)).borrow())
    }))
    .map(|_| ())
    .expect_err("a read past the minimum must panic");
    assert!(
        message.contains("read through an unbound binding past the minimum"),
        "unexpected panic: {message}"
    );

    drop(reader);
    assert_eq!(
        holds.frontier(),
        Antichain::new(),
        "a dropped reader releases"
    );
}

#[mz_ore::test]
fn an_import_built_before_binding_replays_the_chain_once_bound() {
    use std::sync::mpsc;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;
    use timely::dataflow::operators::capture::{Capture, Event};

    timely::execute_directly(move |worker| {
        let (published, mut input, keep) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("late oks");
            (adopt_fresh(&arranged), input, arranged.trace.clone())
        });
        for t in 0..3 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }

        let holds = HoldAccumulation::new();
        let binding = RowBinding::new(holds.clone());
        let reader = binding.reader(Antichain::from_elem(Timestamp::from(0_u64)));
        let (tx, rx) = mpsc::channel();
        let mut seen = Vec::new();
        let drain = |seen: &mut Vec<(Row, Timestamp, Diff)>| {
            for event in rx.try_iter() {
                if let Event::Messages(_, data) = event {
                    seen.extend(data);
                }
            }
        };
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            reader
                .import_frontier_core(
                    scope.clone(),
                    "late import",
                    Antichain::from_elem(Timestamp::from(0_u64)),
                    Antichain::new(),
                )
                .as_collection(|k: DatumSeq, v: DatumSeq| {
                    Row::pack(k.into_iter().chain(v.into_iter()))
                })
                .inner
                .probe_with(&probe)
                .capture_into(tx.clone());
        });
        drop(reader);

        // The import holds `as_of` from construction, so connecting now replays `{0}`, never the
        // empty frontier that would release the writer for good.
        let (pusher, replay) = mz_timely_util::capture::progress_link();
        let replayed = ProbeHandle::new();
        let replay_token = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (stream, token) = replay.replay(scope.clone(), "late holds");
            stream.probe_with(&replayed);
            token
        });
        holds.connect(pusher);

        for _ in 0..64 {
            worker.step();
        }
        drain(&mut seen);
        assert!(seen.is_empty(), "an unbound import produced output");
        replayed.with_frontier(|frontier| {
            assert_eq!(
                frontier.to_vec(),
                vec![Timestamp::from(0_u64)],
                "an unbound import's logical hold is missing from the accumulation"
            )
        });
        probe.with_frontier(|frontier| {
            assert_eq!(
                frontier.to_vec(),
                vec![Timestamp::MIN],
                "an unbound import moved its capability"
            )
        });

        binding
            .bind(Arc::clone(&published.shared))
            .expect("the point's since is at or below the readers' holds");
        let upper = published.shared.upper();
        let mut steps = 0;
        while probe.with_frontier(|frontier| frontier.to_owned()) != upper {
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "the bound import did not reach {upper:?}");
        }
        drain(&mut seen);
        let tick_row = Row::pack_slice(&[Datum::Int32(-1), Datum::String("tick")]);
        let expected: Vec<_> = (0..3_u64)
            .map(|t| (tick_row.clone(), Timestamp::from(t), Diff::ONE))
            .collect();
        assert_eq!(
            consolidate(seen.iter().cloned()),
            expected,
            "the bound import replayed the wrong chain"
        );
        assert_eq!(
            holds.frontier(),
            upper,
            "the import's logical hold follows the frontier it acknowledged"
        );
        let mut steps = 0;
        while replayed.with_frontier(|frontier| frontier.to_owned()) != upper {
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "the replayed holds did not reach {upper:?}");
        }

        drop(replay_token);
        drop_dataflows(worker);
        drop((keep, input));
    });
}

/// The port of `crate::sharing`'s `join_over_point_adopted_late_matches_direct` onto a
/// [`Binding`]: the join captures the import's trace at construction, before A's point exists.
#[mz_ore::test]
fn a_join_over_an_import_bound_late_matches_direct() {
    use std::sync::mpsc;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};

    let a: Vec<(i64, &str, u64, i64)> = vec![
        (1, "a", 0, 1),
        (2, "b", 0, 1),
        (3, "c", 1, 1),
        (1, "a", 2, -1),
    ];
    let b: Vec<(i64, &str, u64, i64)> = vec![(1, "x", 0, 1), (2, "y", 1, 1), (3, "z", 2, 1)];
    let seal = 3u64;
    let expected = direct_join(&a, &b);

    let (tx, rx) = mpsc::channel();
    timely::execute_directly(move |worker| {
        let (pub_b, keep_b, _agent_b) = publish_updates(worker, &b, seal, "late join B");
        let hb = pub_b.handle();
        let binding_a = RowBinding::new(HoldAccumulation::new());
        let ha = binding_a.reader(Antichain::from_elem(Timestamp::from(0_u64)));

        let as_of = Antichain::from_elem(Timestamp::from(0_u64));
        let until = Antichain::from_elem(Timestamp::from(seal));
        let probe = ProbeHandle::new();
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let arr_a = ha.import_frontier_core(
                scope.clone(),
                "import A (unbound)",
                as_of.clone(),
                until.clone(),
            );
            let arr_b = hb.import_frontier_core(scope.clone(), "import B", as_of, until);
            let joined = arr_a.join_core(arr_b, |key, v1, v2| {
                let row = Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
                Some(row)
            });
            joined.inner.probe_with(&probe).capture_into(tx.clone());
        });

        for _ in 0..64 {
            worker.step();
        }
        assert!(
            probe.less_than(&Timestamp::from(1_u64)),
            "join advanced past time 0 before A was bound"
        );

        let (pub_a, keep_a, _agent_a) = publish_updates(worker, &a, seal, "late join A");
        binding_a
            .bind(Arc::clone(&pub_a.shared))
            .expect("the point's since is at or below the readers' holds");

        let seal_ts = Timestamp::from(seal);
        let mut steps = 0;
        while probe.less_than(&seal_ts) {
            let _keep = (&keep_a, &keep_b);
            worker.step();
            steps += 1;
            assert!(steps < 10_000, "join did not seal through {seal_ts:?}");
        }
        drop_dataflows(worker);
    });

    let got = consolidate(rx.extract().into_iter().flat_map(|(_, d)| d));
    assert_eq!(
        got, expected,
        "join over an import bound late diverged from the direct join"
    );
}

#[mz_ore::test]
fn holds_before_connect_are_pushed_at_connect_and_follow_every_change() {
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Probe;

    timely::execute_directly(move |worker| {
        let holds = HoldAccumulation::new();
        let binding = RowBinding::new(holds.clone());
        let at_three = binding.reader(Antichain::from_elem(Timestamp::from(3_u64)));
        let mut at_five = at_three.clone();
        at_five.set_logical_compaction(Antichain::from_elem(Timestamp::from(5_u64)).borrow());
        assert_eq!(
            holds.frontier(),
            Antichain::from_elem(Timestamp::from(3_u64))
        );

        let (pusher, source) = mz_timely_util::capture::progress_link();
        let probe = ProbeHandle::new();
        let token = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (stream, token) = source.replay(scope.clone(), "holds");
            stream.probe_with(&probe);
            token
        });
        let settle_at = |worker: &mut timely::worker::Worker, expected: Vec<Timestamp>| {
            let mut steps = 0;
            while probe.with_frontier(|frontier| frontier.to_vec()) != expected {
                worker.step();
                steps += 1;
                assert!(
                    steps < 10_000,
                    "the replayed holds did not reach {expected:?}"
                );
            }
        };
        settle_at(worker, vec![Timestamp::MIN]);

        holds.connect(pusher);
        settle_at(worker, vec![Timestamp::from(3_u64)]);
        drop(at_three);
        settle_at(worker, vec![Timestamp::from(5_u64)]);
        drop(at_five);
        settle_at(worker, vec![]);
        drop(token);
    });
}

#[mz_ore::test]
fn an_import_refuses_a_point_compacted_past_its_as_of() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("compacted oks");
            (arranged.trace.clone(), adopt_fresh(&arranged), input)
        });
        let target = Antichain::from_elem(Timestamp::from(10_u64));
        writer.set_logical_compaction(target.borrow());
        writer.set_physical_compaction(target.borrow());
        tick(
            worker,
            &mut input,
            Timestamp::from(10_u64),
            Timestamp::from(11_u64),
        );

        let binding = RowBinding::new(HoldAccumulation::new());
        let reader = binding.reader(Antichain::from_elem(Timestamp::from(0_u64)));
        worker.dataflow::<Timestamp, _, _>(|scope| {
            reader.import_frontier_core(
                scope.clone(),
                "stale import",
                Antichain::from_elem(Timestamp::from(0_u64)),
                Antichain::new(),
            );
        });
        // The import's hold, minted at construction, is below the point's `since`.
        assert_eq!(
            binding.bind(Arc::clone(&published.shared)),
            Err(target),
            "the bind refuses a point compacted past a reader's hold"
        );
        assert!(
            !binding.is_bound(),
            "a refused bind leaves the binding unbound"
        );
        drop_dataflows(worker);
    });
}

#[mz_ore::test]
fn a_physical_request_before_binding_holds_from_the_chain_coverage() {
    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("physical oks");
            (arranged.trace.clone(), adopt_fresh(&arranged), input)
        });
        // Release the writer's own physical hold, so the point's physical holds are the reader's.
        writer.set_physical_compaction(Antichain::new().borrow());
        for t in 0..5 {
            tick(
                worker,
                &mut input,
                Timestamp::from(t),
                Timestamp::from(t + 1),
            );
        }
        let coverage = published.shared.upper();
        assert_eq!(coverage, Antichain::from_elem(Timestamp::from(5_u64)));
        assert_eq!(published.physical_holds(), Antichain::new());

        let holds = HoldAccumulation::new();
        let binding = RowBinding::new(holds.clone());
        let mut reader = binding.reader(Antichain::from_elem(Timestamp::from(0_u64)));
        reader.set_physical_compaction(Antichain::from_elem(Timestamp::from(2_u64)).borrow());
        assert_eq!(
            published.physical_holds(),
            Antichain::new(),
            "an unbound reader holds nothing on any point"
        );

        binding
            .bind(Arc::clone(&published.shared))
            .expect("the point's since is at or below the readers' holds");
        assert_eq!(
            published.physical_holds(),
            coverage,
            "the physical hold starts at the request joined with the chain coverage"
        );
        assert_eq!(
            holds.frontier(),
            Antichain::from_elem(Timestamp::from(0_u64)),
            "the logical hold stays in the accumulation"
        );

        reader.set_physical_compaction(Antichain::from_elem(Timestamp::from(7_u64)).borrow());
        assert_eq!(
            published.physical_holds(),
            Antichain::from_elem(Timestamp::from(7_u64))
        );
        drop(reader);
        assert_eq!(published.physical_holds(), Antichain::new());
        drop(input);
    });
}

/// A reader's physical hold never exceeds a frontier its consumer already saw through `map_spans`,
/// whether the reader was minted before the bind or on a bound binding, so a cut there still
/// succeeds after the writer merges past it.
#[mz_ore::test]
fn a_bound_reader_holds_physically_at_the_coverage_its_consumer_saw() {
    use timely::PartialOrder;

    timely::execute_directly(move |worker| {
        let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
            let arranged =
                collection.mz_arrange::<RowRowBatcher<_, _>, RowRowSpine<_, _>>("coverage oks");
            (arranged.trace.clone(), adopt_fresh(&arranged), input)
        });
        // The writer's agent stands in for the trace manager and lets the spine merge up to its
        // upper, so the readers' holds are what keep boundaries. Releasing it entirely instead
        // would drop the spine's physical frontier to the empty antichain, which a later hold
        // cannot rewind.
        let mut ticks = 0_u64..;
        let mut advance = |worker: &mut timely::worker::Worker, count: usize| {
            for t in ticks.by_ref().take(count) {
                tick(
                    worker,
                    &mut input,
                    Timestamp::from(t),
                    Timestamp::from(t + 1),
                );
                writer
                    .set_physical_compaction(Antichain::from_elem(Timestamp::from(t + 1)).borrow());
            }
        };
        advance(worker, 3);

        // The consumer's view of a reader: the coverage `map_spans` shows, then the writer seals
        // and merges, then the physical frontier and a cut at that coverage.
        let check =
            |reader: &mut RowBindingReader,
             worker: &mut timely::worker::Worker,
             advance: &mut dyn FnMut(&mut timely::worker::Worker, usize)| {
                let mut seen = Antichain::from_elem(Timestamp::MIN);
                reader.map_spans(|span| seen.clone_from(span.upper()));
                advance(worker, 30);
                assert!(
                    PartialOrder::less_equal(&reader.get_physical_compaction(), &seen.borrow()),
                    "the physical hold {:?} passed the coverage {seen:?} the consumer saw",
                    reader.get_physical_compaction(),
                );
                let spans = reader
                    .spans_through(seen.borrow())
                    .expect("a cut at the seen coverage");
                assert!(
                    spans.iter().all(|span| PartialOrder::less_equal(
                        &span.upper().borrow(),
                        &seen.borrow()
                    ))
                );
            };

        let binding = RowBinding::new(HoldAccumulation::new());
        let mut before = binding.reader(Antichain::from_elem(Timestamp::from(0_u64)));
        binding
            .bind(Arc::clone(&published.shared))
            .expect("the point's since is at or below the readers' holds");
        check(&mut before, worker, &mut advance);

        let mut after = binding.reader(Antichain::from_elem(Timestamp::from(0_u64)));
        check(&mut after, worker, &mut advance);

        // A clone inherits the original's hold, not the coverage at the clone.
        let held = after.get_physical_compaction().to_owned();
        advance(worker, 30);
        let mut clone = after.clone();
        assert_eq!(clone.get_physical_compaction(), held.borrow());
        drop((before, after, clone));
        drop(input);
    });
}
