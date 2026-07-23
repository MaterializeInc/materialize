// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sharing arrangements across timely runtimes.
//!
//! An arrangement is normally readable only from the worker that maintains it: its batches are
//! reference counted with `Rc` and its trace handle is `Rc<RefCell<..>>`, both pinned to one
//! thread. This module lets a worker publish an arrangement whose batches are `Arc`'d (and whose
//! contents are `Send + Sync`) through a *publication point*, from which readers on other threads
//! take consistent snapshots or import the arrangement into a second timely runtime.
//!
//! The unit that crosses the thread boundary is not the
//! [`Spine`](differential_dataflow::trace::implementations::spine_fueled::Spine), which holds
//! thread-local state and has a single writer, but the spine's *contents*: a chain of immutable
//! `Arc`'d batches together with the trace's `since` and `upper` frontiers. Because batches are
//! immutable, a chain plus frontiers is a self-describing, consistent view. When the publishing
//! worker later merges batches, a reader holding an older chain is unaffected: its `Arc`s keep the
//! pre-merge batches alive until it drops them.
//!
//! ## Pieces
//!
//! * [`PublishArrangement::publish`] attaches a publisher to an arrangement on the owning worker and
//!   returns a [`Published`] whose [`Published::handle`] hands out `Clone + Send`
//!   [`SharedTraceHandle`]s.
//! * [`SharedTraceHandle`] implements [`TraceReader`], so it drives compaction and cursors like any
//!   trace handle. [`SharedTraceHandle::snapshot_at`] serves a point or full-scan read from any
//!   thread. [`SharedTraceHandle::import_snapshot_at`] replays the shared arrangement into another
//!   scope.
//!
//! ## Compaction
//!
//! Every reader registers logical and physical holds. The publisher forwards their *meet* (the
//! greatest lower bound, so the trace never compacts past the least reader's hold) to its own
//! `TraceAgent`, which is the sole writer of the trace's compaction frontiers. Publishing itself
//! carries no compaction floor: with no readers the publisher advances its hold to the
//! writer-driven frontier (the meet of the other agents' holds), so the trace compacts and merges
//! as the writer advances. The publisher never forwards the empty frontier, which would
//! irreversibly release the trace.
//!
//! Ported from the Materialize differential-dataflow fork's
//! `differential_dataflow::operators::arrange::sharing`, so the fork can eventually be dropped. The
//! only behavioral difference is the `publish`/`publish_named` entry points, which the fork exposes
//! as inherent methods on differential's `Arranged` and this port exposes as the
//! [`PublishArrangement`] extension trait, since Materialize cannot add inherent methods to the
//! foreign `Arranged` type.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

use differential_dataflow::lattice::{Lattice, antichain_meet};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent, TraceReplayInstruction};
use differential_dataflow::trace::cursor::{CursorList, Navigable, cursor_list};
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{BatchReader, TraceReader};
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::{Operator, source};
use timely::progress::Antichain;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::scheduling::activate::SyncActivator;

/// The queue and wakeup for one importer registered against a publication point.
struct ImportQueue<Tr: TraceReader> {
    /// Replay instructions the publisher appends and the importer drains, mirroring the local
    /// arrange replay queue. Batches carry a hint time that lower-bounds their updates.
    instructions: VecDeque<TraceReplayInstruction<Tr>>,
    /// Wakes the importer's source operator on the reader's worker when new instructions arrive.
    activator: SyncActivator,
}

/// State shared between a publisher (on the owning worker) and its readers (on any thread).
///
/// The `chain`, `since`, and `upper` are always updated together under the lock, so every reader
/// observes a frontier-consistent view.
struct SharedTraceState<Tr: TraceReader> {
    /// The published chain, sourced from `map_batches`: contiguous descriptions including the
    /// seal-only empty batches that never travel on the arrangement stream. Coverage is at least
    /// `upper` (within a worker step it may briefly run ahead, never behind).
    chain: Vec<Tr::Batch>,
    /// Logical compaction frontier of the published view. Reads at times not beyond `since` are not
    /// accurate. A snapshot must pick a time at or beyond it.
    since: Antichain<Tr::Time>,
    /// Seal frontier: the join of the chain's batch uppers. Batches strictly below `upper` are
    /// complete and readable.
    upper: Antichain<Tr::Time>,
    /// Per-registration logical holds. The publisher forwards their meet, falling back to the
    /// writer-driven frontier when empty (never the destructive empty meet of zero holds).
    logical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Per-registration physical holds, forwarded independently of the logical holds.
    physical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Importer queues, keyed by registration id. A handle may back several registrations, so this
    /// is keyed separately from any handle.
    queues: BTreeMap<usize, ImportQueue<Tr>>,
    /// Monotonic source of registration ids for holds and queues.
    next_id: usize,
    /// Set when the publisher drops. A terminal empty frontier is enqueued to each importer, so
    /// readers close only after draining what was already published.
    closed: bool,
    /// Set true by [`PublishArrangement::adopt`] when a publisher takes over this point. A point
    /// created by [`Published::placeholder`] and never adopted stays false, which the registry uses
    /// to decide it may evict the never-adopted slot when its last reader leaves. Read under this
    /// same mutex so an eviction check that also reads the slot's Arc strong count observes a
    /// consistent (adopted, count) pair. See [`crate::sharing::ArrangementSharingRegistry::evict_unadopted`].
    adopted: bool,
}

impl<Tr: TraceReader> SharedTraceState<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// The meet of `holds`, or `fallback` when there are none.
    ///
    /// Never returns the empty meet of zero holds, which is the empty frontier and would tell the
    /// trace to compact everything, permanently releasing the publisher's capability.
    fn compaction_target(
        holds: &BTreeMap<usize, Antichain<Tr::Time>>,
        fallback: &Antichain<Tr::Time>,
    ) -> Antichain<Tr::Time> {
        let mut iter = holds.values();
        match iter.next() {
            None => fallback.clone(),
            Some(first) => {
                let mut acc = first.clone();
                for hold in iter {
                    acc = antichain_meet(&acc.borrow()[..], &hold.borrow()[..]);
                }
                acc
            }
        }
    }
}

/// A publication point paired with its live [`Condvar`], so peek waiters can block for `upper` to
/// advance without a lost-wakeup race: the publisher signals under the same lock it uses to move
/// `upper`.
struct SharedTrace<Tr: TraceReader> {
    state: Mutex<SharedTraceState<Tr>>,
    upper_changed: Condvar,
    /// Total peer count (workers-per-process times processes) of the scope that published this
    /// arrangement. Set once at publish time and never mutated afterward, so `import` reads it
    /// without taking `state`'s lock. Pairwise import (importer worker `i` reads publisher worker
    /// `i`) is sound only when an importing scope shards keys the same way, which requires this to
    /// match the importing scope's own `peers()`.
    peers: usize,
}

type SharedTraceRef<Tr> = Arc<SharedTrace<Tr>>;

impl<Tr: TraceReader> SharedTrace<Tr> {
    /// A fresh publication point: empty chain, `since` and `upper` at the minimum time, no reader
    /// holds, no publisher attached.
    ///
    /// Shared by [`Published::placeholder`], which leaves the point unbacked for a later
    /// [`PublishArrangement::adopt`], and [`PublishArrangement::publish_named`], for which it is the
    /// starting state before adopting with no prior reader.
    fn new_empty(peers: usize) -> Self {
        SharedTrace {
            state: Mutex::new(SharedTraceState {
                chain: Vec::new(),
                // NOTE: `Antichain::from_elem(minimum)`, never `Antichain::new()`. The empty
                // frontier reads as "complete through the end of time", making every snapshot wait
                // vacuously true and returning empty results instead of blocking.
                since: Antichain::from_elem(batch_min::<Tr>()),
                upper: Antichain::from_elem(batch_min::<Tr>()),
                logical_holds: BTreeMap::new(),
                physical_holds: BTreeMap::new(),
                queues: BTreeMap::new(),
                next_id: 0,
                closed: false,
                adopted: false,
            }),
            upper_changed: Condvar::new(),
            peers,
        }
    }
}

/// The result of publishing an arrangement. Holding it keeps the publication point registered;
/// dropping it does not stop the publisher (the publisher lives with its dataflow), but no further
/// handles can be minted from it.
pub struct Published<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Published<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub fn handle(&self) -> SharedTraceHandle<Tr> {
        SharedTraceHandle::register(Arc::clone(&self.shared))
    }

    /// Creates an unbacked publication point: an empty chain with `since` and `upper` at the minimum
    /// time and no publisher.
    ///
    /// A reader may immediately mint handles ([`Self::handle`]) and build imports over it, but they
    /// produce nothing (the import frontier stays at the minimum) until a publisher adopts this point
    /// via [`PublishArrangement::adopt`] and begins refreshing it. Adoption fills the same `Arc`, so
    /// a handle captured by value at construction (as a differential join captures its input trace)
    /// observes the filled chain: the handle is a live proxy into the shared state, not a snapshot.
    ///
    /// `peers` must equal the total peer count of the scope that later adopts the point, the same
    /// invariant [`SharedTraceHandle::import_snapshot_at`] enforces.
    pub fn placeholder(peers: usize) -> Self {
        Published {
            shared: Arc::new(SharedTrace::new_empty(peers)),
        }
    }

    /// Closes this publication point without a publisher attached, terminating every importer.
    ///
    /// Marks the point closed and pushes a terminal empty frontier ([`Antichain::new`]) to each
    /// importer queue, waking them so they drain and drop their capability. This is the never-adopted
    /// counterpart to `Publisher::drop`, which performs the same close for an adopted point. Use it
    /// for a placeholder whose index creation was cancelled before it published: without it the
    /// placeholder's importers would hold their frontier at the minimum forever.
    ///
    /// Idempotent. Closing an already-closed point re-enqueues the terminal frontier, which a drained
    /// importer ignores. Do NOT call this on a point that will still be adopted: the terminal frontier
    /// would tear down importers that a later publisher is meant to feed.
    pub fn close(&self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.closed = true;
            // Empty frontier = sealed through the end of time, the same terminal signal `Publisher::drop`
            // sends. Never `from_elem(minimum)`, which would read as "no progress" and wedge instead.
            let empty = Antichain::new();
            for queue in state.queues.values_mut() {
                queue
                    .instructions
                    .push_back(TraceReplayInstruction::Frontier(empty.clone()));
                let _ = queue.activator.activate();
            }
        }
        self.shared.upper_changed.notify_all();
    }

    /// Reads this point's adoption flag and runs `f` while holding the state lock, returning both.
    ///
    /// The registry pairs the flag with the slot's Arc strong count under one lock acquisition so an
    /// adopter in flight is always observed as either adopted or still-referenced. `f` MUST NOT lock
    /// this point's state again (it would deadlock) nor acquire the registry map lock (it would invert
    /// the map-then-state lock order the eviction path relies on).
    pub(crate) fn adopted_and<R>(&self, f: impl FnOnce() -> R) -> (bool, R) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        let adopted = state.adopted;
        (adopted, f())
    }
}

/// A `Clone + Send` reader of a published arrangement.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone carries an independent registration: cloning mints a fresh id
/// and copies the source's holds, so two consumers of one import cannot release each other's holds.
pub struct SharedTraceHandle<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    /// This handle's hold registration id.
    id: usize,
    /// This handle's own logical frontier, mirrored into `logical_holds[id]`. Kept locally so
    /// `get_logical_compaction` can return a borrow.
    logical: Antichain<Tr::Time>,
    /// This handle's own physical frontier, mirrored into `physical_holds[id]`.
    physical: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Registers a fresh hold at the current published `since` and returns a handle for it.
    fn register(shared: SharedTraceRef<Tr>) -> Self {
        let (id, since) = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            let since = state.since.clone();
            state.logical_holds.insert(id, since.clone());
            state.physical_holds.insert(id, since.clone());
            (id, since)
        };
        Self {
            shared,
            id,
            logical: since.clone(),
            physical: since,
        }
    }

    /// Takes a consistent snapshot of the published arrangement as of `time`, blocking until `upper`
    /// passes `time`.
    ///
    /// Works from any thread. Returns `None` when the snapshot cannot serve `time`, which is either
    /// the publisher closed before `upper` passed `time`, or compaction has advanced `since` beyond
    /// `time` so the accumulation at `time` is no longer accurate. The gate on `since` mirrors the
    /// single-runtime peek path, which errors when the compaction frontier is beyond the read time
    /// rather than returning coalesced results. A caller that needs to tell the two `None` cases
    /// apart should inspect `since` before the read.
    pub fn snapshot_at(&self, time: &Tr::Time) -> Option<TraceSnapshot<Tr>> {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        // `upper` not less-equal `time` means all updates at `time` are sealed.
        while state.upper.less_equal(time) {
            if state.closed {
                return None;
            }
            state = self
                .shared
                .upper_changed
                .wait(state)
                .expect("shared trace poisoned");
        }
        // `since` beyond `time` means times at `time` have been coalesced and a read there would be
        // inaccurate. Fail to `None` rather than serve stale data.
        if !state.since.less_equal(time) {
            return None;
        }
        Some(TraceSnapshot {
            chain: state.chain.clone(),
            since: state.since.clone(),
            upper: state.upper.clone(),
        })
    }

    /// The published arrangement's current `(since, upper)` frontiers, read under the state lock.
    ///
    /// A point-in-time observation for diagnostics. Either frontier may have advanced by the time
    /// the caller inspects the returned values.
    pub fn frontiers(&self) -> (Antichain<Tr::Time>, Antichain<Tr::Time>) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        (state.since.clone(), state.upper.clone())
    }

    /// Writes this handle's logical or physical hold into the shared registry.
    fn update_hold(&self, logical: bool) {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        if logical {
            state.logical_holds.insert(self.id, self.logical.clone());
        } else {
            state.physical_holds.insert(self.id, self.physical.clone());
        }
    }
}

impl<Tr: TraceReader> Clone for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    fn clone(&self) -> Self {
        // A clone must be an independent hold: `import` returns `Arranged { trace: handle.clone() }`
        // and `Arranged` is itself `Clone`, so distinct downstream operators drive compaction on
        // distinct clones. Sharing one hold slot would let the faster operator release the slower
        // one's hold. This mirrors `TraceAgent::clone`, which registers an independent counted hold.
        let id = {
            let mut state = self.shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            state.logical_holds.insert(id, self.logical.clone());
            state.physical_holds.insert(id, self.physical.clone());
            id
        };
        Self {
            shared: Arc::clone(&self.shared),
            id,
            logical: self.logical.clone(),
            physical: self.physical.clone(),
        }
    }
}

impl<Tr: TraceReader> Drop for SharedTraceHandle<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.logical_holds.remove(&self.id);
            state.physical_holds.remove(&self.id);
        }
    }
}

impl<Tr: TraceReader> TraceReader for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<Vec<Self::Batch>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        // A clean cut of the published chain: all non-empty batches whose upper is not beyond
        // `upper`, and none whose lower is beyond `upper`. Empty batches are dropped, as
        // `Spine::batches_through` does.
        let mut out = Vec::new();
        for batch in state.chain.iter() {
            // A batch whose lower is beyond the cut, and everything after it in the totally
            // ordered chain, lies past `upper`. Empty batches never carry updates to read.
            if timely::PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
                break;
            }
            if !batch.is_empty() {
                // Fail-stop on a batch that straddles the cut (`lower < upper < batch.upper()`),
                // matching `Spine::batches_through`. Returning it would hand back updates at times
                // not before `upper`, corrupting a downstream `cursor_through` consumer such as
                // `join`. The published chain is totally ordered by description, so this cut is
                // clean unless a caller requested a frontier that is not batch-aligned.
                assert!(
                    timely::PartialOrder::less_equal(&batch.upper().borrow(), &upper),
                    "batches_through: upper straddles batch"
                );
                out.push(batch.clone());
            }
        }
        Some(out)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        self.logical = frontier.to_owned();
        self.update_hold(true);
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        self.physical = frontier.to_owned();
        self.update_hold(false);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.physical.borrow()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        for batch in state.chain.iter() {
            f(batch);
        }
    }
}

/// Smallest time, used only to satisfy the borrow in the cut check for empty lower frontiers.
fn batch_min<Tr: TraceReader>() -> Tr::Time {
    <Tr::Time as timely::progress::Timestamp>::minimum()
}

/// An owned, consistent snapshot of a published arrangement: an immutable chain plus its frontiers.
///
/// Holding it pins the chain's batches, keeping their memory alive even as the publishing worker
/// merges. Dropping it releases them, from whatever thread drops last.
pub struct TraceSnapshot<Tr: TraceReader> {
    chain: Vec<Tr::Batch>,
    since: Antichain<Tr::Time>,
    upper: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> TraceSnapshot<Tr> {
    /// The logical compaction frontier of the snapshot. Times not beyond it are not accurate.
    pub fn since(&self) -> AntichainRef<'_, Tr::Time> {
        self.since.borrow()
    }
    /// The seal frontier of the snapshot. Times strictly below it are complete.
    pub fn upper(&self) -> AntichainRef<'_, Tr::Time> {
        self.upper.borrow()
    }
    /// A cursor merging the snapshot's batch cursors, with the batches as its storage.
    pub fn cursor(&self) -> (CursorList<<Tr::Batch as Navigable>::Cursor>, Vec<Tr::Batch>)
    where
        Tr::Batch: Navigable,
    {
        cursor_list(self.chain.clone())
    }
}

/// Publishes an [`Arranged`] arrangement through a publication point on its owning worker.
///
/// In the differential fork these are inherent methods on `Arranged`. Materialize cannot add
/// inherent methods to the foreign `Arranged` type, so it exposes them as this extension trait
/// instead. Bring it into scope at a call site to use `arranged.publish()`.
pub trait PublishArrangement<Tr: TraceReader> {
    /// Publishes this arrangement through a publication point on the owning worker.
    ///
    /// Attaches a publisher operator to the arrangement stream. On each activation the publisher
    /// refreshes the published chain, `since`, and `upper` from the trace, appends newly arrived
    /// batches to importer queues, and forwards the meet of reader holds to the trace's compaction.
    /// The returned [`Published`] mints [`SharedTraceHandle`]s that read the arrangement from any
    /// thread.
    fn publish(&self) -> Published<Tr>;

    /// [`PublishArrangement::publish`], with a name for the publisher operator.
    fn publish_named(&self, name: &str) -> Published<Tr>;

    /// Installs this arrangement's publisher into an existing `placeholder` publication point,
    /// created by [`Published::placeholder`], rather than minting a fresh one.
    ///
    /// This is the late-binding path: a reader may create the placeholder and build handles and
    /// imports over it before this arrangement is rendered. Those imports produce nothing (their
    /// frontier stays at the minimum) until adoption begins the refresh loop, at which point the
    /// already-registered importer queues fill from the same publisher iteration that serves any
    /// later-registered reader.
    ///
    /// Requires the adopting scope's total peer count to equal the placeholder's, panicking
    /// otherwise.
    fn adopt(&self, placeholder: &Published<Tr>);

    /// [`PublishArrangement::adopt`], with a name for the publisher operator.
    fn adopt_named(&self, placeholder: &Published<Tr>, name: &str);
}

impl<'scope, Tr> PublishArrangement<Tr> for Arranged<'scope, TraceAgent<Tr>>
where
    Tr: differential_dataflow::trace::Trace + 'static,
    Tr::Batch: Send + Sync,
    Tr::Time: Lattice + Clone + Send + Sync,
{
    fn publish(&self) -> Published<Tr> {
        // Call the trait method explicitly. While the differential fork is still `[patch]`-active,
        // `Arranged` also has the fork's inherent `publish_named`, and an inherent method would win
        // over this trait method, returning the fork's `Published` instead of ours.
        PublishArrangement::publish_named(self, "PublishShared")
    }

    fn publish_named(&self, name: &str) -> Published<Tr> {
        // Publishing is adopting a fresh publication point with no prior reader.
        let peers = self.stream.scope().peers();
        let published = Published {
            shared: Arc::new(SharedTrace::new_empty(peers)),
        };
        self.adopt_named(&published, name);
        published
    }

    fn adopt(&self, placeholder: &Published<Tr>) {
        // Call the trait method explicitly, for the same reason as `publish` above.
        PublishArrangement::adopt_named(self, placeholder, "PublishShared");
    }

    fn adopt_named(&self, placeholder: &Published<Tr>, name: &str) {
        assert_eq!(
            self.stream.scope().peers(),
            placeholder.shared.peers,
            "adopt requires equal total peers (workers_per_process * num_processes)"
        );

        // Mark the point adopted before installing the publisher. The registry reads this flag (under
        // the same state mutex) to distinguish a live, published slot from a never-adopted placeholder
        // it may evict. Setting it before the caller can drop its `Arc<SharedIndexArrangement>` clone
        // (the borrow of `placeholder` keeps that clone alive across this call) is what makes the
        // eviction race sound. See `crate::sharing::ArrangementSharingRegistry::evict_unadopted`.
        {
            let mut state = placeholder
                .shared
                .state
                .lock()
                .expect("shared trace poisoned");
            state.adopted = true;
        }

        // The publisher owns a `TraceAgent` clone: its read capability is the aggregate lease for
        // all readers, so the trace cannot compact or drop out from under them.
        let mut agent = self.trace.clone();

        let publisher = Publisher {
            shared: Arc::clone(&placeholder.shared),
        };

        let sink_shared = Arc::clone(&placeholder.shared);
        self.stream.clone().sink(
            timely::dataflow::channels::pact::Pipeline,
            name,
            move |(input, frontier)| {
                // Keep `publisher` alive with the operator, so operator (dataflow) drop closes the
                // publication point.
                let _publisher = &publisher;

                // Batches arriving on the stream, each with a capability time that lower-bounds the
                // batch's updates. Empty seal batches do not travel the stream; they are picked up
                // from the trace below.
                let mut arrived: Vec<(Tr::Batch, Tr::Time)> = Vec::new();
                input.for_each(|cap, data| {
                    let hint = cap.time().clone();
                    for batch in data.drain(..) {
                        arrived.push((batch, hint.clone()));
                    }
                });

                // The stream frontier is the authoritative upper. It never leads the batches
                // delivered on the stream, unlike the trace's `map_batches` upper, which can run
                // ahead within a worker step and strand the importer's capability below a
                // not-yet-emitted batch.
                let upper = frontier.frontier().to_owned();

                // Refresh the chain from the trace, but only accept batches the stream frontier
                // has reached, so the published upper and the enqueued batches stay consistent.
                let mut chain = Vec::new();
                agent.map_batches(|batch| {
                    if timely::PartialOrder::less_equal(&batch.upper().borrow(), &upper.borrow()) {
                        chain.push(batch.clone());
                    }
                });
                // Contract: publishing carries no independent compaction floor. The trace's writer
                // (and, in Materialize, the controller) drive `since` through their own trace
                // handles. Only a live importer's registered hold may hold the trace back, and it
                // releases on drop. The publisher keeps a holding agent solely so importer holds
                // have somewhere to forward to, so that hold must FOLLOW the writer rather than pin
                // the trace.
                //
                // The writer-driven frontier is the meet of all other agents' holds, i.e. the
                // frontier the trace would compact to if the publisher were not holding it. The
                // `TraceBox` accumulates every agent's hold in a `MutableAntichain`; cloning it and
                // subtracting the publisher's own contribution yields that meet. Advancing the
                // publisher's hold to it lets the trace compact and merge as the writer advances.
                //
                // NOTE: the per-batch `since` from `map_batches` cannot serve as this source. Those
                // frontiers advance only when the Spine actually compacts, which the publisher's own
                // pinning hold prevents, so they stay frozen at the publish-time `since`. Reading
                // the trace-box meet breaks that circularity.
                let publisher_logical = agent.get_logical_compaction().to_owned();
                let publisher_physical = agent.get_physical_compaction().to_owned();
                // The writer-driven frontier for one dimension: the meet of the accumulated holds
                // with the publisher's own contribution removed. An empty result means the
                // publisher is the sole holder. Never forward the empty frontier: it would compact
                // everything and release the publisher's capability, so fall back to the publisher's
                // current hold and keep the current floor.
                let others_meet = |accumulated: &MutableAntichain<Tr::Time>,
                                   own: &Antichain<Tr::Time>| {
                    let mut others = accumulated.clone();
                    others.update_iter(own.iter().map(|t| (t.clone(), -1)));
                    if others.frontier().is_empty() {
                        own.clone()
                    } else {
                        others.frontier().to_owned()
                    }
                };
                let (writer_logical, writer_physical) = {
                    let trace_box = agent.trace_box_unstable();
                    let trace_box = trace_box.borrow();
                    (
                        others_meet(trace_box.logical_compaction(), &publisher_logical),
                        others_meet(trace_box.physical_compaction(), &publisher_physical),
                    )
                };

                let (logical_target, physical_target) = {
                    let mut state = sink_shared.state.lock().expect("shared trace poisoned");

                    for (batch, hint) in arrived.drain(..) {
                        for queue in state.queues.values_mut() {
                            queue.instructions.push_back(TraceReplayInstruction::Batch(
                                batch.clone(),
                                Some(hint.clone()),
                            ));
                        }
                    }

                    let upper_advanced = state.upper != upper;
                    if upper_advanced {
                        for queue in state.queues.values_mut() {
                            queue
                                .instructions
                                .push_back(TraceReplayInstruction::Frontier(upper.clone()));
                        }
                    }

                    // Fall back to the writer-driven frontier (not the publisher's frozen hold) when
                    // there are no reader holds, so with zero readers the target follows the writer.
                    let logical = SharedTraceState::<Tr>::compaction_target(
                        &state.logical_holds,
                        &writer_logical,
                    );
                    let physical = SharedTraceState::<Tr>::compaction_target(
                        &state.physical_holds,
                        &writer_physical,
                    );

                    state.chain = chain;
                    // Publish the trace's real logical compaction after we forward `logical` below.
                    // Agent compaction only advances (joins), so the publisher's hold becomes
                    // `join(publisher_logical, logical)`. The trace's real compaction is the meet of
                    // every agent's hold: the publisher's post-forward hold and the writer-driven
                    // meet of the others. Publishing exactly that keeps the gate in step with the
                    // trace, so a reader hold keeps its own frontier readable rather than being
                    // raced past by the writer. It is never below the real compaction (it equals
                    // it), so a handle registering in this window cannot latch an anti-conservative
                    // `since` that claims accuracy at already-merged times.
                    let publisher_after = publisher_logical.join(&logical);
                    state.since =
                        antichain_meet(&publisher_after.borrow()[..], &writer_logical.borrow()[..]);
                    state.upper = upper;

                    // Wake importers and any peek waiters.
                    for queue in state.queues.values() {
                        let _ = queue.activator.activate();
                    }
                    if upper_advanced {
                        sink_shared.upper_changed.notify_all();
                    }

                    (logical, physical)
                };

                // Apply compaction to the agent OUTSIDE the lock: `set_physical_compaction` can run
                // an unbounded merge synchronously, which must not block concurrent readers.
                agent.set_logical_compaction(logical_target.borrow());
                agent.set_physical_compaction(physical_target.borrow());
            },
        );
    }
}

/// Guard that marks the publication point closed when the publisher operator drops, waking readers
/// so they drain and shut down.
struct Publisher<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Drop for Publisher<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.closed = true;
            let empty = Antichain::new();
            for queue in state.queues.values_mut() {
                queue
                    .instructions
                    .push_back(TraceReplayInstruction::Frontier(empty.clone()));
                let _ = queue.activator.activate();
            }
        }
        self.shared.upper_changed.notify_all();
    }
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr: 'static,
    Tr::Time: Lattice + Clone,
    Tr::Batch: Navigable,
{
    /// Imports the published arrangement restricted to `[as_of, until)`, presented at `as_of`.
    ///
    /// This is the port of differential's `TraceAgent::import_frontier_core` onto the shared trace.
    /// It registers a replay queue seeded with the current chain and drains it as the publisher
    /// appends, wrapping the emitted batches in [`BatchFrontier`] and the trace in [`TraceFrontier`],
    /// both advanced to `as_of` and bounded by `until`. Every update at a time not beyond `as_of`
    /// therefore coalesces to `as_of`, so pre-`as_of` retractions cancel and a downstream monotonic
    /// operator sees only the accumulation at `as_of`.
    ///
    /// The wrapper advances times on read, so the shared `Arc` batches are reused as-is, never
    /// re-arranged.
    ///
    /// Requires `scope`'s total peer count (workers-per-process times processes) to equal the
    /// publisher's, panicking otherwise. Pairwise import (importer worker `i` reads publisher
    /// worker `i`) is sound only when both sides shard by the same `key.hashed() % peers`; a
    /// mismatched peer count would silently read the wrong shard instead of failing loudly.
    ///
    /// The importer registration is owned by the source operator, so dropping the import dataflow
    /// deregisters it and releases its holds even while other handle clones and the reader worker
    /// live on.
    ///
    /// # Why replay rather than a one-shot emit
    ///
    /// The returned [`Arranged`]'s `stream` and `trace` must stay consistent: the trace is the
    /// accumulation of the stream, and their frontiers advance together. A differential join relies
    /// on this (it computes `A.stream x B.trace + B.stream x A.trace`, counting each match once only
    /// when the trace never runs ahead of the stream). Driving the output capability off the replayed
    /// `Frontier` instructions keeps `stream.frontier == trace.upper`. A one-shot emit that shipped
    /// the whole chain and then dropped straight to the empty frontier would leave the pre-populated
    /// shared trace ahead of the stream, and the join would read the same record from both and double
    /// it.
    ///
    /// For a single-time interactive read pass `until = as_of.step_forward()`: the capability then
    /// drops once the trace's frontier passes `as_of`, so the one-shot result completes. An empty
    /// `until` performs no bounding and the import stays live with the trace.
    pub fn import_snapshot_at<'scope>(
        &self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        as_of: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> Arranged<'scope, TraceFrontier<SharedTraceHandle<Tr>>> {
        assert_eq!(
            scope.peers(),
            self.shared.peers,
            "shared-trace import requires equal total peers (workers_per_process * num_processes)"
        );

        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        let shared = Arc::clone(&self.shared);

        let stream = source(scope, name, move |capability, info| {
            let activator = scope.worker().sync_activator_for(info.address.to_vec());

            // Register under one lock acquisition: mint an id, seed the queue with the current
            // chain (hint `minimum`, as the local replay does for historical batches) followed by
            // the current upper, and install the queue. Later batches append; earlier ones are
            // seeded. Nothing is missed or duplicated.
            let reg_id = {
                let mut state = shared.state.lock().expect("shared trace poisoned");
                let reg_id = state.next_id;
                state.next_id += 1;
                let mut instructions = VecDeque::new();
                for batch in state.chain.iter() {
                    instructions.push_back(TraceReplayInstruction::Batch(
                        batch.clone(),
                        Some(batch_min::<Tr>()),
                    ));
                }
                instructions.push_back(TraceReplayInstruction::Frontier(state.upper.clone()));
                // If the publisher already closed, its one-shot terminal frontier has been and gone,
                // so seed our own. Otherwise a late importer would drain the chain and then wait
                // forever for a frontier that never arrives, leaking its capability. Mirrors the
                // `state.closed` guard in `snapshot_at`.
                if state.closed {
                    instructions.push_back(TraceReplayInstruction::Frontier(Antichain::new()));
                }
                state.queues.insert(
                    reg_id,
                    ImportQueue {
                        instructions,
                        activator,
                    },
                );
                reg_id
            };

            // Deregisters the queue when the source operator (and thus this closure) drops.
            let _guard = QueueGuard {
                shared: Arc::clone(&shared),
                reg_id,
            };

            let mut capabilities = Some(CapabilitySet::new());
            capabilities.as_mut().unwrap().insert(capability);

            move |output| {
                let _guard = &_guard;
                let mut drained = Vec::new();
                {
                    let mut state = shared.state.lock().expect("shared trace poisoned");
                    if let Some(queue) = state.queues.get_mut(&reg_id) {
                        drained.extend(queue.instructions.drain(..));
                    }
                }

                if let Some(caps) = capabilities.as_mut() {
                    for instruction in drained {
                        match instruction {
                            TraceReplayInstruction::Frontier(frontier) => {
                                // Bound the read at `until`: once the trace's frontier reaches it, drop
                                // the capability so a single-time read completes. Otherwise track the
                                // trace's `upper`, keeping the stream frontier equal to the trace upper
                                // (the consistency the join depends on). The empty frontier is the
                                // publisher's terminal signal and likewise drops the capability.
                                if frontier.is_empty()
                                    || timely::PartialOrder::less_equal(&until, &frontier)
                                {
                                    capabilities = None;
                                    break;
                                }
                                caps.downgrade(&frontier.borrow()[..]);
                            }
                            TraceReplayInstruction::Batch(batch, hint) => {
                                if let Some(time) = hint {
                                    if !batch.is_empty() {
                                        // Emit under a capability delayed to the batch's hint. The
                                        // `BatchFrontier` wrapper advances times to `as_of` and drops
                                        // times at or beyond `until` on read, so the stream presents
                                        // the same `[as_of, until)` view as the trace.
                                        let cap = caps.delayed(&time);
                                        output.session(&cap).give(BatchFrontier::make_from(
                                            batch,
                                            as_of.borrow(),
                                            until.borrow(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Arranged { stream, trace }
    }
}

/// Deregisters an importer's replay queue when its source operator drops.
struct QueueGuard<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    reg_id: usize,
}

impl<Tr: TraceReader> Drop for QueueGuard<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.queues.remove(&self.reg_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use differential_dataflow::trace::Cursor;
    use mz_ore::cast::CastFrom;
    use mz_repr::{Datum, Diff, Row, Timestamp};
    use mz_row_spine::{RowRowBatcher, RowRowBuilder};
    use mz_timely_util::columnation::ColumnationChunker;

    use crate::extensions::arrange::MzArrange;
    use crate::typedefs::RowRowSpine;

    use super::*;

    /// Smoke test: arrange two rows into a `RowRow` spine, publish it through the extension trait,
    /// mint a `Send` handle, and read the sealed contents back via `snapshot_at`.
    ///
    /// Publisher and reader share one worker stepped to completion inside `execute_directly`. The
    /// returned handle keeps the published chain alive through its `Arc`s, so the snapshot observes
    /// the sealed rows even after the publishing worker tears down. This is the single-worker
    /// publish + snapshot path adapted from the fork's `tests/sharing.rs`; the full cross-thread and
    /// import-replay coverage lives in `crate::sharing`.
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
                // The extension trait under test: resolve `publish` explicitly so it cannot bind to
                // the fork's inherent method while the fork remains patched.
                let published = PublishArrangement::publish(&arranged);
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
                let published = PublishArrangement::publish(&arranged);
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
    /// concurrently on its own thread so the reader's `snapshot_at` genuinely blocks on the
    /// `Condvar` wait and is woken by the publisher's `notify_all`, rather than observing an
    /// already-sealed chain.
    #[mz_ore::test]
    fn snapshot_at_blocks_until_upper_passes_time() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::mpsc;

        let key = |k: i32| Row::pack_slice(&[Datum::Int32(k)]);
        let val = |v: &str| Row::pack_slice(&[Datum::String(v)]);

        let (handle_tx, handle_rx) =
            mpsc::channel::<SharedTraceHandle<RowRowSpine<Timestamp, Diff>>>();
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
                let published = PublishArrangement::publish(&arranged);
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

    /// Publishing must not pin compaction. With no registered reader holds, as the trace's writer
    /// advances its compaction the publisher's own forwarded hold must follow, so the trace
    /// actually compacts.
    ///
    /// Ported from the differential-dataflow primitive's own `tests/sharing.rs`
    /// `publish_without_readers_does_not_pin_compaction`. This exercises the zero-reader-holds
    /// fallback branch of the publisher's compaction forwarding (`SharedTraceState::compaction_target`
    /// falling back to the writer-driven frontier), which no other test in this crate or
    /// `crate::sharing` covers: `crate::render`'s `interactive_import_hold_releases_on_drop` always
    /// has a live reader hold present at some point in the scenario.
    #[mz_ore::test]
    fn publish_without_readers_does_not_pin_compaction() {
        timely::execute_directly(move |worker| {
            // Keep a writer handle (a plain `TraceAgent` clone) alongside the publication, and mint
            // no `SharedTraceHandle` until after compaction: that keeps `logical_holds`/
            // `physical_holds` empty, so the publisher has zero registered reader holds throughout.
            let (mut writer, published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
                let arranged = collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("no-readers oks");
                let writer = arranged.trace.clone();
                let published = PublishArrangement::publish(&arranged);
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

            // The writer requests compaction to 10, as a controller would, then a fresh tick
            // reactivates the publisher so it recomputes its forwarded `since` from the writer-driven
            // frontier (there being no reader holds to meet against).
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

        let (handle_tx, handle_rx) =
            mpsc::channel::<SharedTraceHandle<RowRowSpine<Timestamp, Diff>>>();
        // `execute` requires a `Sync` closure; `mpsc::Sender` is not `Sync`.
        let handle_tx = Mutex::new(handle_tx);

        // Publisher runtime: two worker threads, so the publishing scope's `peers()` is 2. The
        // publisher's `peers` is captured at `publish` time from `self.stream.scope().peers()`, so
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
                let published = PublishArrangement::publish(&arranged);
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

    /// The delayed-capability panic, reproduced against the real importer source operator.
    ///
    /// The buggy two-source feed can enqueue a `Frontier` (sourced from the trace, which runs
    /// ahead) before a `Batch` (sourced from the lagging stream) whose hint lies below that
    /// frontier. The importer downgrades its `CapabilitySet` to the frontier, then `delayed(hint)`
    /// with `hint` below it panics. Here we drive a real published arrangement's importer and inject
    /// exactly that hazardous ordering into its replay queue, using a real non-empty `Arc` batch, so
    /// the panic is raised by the production drain-and-emit loop rather than a reconstruction.
    ///
    /// This drain-and-emit loop, including the `caps.delayed(hint)` call that panics, is shared code
    /// between every mode of `import_snapshot_at`, not something specific to a since-removed live
    /// import. An unbounded `until` (`Antichain::new()`) reproduces the exact same failure as the
    /// pre-bound-checking import once did: with a finite `until` the frontier-reached check in
    /// `import_snapshot_at` would drop the capability before the injected batch is ever replayed,
    /// masking the hazard instead of exercising it.
    #[mz_ore::test]
    #[should_panic(expected = "failed to create a delayed capability")]
    fn frontier_ahead_of_batch_trips_delayed_capability() {
        timely::execute_directly(move |worker| {
            let (published, mut input) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (input, collection) = scope.new_collection::<(Row, Row), Diff>();
                let arranged = collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("hazard oks");
                let published = PublishArrangement::publish(&arranged);
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
            // `Batch(1)` then panics in `delayed`. Activate the importer so it drains this step.
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
            let published = PublishArrangement::publish(&arranged);
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
                let row =
                    Row::pack_slice(&[Datum::Int64(ka), Datum::String(la), Datum::String(rb)]);
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
                    let row =
                        Row::pack(key.into_iter().chain(v1.into_iter()).chain(v2.into_iter()));
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

        let got: Vec<(Row, Timestamp, Diff)> =
            rx.extract().into_iter().flat_map(|(_, d)| d).collect();
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
                let published = PublishArrangement::publish(&arranged);
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
}
