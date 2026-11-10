// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A registry of published index arrangements, shared by one maintenance worker and its
//! interactive peer.
//!
//! An index arrangement is normally readable only from the timely worker that maintains it. When
//! `crate::render` publishes a maintained index through `crate::shared_trace` it records the
//! resulting `Published` points here, keyed by the index's [`GlobalId`] and the
//! [`CreationNumber`] of its incarnation, so a reader on another thread or runtime can read the
//! same arrangement.
//!
//! Only the publisher creates slots. The reading runtime records each peer index as a
//! [`PeerIndex`] whose readers exist before the slot does, and binds it to the slot of exactly its
//! incarnation once the publisher reported applying the command that created it.
//!
//! A process holds one registry per local worker ordinal, and each runtime's worker with that
//! ordinal holds a clone of it. Pairing worker `i` of one runtime with worker `i` of the other is
//! sound only because both runtimes run the same number of workers per process at the same process
//! ordinal, so both sides shard keys by the same `key.hashed() % peers`.

use std::any::Any;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::{Thread, ThreadId};

use differential_dataflow::trace::TraceReader;
use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_timely_util::capture::{ProgressPusher, ProgressSource, progress_link};
use mz_timely_util::shared_trace::{Binding, HoldAccumulation};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::progress::{Antichain, Timestamp as _};

use crate::arrangement::manager::TraceBundle;
use crate::command_channel::CommandNumber;
use crate::shared_trace::{
    PeerErrsBinding, PeerErrsReader, PeerOksBinding, PeerOksReader, Published, SharedErrsHandle,
    SharedOksHandle, adopt_trace,
};
use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent, RowRowSpine};

/// The number of the command that created an incarnation of a collection.
///
/// A `CreateDataflow` creates the incarnations of its exports, and `CreateInstance` those of the
/// logging indexes. An incarnation a reconnect retains keeps its number, so together with its
/// `GlobalId` the number names one incarnation on both runtimes of a process.
pub(crate) type CreationNumber = CommandNumber;

/// What both runtimes derive from the command that created an incarnation, which a reader compares
/// against the slot it binds.
///
/// Equal on both runtimes for one incarnation. A difference means the two runtimes assigned the
/// same creation number to different commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotCheck {
    /// The `as_of` of the dataflow that exports the index.
    pub(crate) as_of: Antichain<Timestamp>,
    /// The exported index, `None` for a logging index.
    pub(crate) index: Option<IndexDesc<LirScalarExpr>>,
}

impl SlotCheck {
    /// The check of a logging index, whose dataflow reads from the minimum time.
    pub(crate) fn logging() -> Self {
        SlotCheck {
            as_of: Antichain::from_elem(Timestamp::minimum()),
            index: None,
        }
    }
}

/// The published `oks`/`errs` arrangements of one maintained index on one worker.
///
/// An index's `oks` is always a `RowRowSpine` and its `errs` always an `ErrSpine`. Holding the
/// `Published` values keeps the publication points registered and lets us mint further handles.
pub struct SharedIndexArrangement {
    /// What the publisher knew of the incarnation when it created the slot.
    check: SlotCheck,
    /// The published `oks` arrangement.
    pub(crate) oks: Published<RowRowSpine<Timestamp, Diff>>,
    /// The published `errs` arrangement.
    pub(crate) errs: Published<ErrSpine<Timestamp, Diff>>,
    /// The link that carries the reading runtime's logical holds on both arrangements to the
    /// publisher, as one frontier.
    hold_link: HoldLink,
}

/// The two ends of a slot's hold link, each until its side takes it.
///
/// Each end is handed out once instead of cloned, as [`CommandProgressLink`]'s are. The link exists
/// from the slot's creation, so the peer can push before the publisher replays, and the replay
/// starts from the link's head.
struct HoldLink {
    pusher: Mutex<Option<ProgressPusher<Timestamp>>>,
    source: Mutex<Option<ProgressSource<Timestamp>>>,
}

impl Default for HoldLink {
    fn default() -> Self {
        let (pusher, source) = progress_link();
        Self {
            pusher: Mutex::new(Some(pusher)),
            source: Mutex::new(Some(source)),
        }
    }
}

/// The registry's state: the published slots and the two workers attached to them. One lock covers
/// all three. Every critical section is a few map operations, and the publisher takes it on every
/// seal of every point it backs, never per record.
#[derive(Default)]
struct Inner {
    /// Weak, so an entry lives exactly as long as a publisher or a reader holds its slot. Dead
    /// entries are pruned whenever a slot is created.
    ///
    /// Keyed by incarnation, so two incarnations of one id coexist for as long as both are held.
    map: BTreeMap<(GlobalId, CreationNumber), Weak<SharedIndexArrangement>>,
    /// The worker that publishes here, `None` until it attaches.
    publisher: Option<ThreadId>,
    /// The worker that reads here and is woken on a publication, `None` until it attaches.
    ///
    /// Unparked rather than activated: every timely allocator's `await_events` bottoms out in
    /// `std::thread::park`, and a root-path `SyncActivator` would additionally mark the worker's
    /// dataflows schedulable, which is work this wake does not need.
    reader: Option<Thread>,
}

/// A registry of published index arrangements, shared by one maintenance worker and its
/// interactive peer.
///
/// Cloning shares the same underlying map. A slot is an `Arc` held by its publisher, through a
/// `PublishToken`, and by its readers. The registry itself holds none, so a slot nobody holds
/// is gone, and the publisher's trace stops publishing into its points at its next mutation.
#[derive(Clone, Default)]
pub struct ArrangementSharingRegistry {
    inner: Arc<Mutex<Inner>>,
    /// The link over which the maintenance worker attached here reports the commands it applied
    /// to its interactive peer.
    command_progress: Arc<CommandProgressLink>,
}

/// The two ends of a registry's command progress link, each until a worker takes it.
///
/// Each end is handed out once instead of cloned, because an untaken or second source holds the
/// link's head, which retains every event pushed after it.
struct CommandProgressLink {
    pusher: Mutex<Option<ProgressPusher<CommandNumber>>>,
    source: Mutex<Option<ProgressSource<CommandNumber>>>,
}

impl Default for CommandProgressLink {
    fn default() -> Self {
        let (pusher, source) = progress_link();
        Self {
            pusher: Mutex::new(Some(pusher)),
            source: Mutex::new(Some(source)),
        }
    }
}

impl SharedIndexArrangement {
    /// Whether the traces that published into this slot have closed or dropped.
    fn is_frozen(&self) -> bool {
        self.oks.is_frozen() || self.errs.is_frozen()
    }

    /// Releases the reading runtime's hold on this slot for good, unless a peer took the hold link
    /// before, for an incarnation that peer will never read.
    ///
    /// A slot no peer binds would otherwise keep its traces from compacting for as long as it is
    /// published, see the hold in [`ArrangementSharingRegistry::publish`].
    pub(crate) fn release_hold_link(&self) {
        if let Some(mut pusher) = self.take_hold_pusher() {
            pusher.advance_to(Antichain::new().borrow());
        }
    }

    /// Takes the end of the hold link the reading runtime pushes its holds into, `None` if taken
    /// before.
    fn take_hold_pusher(&self) -> Option<ProgressPusher<Timestamp>> {
        self.hold_link
            .pusher
            .lock()
            .expect("hold link poisoned")
            .take()
    }

    /// Takes the end of the hold link the publisher replays.
    ///
    /// # Panics
    ///
    /// Panics if taken before. A slot is created by its one publication, which takes it.
    fn take_hold_source(&self) -> ProgressSource<Timestamp> {
        self.hold_link
            .source
            .lock()
            .expect("hold link poisoned")
            .take()
            .expect("a slot is published once")
    }
}

impl ArrangementSharingRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates one registry per local worker ordinal, for a process running `workers_per_process`
    /// workers per runtime.
    pub fn per_worker(workers_per_process: usize) -> Vec<Self> {
        (0..workers_per_process).map(|_| Self::new()).collect()
    }

    /// Takes the end of the command progress link the maintenance worker reports through.
    ///
    /// # Panics
    ///
    /// Panics if taken before.
    pub(crate) fn take_command_progress_pusher(&self) -> ProgressPusher<CommandNumber> {
        self.command_progress
            .pusher
            .lock()
            .expect("command progress link poisoned")
            .take()
            .expect("the command progress pusher is taken once")
    }

    /// Takes the end of the command progress link the interactive worker replays.
    ///
    /// # Panics
    ///
    /// Panics if taken before.
    pub(crate) fn take_command_progress_source(&self) -> ProgressSource<CommandNumber> {
        self.command_progress
            .source
            .lock()
            .expect("command progress link poisoned")
            .take()
            .expect("the command progress source is taken once")
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("registry poisoned")
    }

    /// The slot of the incarnation of `id` created by command `creation`, if someone holds it.
    pub(crate) fn slot(
        &self,
        id: GlobalId,
        creation: CreationNumber,
    ) -> Option<Arc<SharedIndexArrangement>> {
        self.lock().map.get(&(id, creation)).and_then(Weak::upgrade)
    }

    /// The live slot of `id` with the largest creation number. Test-only.
    #[cfg(test)]
    fn newest_slot(inner: &Inner, id: &GlobalId) -> Option<Arc<SharedIndexArrangement>> {
        inner
            .map
            .range((*id, CreationNumber::MIN)..=(*id, CreationNumber::MAX))
            .rev()
            .find_map(|(_, slot)| slot.upgrade())
    }

    /// The largest creation number of a live slot of `id`. Test-only.
    #[cfg(test)]
    pub(crate) fn newest_creation(&self, id: &GlobalId) -> Option<CreationNumber> {
        self.lock()
            .map
            .range((*id, CreationNumber::MIN)..=(*id, CreationNumber::MAX))
            .rev()
            .find_map(|(&(_, creation), slot)| (slot.strong_count() > 0).then_some(creation))
    }

    /// Publishes index `id`'s `oks` and `errs` traces as the incarnation created by command
    /// `creation`, and wakes readers waiting on `id`. `check` describes the incarnation for the
    /// readers to compare, and `scope` must belong to the worker that maintains the traces.
    ///
    /// Creates a fresh slot under `(id, creation)`. A live slot under that key is a second
    /// publication of one incarnation, a protocol violation, which is reported, and the fresh slot
    /// replaces it. Each half signals its own seal. A peek whose result is an error carries its
    /// data on the errs arrangement, whose frontier is held back until the error is emitted, so an
    /// oks-only signal would leave that peek parked.
    ///
    /// Every incarnation gets its own publication points, including an index that re-exports
    /// another's arrangement. The point's writer frontier and peer holds are per collection, and
    /// the controller compacts two collections independently even when they share a trace. A
    /// recreated re-export publishes new points over the same trace while readers of the old
    /// incarnation may still hold the old ones, which the trace keeps backing.
    ///
    /// Builds in `scope` the replay of the slot's hold link, through which the reading runtime's
    /// holds keep the traces from compacting past them, see [`replay_reader_holds`].
    ///
    /// The publication lasts as long as the returned token, and so does the replay.
    #[must_use]
    pub(crate) fn publish(
        &self,
        id: GlobalId,
        creation: CreationNumber,
        check: SlotCheck,
        scope: Scope<'_, Timestamp>,
        oks: &RowRowAgent<Timestamp, Diff>,
        errs: &ErrAgent<Timestamp, Diff>,
    ) -> PublishToken {
        let slot = Arc::new(SharedIndexArrangement {
            check,
            oks: Published::new(),
            errs: Published::new(),
            hold_link: Default::default(),
        });
        let replaced = {
            let mut inner = self.lock();
            inner.map.retain(|_, slot| slot.strong_count() > 0);
            inner.map.insert((id, creation), Arc::downgrade(&slot))
        };
        if replaced.is_some_and(|replaced| replaced.strong_count() > 0) {
            mz_ore::soft_panic_or_log!("incarnation {creation} of {id} published twice");
        }
        let registry = self.clone();
        adopt_trace(oks, scope.worker(), &slot.oks, move || registry.notify());
        let registry = self.clone();
        adopt_trace(errs, scope.worker(), &slot.errs, move || registry.notify());
        // The reading runtime holds the index from its own copy of this `CreateDataflow`, which it
        // can reach arbitrarily later. Until then nothing stops this runtime from applying a
        // compaction past a dataflow that runtime has yet to render: the controller drops its read
        // hold for a cancelled read before the reader renders it. So the replay holds from here,
        // ahead of any `AllowCompaction` for the index on this runtime's stream, at the minimum
        // until the reading runtime's holds arrive.
        //
        // NOTE: Dropping a re-export's token releases its replayed holds while the interactive
        // runtime may still read the re-export. The shared trace stays protected by the base
        // index's replayed hold: the trace compacts to the meet of the holds across every point
        // it backs (`TraceBox::sync_holds` in `mz_timely_util::shared_trace`), the controller
        // holds the base at or below the re-export's `since`, and the interactive runtime applies
        // the base's compactions in command order.
        let minimum = Antichain::from_elem(Timestamp::minimum());
        let replay = replay_reader_holds(
            scope,
            id,
            slot.take_hold_source(),
            slot.oks.shared.reader_at_least(&minimum),
            slot.errs.shared.reader_at_least(&minimum),
        );
        self.notify();
        PublishToken {
            registry: self.clone(),
            slot: Some(slot),
            _replay: replay,
        }
    }

    /// Readers of the incarnation of `id` created by `creation` at `as_of`, through a peer bound
    /// at once, which hold logically at `as_of` and nothing physically. Test-only.
    ///
    /// # Panics
    ///
    /// Panics if nobody holds the slot, or a published `since` is beyond `as_of`.
    #[cfg(test)]
    pub(crate) fn peer_readers_at(
        &self,
        id: GlobalId,
        creation: CreationNumber,
        as_of: &Antichain<Timestamp>,
    ) -> (PeerOksReader, PeerErrsReader) {
        let slot = self.slot(id, creation).expect("a published slot");
        let mut peer = PeerIndex::new(creation, slot.check.clone());
        // Readers at the slot's `as_of` first, so the bind connects a hold.
        let (oks, errs) = peer.readers();
        peer.bind(id, slot);
        let mut at_as_of = (oks.reader_at_least(as_of), errs.reader_at_least(as_of));
        at_as_of
            .0
            .set_physical_compaction(Antichain::new().borrow());
        at_as_of
            .1
            .set_physical_compaction(Antichain::new().borrow());
        at_as_of
    }

    /// The bundle over [`Self::peer_readers_at`]. Test-only.
    #[cfg(test)]
    pub(crate) fn peer_bundle_at(
        &self,
        id: GlobalId,
        creation: CreationNumber,
        as_of: &Antichain<Timestamp>,
    ) -> TraceBundle {
        let (oks, errs) = self.peer_readers_at(id, creation, as_of);
        TraceBundle::shared(oks, errs)
    }

    /// The published `oks` `since` of `id`. Test-only.
    ///
    /// # Panics
    ///
    /// Panics if nobody holds a slot for `id`.
    #[cfg(test)]
    pub(crate) fn published_since(&self, id: &GlobalId) -> Antichain<Timestamp> {
        let inner = self.lock();
        let slot = Self::newest_slot(&inner, id).expect("a slot for the id");
        slot.oks.shared.since()
    }

    /// The published `oks` `since` of the incarnation of `id` created by `creation`. Test-only.
    ///
    /// # Panics
    ///
    /// Panics if nobody holds that slot.
    #[cfg(test)]
    pub(crate) fn published_since_at(
        &self,
        id: GlobalId,
        creation: CreationNumber,
    ) -> Antichain<Timestamp> {
        let slot = self.slot(id, creation).expect("a published slot");
        slot.oks.shared.since()
    }

    /// Mints reader handles for `id`, if published. Test-only.
    #[cfg(test)]
    pub(crate) fn handles(&self, id: &GlobalId) -> Option<(SharedOksHandle, SharedErrsHandle)> {
        let inner = self.lock();
        let slot = Self::newest_slot(&inner, id)?;
        Some((slot.oks.handle(), slot.errs.handle()))
    }

    /// The accumulated `oks` logical holds registered against `id`, if published.
    ///
    /// Test-only. Minting a handle to observe the published frontiers cannot distinguish a live
    /// reader hold from a frontier that happens to sit there, and that distinction is what says
    /// whether an import is still protected. Empty when every hold has released.
    #[cfg(test)]
    pub(crate) fn published_logical_holds(&self, id: &GlobalId) -> Option<Antichain<Timestamp>> {
        let inner = self.lock();
        let slot = Self::newest_slot(&inner, id)?;
        Some(slot.oks.logical_holds())
    }

    /// How far the readers' holds keep the publishing runtime from compacting, over every
    /// arrangement published here, `oks` and `errs` alike.
    ///
    /// Returns the largest gap between an arrangement's requested and applied logical compaction
    /// frontiers, and how many arrangements have any gap at all. The gap is in timestamp units,
    /// milliseconds for an index in the epoch-millisecond timeline. Every published id is a point
    /// of its own, so a trace published under several ids, as a re-export's is, counts once per
    /// id. A point whose requested frontier is empty is counted as held but contributes no gap:
    /// its collection is being dropped and has no finite frontier left to subtract from.
    ///
    /// Only logical compaction is measured. Physical holds and the batches a reader's chain pins
    /// are not.
    ///
    /// NOTE: A frozen slot is skipped, which is a blind spot. Its writer closed, so its points
    /// stopped publishing frontiers and its gap cannot be measured, but its trace lives on while a
    /// same-thread agent holds it, for example a dataflow importing the index, and the readers'
    /// holds on the frozen points still hold that trace back.
    ///
    /// Takes the registry lock and up to four point locks per live slot. The caller throttles it.
    pub(crate) fn hold_gaps(&self) -> (u64, usize) {
        let inner = self.lock();
        let mut max_gap = 0;
        let mut held = 0;
        let live = inner.map.values().filter_map(Weak::upgrade);
        for slot in live.filter(|slot| !slot.is_frozen()) {
            for (applied, requested) in
                [slot.oks.logical_frontiers(), slot.errs.logical_frontiers()]
            {
                match (applied.as_option(), requested.as_option()) {
                    (Some(applied), Some(requested)) => {
                        let gap = u64::from(*requested).saturating_sub(u64::from(*applied));
                        if gap > 0 {
                            held += 1;
                            max_gap = max_gap.max(gap);
                        }
                    }
                    (Some(_), None) => held += 1,
                    _ => {}
                }
            }
        }
        (max_gap, held)
    }

    /// Attaches the current thread as the worker that publishes here.
    ///
    /// Worker `i` of each runtime holds registry `i`, and both sides must be a single worker. Two
    /// publishing workers hold different shards of an index and would back one slot with both, and
    /// a second reader would take over the wake, so the first one's waiting peeks never wake.
    ///
    /// # Panics
    ///
    /// Panics if another thread attached as the publisher.
    pub(crate) fn attach_publisher(&self) {
        let current = std::thread::current().id();
        let previous = self.lock().publisher.replace(current);
        assert!(
            previous.is_none_or(|previous| previous == current),
            "a sharing registry has one publishing worker"
        );
    }

    /// Attaches the current thread as the worker that reads here, which a publication unparks. A
    /// peek waiting on a seal is served by the worker's next sweep, so the unpark is all it needs.
    ///
    /// # Panics
    ///
    /// Panics if another thread attached as the reader, see [`Self::attach_publisher`].
    pub(crate) fn attach_reader(&self) {
        let current = std::thread::current();
        let mut inner = self.lock();
        if let Some(previous) = &inner.reader {
            assert_eq!(
                previous.id(),
                current.id(),
                "a sharing registry has one reading worker"
            );
        }
        inner.reader = Some(current);
    }

    /// Unparks the reading worker, if one is attached.
    ///
    /// [`Self::publish`] calls this once a slot's publishers are installed, each publisher calls it
    /// again on every seal, and an unpublication calls it once more, since a peek waiting on a
    /// shared trace is re-examined only when its worker runs.
    ///
    /// No wake is lost. The publisher updates the point before it calls this, and the worker
    /// re-reads every waiting peek's trace each time it runs, before it parks. A wake that lands
    /// after that read is remembered by `unpark`, so the worker's next park returns at once and it
    /// reads again.
    pub(crate) fn notify(&self) {
        if let Some(reader) = &self.lock().reader {
            // `unpark` coalesces by itself: the thread keeps one token, and a wake while it runs
            // costs an atomic swap without a syscall.
            reader.unpark();
        }
    }
}

/// The publisher's hold on a published slot. Dropping it ends the publication.
pub(crate) struct PublishToken {
    registry: ArrangementSharingRegistry,
    /// `Some` until dropped.
    slot: Option<Arc<SharedIndexArrangement>>,
    /// Shuts the replay of the slot's hold link down when dropped, which releases the holds it
    /// keeps.
    _replay: Rc<dyn Any>,
}

impl Drop for PublishToken {
    fn drop(&mut self) {
        // Dropped before the wake, so a woken reader finds the slot gone unless it holds the slot
        // itself.
        drop(self.slot.take());
        self.registry.notify();
    }
}

/// The reading runtime's side of one incarnation of a peer index: bindings whose readers exist
/// from the moment the peer is recorded, and that bind the incarnation's slot once the publishing
/// runtime created it.
///
/// Every reader of both bindings holds in one accumulation, which the bind connects to the slot's
/// hold link. Until then the publisher holds on the readers' behalf, see
/// [`ArrangementSharingRegistry::publish`].
pub(crate) struct PeerIndex {
    /// The number of the command that created the incarnation.
    creation: CreationNumber,
    /// Compared against the slot's at the bind.
    check: SlotCheck,
    holds: HoldAccumulation<Timestamp>,
    oks: PeerOksBinding,
    errs: PeerErrsBinding,
    /// The bound slot, `None` until bound.
    slot: Option<Arc<SharedIndexArrangement>>,
}

impl PeerIndex {
    /// An unbound peer of the incarnation that command `creation` created, described by `check`.
    pub(crate) fn new(creation: CreationNumber, check: SlotCheck) -> Self {
        let holds = HoldAccumulation::new();
        PeerIndex {
            creation,
            check,
            oks: Binding::new(holds.clone()),
            errs: Binding::new(holds.clone()),
            holds,
            slot: None,
        }
    }

    /// The number of the command that created the incarnation.
    pub(crate) fn creation(&self) -> CreationNumber {
        self.creation
    }

    /// Whether a slot is bound. Test-only.
    #[cfg(test)]
    pub(crate) fn is_bound(&self) -> bool {
        self.slot.is_some()
    }

    /// Readers of both bindings that hold logically at the incarnation's `as_of` and nothing
    /// physically, so the publisher keeps merging. Imports from them mint readers that do hold
    /// physically.
    ///
    /// # Panics
    ///
    /// Panics if bound and a published `since` is beyond the `as_of`.
    pub(crate) fn readers(&self) -> (PeerOksReader, PeerErrsReader) {
        let mut oks = self.oks.reader(self.check.as_of.clone());
        oks.set_physical_compaction(Antichain::new().borrow());
        let mut errs = self.errs.reader(self.check.as_of.clone());
        errs.set_physical_compaction(Antichain::new().borrow());
        (oks, errs)
    }

    /// The bundle through which the reading runtime holds and reads the index, over
    /// [`Self::readers`]. Its logical compaction is this runtime's hold on the publication, so an
    /// index's `since` never passes the `as_of` of a dataflow importing it.
    ///
    /// Taken before [`Self::bind`], so the first frontier the bind pushes is the bundle's hold.
    ///
    /// # Panics
    ///
    /// Panics if bound and a published `since` is beyond the `as_of`.
    pub(crate) fn bundle(&self) -> TraceBundle {
        let (oks, errs) = self.readers();
        TraceBundle::shared(oks, errs)
    }

    /// Binds both halves to `slot`, the slot of peer index `id` under this peer's creation number,
    /// connects the readers' holds to its hold link, and keeps it.
    ///
    /// A slot whose check differs from the peer's, a slot whose hold link another peer took, and a
    /// point compacted past the readers' holds are each a protocol violation, which is reported.
    /// The peer then stays unbound and its reads wait forever, instead of reading another
    /// incarnation or reading without a hold. A slot of another incarnation is released, see
    /// [`SharedIndexArrangement::release_hold_link`].
    pub(crate) fn bind(&mut self, id: GlobalId, slot: Arc<SharedIndexArrangement>) {
        if slot.check != self.check {
            // No peer of this process has the slot's incarnation under this number, so none ever
            // reads it.
            slot.release_hold_link();
            mz_ore::soft_panic_or_log!(
                "peer {id} at {} binds a slot of another incarnation: {:?} != {:?}",
                self.creation,
                slot.check,
                self.check,
            );
            return;
        }
        let Some(mut pusher) = slot.take_hold_pusher() else {
            mz_ore::soft_panic_or_log!(
                "a second peer for slot {id} at {}, whose peer holds its hold link",
                self.creation,
            );
            return;
        };
        // Before the holds are connected, so the publisher's initial hold still keeps the points'
        // `since` at or below every reader's hold.
        let bound = self
            .oks
            .bind(Arc::clone(&slot.oks.shared))
            .and_then(|()| self.errs.bind(Arc::clone(&slot.errs.shared)));
        // Connected even if a bind was refused, so the publisher's hold follows the readers'
        // lifetime instead of staying at the minimum. A connect while nothing holds would end the
        // stream through a soft assert, so with no reader left the hold is released directly.
        if self.holds.frontier().is_empty() {
            pusher.advance_to(Antichain::new().borrow());
        } else {
            self.holds.connect(pusher);
        }
        match bound {
            Ok(()) => self.slot = Some(slot),
            // NOTE: A refused `errs` leaves `oks` bound. Every read of the index waits on both
            // halves, so the peer reads nothing either way.
            Err(since) => mz_ore::soft_panic_or_log!(
                "peer {id} at {} binds a point compacted to {since:?}, past its readers",
                self.creation,
            ),
        }
    }
}

/// Replays `source`, a slot's hold link, in `scope` into a sink that holds `oks` and `errs`
/// logically at the replayed frontier, and drops them once it is empty. `id` names the operators.
///
/// The readers must hold nothing physically, or they pin merging. Returns the token that shuts the
/// replay down, which ends the stream and so releases the readers.
fn replay_reader_holds(
    scope: Scope<'_, Timestamp>,
    id: GlobalId,
    source: ProgressSource<Timestamp>,
    oks: SharedOksHandle,
    errs: SharedErrsHandle,
) -> Rc<dyn Any> {
    let (stream, token) = source.replay(scope, &format!("ReplayReaderHolds({id})"));
    let mut readers = Some((oks, errs));
    stream.sink(
        Pipeline,
        &format!("ReaderHolds({id})"),
        move |(input, frontier)| {
            // The link carries progress only.
            input.for_each(|_time, _data| {});
            let frontier = frontier.frontier();
            if frontier.is_empty() {
                readers = None;
            } else if let Some((oks, errs)) = &mut readers {
                oks.set_logical_compaction(frontier);
                errs.set_logical_compaction(frontier);
            }
        },
    );
    token
}

#[cfg(test)]
mod tests;
