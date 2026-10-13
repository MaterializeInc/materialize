// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A trace agent whose trace can be published to readers on other threads.
//!
//! An arrangement's trace is owned by the worker that arranges into it and is not `Send`: its
//! handles are `Rc<RefCell<..>>`, and its spine does merge work whenever it is touched, including
//! inside `roll_up` and `complete_at`, which finish merges synchronously as part of structural
//! changes. A spine behind a mutex would therefore hold that mutex for whole merges. The batches,
//! though, are immutable and reference counted, so a chain of them plus the trace's frontiers is a
//! consistent, self-describing view that any thread can read.
//!
//! [`SharedAgent`] is the agent arranging operators return in place of differential's
//! `TraceAgent`. Its [`SharedBox`] owns a plain spine on the arranging worker and its
//! [`SharedWriter`] feeds it. Same-thread agents read the spine directly, as `TraceAgent`s do.
//! Once [published](SharedAgent::publish) to a [`Shared`] point, the box mirrors the spine's batch
//! chain, `upper`, and compaction frontiers into the point after every mutation. The lock is held
//! for a chain swap, never for merge work. A trace nobody published pays no mirror cost and takes
//! no lock.
//!
//! Every handle holds the trace through one accounting. While unpublished, the box keeps the holds
//! of its same-thread agents, as differential's `TraceBox` does. Publishing moves them into the
//! point the trace first publishes to, its home, where [`SharedReader`]s on any thread register
//! theirs next to them. The box applies the meet to the spine. A reader that moves the meet wakes
//! the writer so the spine learns of it without waiting for the arrangement's next input.
//!
//! Logical compaction decides which times stay distinguishable, physical compaction which batches
//! may merge. A reader needs distinguishability at the times it reads, and a batch boundary at each
//! frontier it passes to `cursor_through`. Its physical hold therefore starts at the chain's
//! coverage, which is where an import seeded with that chain makes its first cut, and follows the
//! frontiers it acknowledges from there.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::{Logger, TraceShare};
use differential_dataflow::operators::arrange::{
    Agent, AgentWriter, Arranged, ShutdownButton, TraceReplayInstruction,
};
use differential_dataflow::trace::cursor::Navigable;
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{Batch, BatchReader, Trace, TraceReader};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::{OperatorInfo, source};
use timely::order::TotalOrder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Activator;
use timely::scheduling::activate::SyncActivator;

/// A replay instruction for an importer's queue.
enum Replay<B: BatchReader> {
    Frontier(Antichain<B::Time>),
    Batch(B),
}

/// The queue and wakeup for one importer.
///
/// The importer owns the only strong reference. The writer holds it weakly and prunes it once the
/// importer drops. The queue has its own lock, so an importer draining it never contends with the
/// writer publishing.
struct ImportQueue<B: BatchReader> {
    instructions: Mutex<VecDeque<Replay<B>>>,
    activator: SyncActivator,
}

impl<B: BatchReader> ImportQueue<B> {
    fn push(&self, instructions: impl IntoIterator<Item = Replay<B>>) {
        let mut queue = self.instructions.lock().expect("import queue poisoned");
        queue.extend(instructions);
    }

    fn activate(&self) {
        let _ = self.activator.activate();
    }
}

/// The view readers see, plus the holds every handle registers.
struct State<B: BatchReader> {
    /// The spine's batches in time order, as `map_batches` yields them.
    chain: Vec<B>,
    /// Upper of the last batch in `chain`, or the minimum time for an empty chain.
    upper: Antichain<B::Time>,
    /// The spine's logical compaction frontier. Reads at times not beyond it are not accurate.
    logical: Antichain<B::Time>,
    /// The frontier the publishing trace's same-thread agents ask for, before meeting the readers'
    /// holds.
    ///
    /// Equal to `logical` when no reader holds the trace back. Above it by exactly the amount a
    /// reader is keeping the writer from compacting, which is the coupling this publication
    /// introduces and the only place it is observable.
    writer_logical: Antichain<B::Time>,
    /// The spine's physical compaction frontier.
    physical: Antichain<B::Time>,
    /// The logical holds of every handle on this point. On the home point of a published trace that
    /// includes the trace's same-thread agents.
    holds_logical: MutableAntichain<B::Time>,
    /// The physical holds, as `holds_logical`.
    holds_physical: MutableAntichain<B::Time>,
    /// The part of `holds_logical` the publishing trace's same-thread agents registered. Read only
    /// to report `writer_logical`.
    local_logical: MutableAntichain<B::Time>,
    queues: Vec<Weak<ImportQueue<B>>>,
    /// Wakes the writer's arrange operator so a changed hold reaches the spine.
    writer_activator: Option<SyncActivator>,
    /// How many writers have attached. The latest one owns `writer_activator`.
    writers: u64,
}

impl<B: BatchReader> State<B> {
    /// The live importer queues, dropping entries whose importer has gone. Returns strong
    /// references so the caller can push and activate after releasing the state lock.
    fn live_queues(&mut self) -> Vec<Arc<ImportQueue<B>>> {
        let mut live = Vec::with_capacity(self.queues.len());
        self.queues.retain(|weak| match weak.upgrade() {
            Some(queue) => {
                live.push(queue);
                true
            }
            None => false,
        });
        live
    }
}

/// A publication point: the view of one published trace and the holds its handles register.
///
/// Created unattached, with an empty chain and frontiers at the minimum time. Readers may register
/// and import before a trace [publishes](SharedAgent::publish) to it. Their imports produce nothing
/// until then and are seeded from the trace's chain at attachment.
pub struct Shared<B: BatchReader> {
    state: Mutex<State<B>>,
}

impl<B: BatchReader> Default for Shared<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: BatchReader + Clone> Shared<B> {
    /// The published chain. Holding it pins its batches.
    pub fn chain(&self) -> Vec<B> {
        self.lock().chain.clone()
    }
}

impl<B: BatchReader> Shared<B> {
    /// An unattached publication point.
    pub fn new() -> Self {
        let minimum = Antichain::from_elem(B::Time::minimum());
        Shared {
            state: Mutex::new(State {
                chain: Vec::new(),
                upper: minimum.clone(),
                logical: minimum.clone(),
                writer_logical: minimum.clone(),
                physical: minimum,
                holds_logical: MutableAntichain::new(),
                holds_physical: MutableAntichain::new(),
                local_logical: MutableAntichain::new(),
                queues: Vec::new(),
                writer_activator: None,
                writers: 0,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<B>> {
        self.state.lock().expect("shared trace poisoned")
    }

    /// The published `upper`: batches strictly below it are complete and readable.
    pub fn upper(&self) -> Antichain<B::Time> {
        self.lock().upper.clone()
    }

    /// The published logical compaction frontier. Reads at times not beyond it are not accurate.
    pub fn since(&self) -> Antichain<B::Time> {
        self.lock().logical.clone()
    }

    /// The published `since`, and the logical compaction frontier the publishing trace's own
    /// agents asked for before the readers' holds were met into it, read together. The second is
    /// at or beyond the first, and equal to it when no reader holds this point.
    pub fn logical_frontiers(&self) -> (Antichain<B::Time>, Antichain<B::Time>) {
        let state = self.lock();
        (state.logical.clone(), state.writer_logical.clone())
    }

    /// The published `(since, upper)`, read together.
    pub fn frontiers(&self) -> (Antichain<B::Time>, Antichain<B::Time>) {
        let state = self.lock();
        (state.logical.clone(), state.upper.clone())
    }

    /// The meet of every logical hold on this point, the publishing trace's agents included.
    /// Empty when nothing holds.
    pub fn logical_holds(&self) -> Antichain<B::Time> {
        self.lock().holds_logical.frontier().to_owned()
    }

    /// The meet of every physical hold on this point, as [`Self::logical_holds`].
    pub fn physical_holds(&self) -> Antichain<B::Time> {
        self.lock().holds_physical.frontier().to_owned()
    }

    /// A reader holding logical compaction at the published `since` and physical compaction at the
    /// chain's coverage.
    pub fn reader(self: &Arc<Self>) -> SharedReader<B> {
        let mut state = self.lock();
        let logical = state.logical.clone();
        let physical = state.upper.clone();
        adjust(&mut state.holds_logical, &[], &logical);
        adjust(&mut state.holds_physical, &[], &physical);
        drop(state);
        SharedReader {
            shared: Arc::clone(self),
            logical,
            physical,
        }
    }

    /// A reader holding logical compaction at `as_of`, or `Err` with the published `since` when
    /// that is already beyond `as_of`.
    ///
    /// Checks and registers under one lock acquisition, so a returned reader holds a frontier the
    /// trace can still honour.
    pub fn reader_at(
        self: &Arc<Self>,
        as_of: &Antichain<B::Time>,
    ) -> Result<SharedReader<B>, Antichain<B::Time>> {
        let mut state = self.lock();
        if !PartialOrder::less_equal(&state.logical, as_of) {
            return Err(state.logical.clone());
        }
        let physical = state.upper.clone();
        adjust(&mut state.holds_logical, &[], as_of);
        adjust(&mut state.holds_physical, &[], &physical);
        drop(state);
        Ok(SharedReader {
            shared: Arc::clone(self),
            logical: as_of.clone(),
            physical,
        })
    }

    /// A reader holding logical compaction at the join of `as_of` and the published `since`, and
    /// nothing physically.
    ///
    /// Never refuses. It is the hold a runtime keeps on a collection its peer publishes, whose
    /// `as_of` can sit below the published `since`, and which reads only through readers it mints
    /// with [`SharedReader::fresh_reader`].
    pub fn reader_at_least(self: &Arc<Self>, as_of: &Antichain<B::Time>) -> SharedReader<B> {
        let mut state = self.lock();
        let logical = as_of.join(&state.logical);
        adjust(&mut state.holds_logical, &[], &logical);
        drop(state);
        SharedReader {
            shared: Arc::clone(self),
            logical,
            physical: Antichain::new(),
        }
    }

    fn wake_writer(state: &State<B>) {
        if let Some(activator) = &state.writer_activator {
            let _ = activator.activate();
        }
    }
}

/// The non-empty batches of the totally ordered `chain` before the cut `upper`.
///
/// Panics if a batch straddles the cut, which means a merge ate a boundary the reader needed.
fn chain_through<B: BatchReader + Clone>(chain: &[B], upper: AntichainRef<B::Time>) -> Vec<B> {
    let mut out = Vec::new();
    for batch in chain {
        if PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
            break;
        }
        if !batch.is_empty() {
            assert!(
                PartialOrder::less_equal(&batch.upper().borrow(), &upper),
                "chain_through: upper straddles batch"
            );
            out.push(batch.clone());
        }
    }
    out
}

/// Replaces the elements of `lower` with those of `upper` in `accumulated`, and returns whether
/// that moved the frontier.
///
/// The writer reads the readers' holds only when it runs, so a change that leaves the frontier in
/// place gives it nothing to do and needs no wake.
fn adjust<T: Timestamp>(accumulated: &mut MutableAntichain<T>, lower: &[T], upper: &[T]) -> bool {
    let before = accumulated.frontier().to_owned();
    accumulated.update_iter(upper.iter().cloned().map(|time| (time, 1)));
    accumulated.update_iter(lower.iter().cloned().map(|time| (time, -1)));
    accumulated.frontier() != before.borrow()
}

/// A point a [`SharedBox`] publishes into.
struct Attachment<B: BatchReader> {
    shared: Arc<Shared<B>>,
    /// Which writer of `shared` this trace is, counting from one.
    writer: u64,
    /// Fires after every publish that advanced `upper`, outside the state lock.
    on_seal: Box<dyn Fn()>,
}

impl<B: BatchReader> Attachment<B> {
    /// Whether anything besides this trace references the point. Minting a reader needs a
    /// reference, so an unreferenced point cannot gain one while the trace runs.
    fn reachable(&self) -> bool {
        Arc::strong_count(&self.shared) > 1
    }
}

/// The points a published trace backs.
struct Publication<B: BatchReader> {
    /// The first point the trace published to. Its holds include the trace's same-thread agents.
    home: Attachment<B>,
    /// Further points over the same trace, as an index re-exporting another's arrangement
    /// publishes. They carry their readers' holds only.
    others: Vec<Attachment<B>>,
    chain_scratch: Vec<B>,
}

/// The shared state behind a [`SharedAgent`]: the spine and the holds on it.
///
/// It lives on the arranging worker's thread only.
pub struct SharedBox<Tr: Trace> {
    trace: Tr,
    /// Every same-thread agent's logical hold while the trace is unpublished. Empty once
    /// published, when they live in the home point.
    logical: MutableAntichain<Tr::Time>,
    /// The physical holds, as `logical`.
    physical: MutableAntichain<Tr::Time>,
    publication: Option<Publication<Tr::Batch>>,
}

impl<Tr: Trace> SharedBox<Tr> {
    fn new(mut trace: Tr) -> Self {
        // As differential's `TraceBox`, the trace's own frontiers become the first agent's holds.
        let mut logical = MutableAntichain::new();
        logical.update_iter(
            trace
                .get_logical_compaction()
                .iter()
                .cloned()
                .map(|t| (t, 1)),
        );
        let mut physical = MutableAntichain::new();
        physical.update_iter(
            trace
                .get_physical_compaction()
                .iter()
                .cloned()
                .map(|t| (t, 1)),
        );
        SharedBox {
            trace,
            logical,
            physical,
            publication: None,
        }
    }

    /// The spine, for inspecting its batches.
    pub fn trace(&self) -> &Tr {
        &self.trace
    }

    fn adjust_logical(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        if self.publication.is_none() {
            adjust(&mut self.logical, &lower[..], &upper[..]);
            self.trace.set_logical_compaction(self.logical.frontier());
            return;
        }
        self.sync_holds(|state| {
            adjust(&mut state.local_logical, &lower[..], &upper[..]);
            adjust(&mut state.holds_logical, &lower[..], &upper[..])
        });
    }

    fn adjust_physical(&mut self, lower: AntichainRef<Tr::Time>, upper: AntichainRef<Tr::Time>) {
        if self.publication.is_none() {
            adjust(&mut self.physical, &lower[..], &upper[..]);
            self.trace.set_physical_compaction(self.physical.frontier());
            return;
        }
        // Compute's maintenance sets this on every trace every few milliseconds, mostly to the
        // frontier it already has, and publishing then costs locks and clones per trace. A moved
        // hold introduces pending merges and is published, and a reader's moved hold reaches the
        // spine through `exert`, which the hold's activation schedules.
        let moved =
            self.sync_holds(|state| adjust(&mut state.holds_physical, &lower[..], &upper[..]));
        if moved {
            self.publish_chain(None);
        }
    }

    /// Applies `change` to the home point's holds, and when it reports the meet moved, applies the
    /// meet of every point's holds to the spine. Returns whether it applied. Unpublished, it does
    /// nothing.
    fn sync_holds(&mut self, change: impl FnOnce(&mut State<Tr::Batch>) -> bool) -> bool {
        let Some(publication) = &self.publication else {
            return false;
        };
        let current_logical = self.trace.get_logical_compaction().to_owned();
        // Reading the holds and publishing the frontier they produce happen under one acquisition,
        // and the spine is touched only afterwards. A reader that registers before us is counted
        // in the meet. One that registers after us sees the frontier we are about to apply, and
        // `reader_at` refuses an `as_of` below it. So no reader is ever admitted at a time the
        // trace is about to coalesce away, which a read-then-release-then-apply order permits for
        // the length of the merge it runs.
        //
        // Publishing the target before applying it means the point can briefly advertise a `since`
        // ahead of the spine's own. That refuses a reader the trace could still have served, which
        // is the safe direction. The reverse admits a reader the trace cannot serve.
        //
        // Points are locked in address order, so two traces sharing a pair of points cannot
        // deadlock. The critical section is antichain arithmetic, never merge work.
        let mut ordered: Vec<(bool, &Arc<Shared<Tr::Batch>>)> =
            std::iter::once((true, &publication.home.shared))
                .chain(publication.others.iter().map(|a| (false, &a.shared)))
                .collect();
        ordered.sort_unstable_by_key(|(_, shared)| Arc::as_ptr(shared));
        let mut guards: Vec<(bool, MutexGuard<'_, State<Tr::Batch>>)> = ordered
            .into_iter()
            .map(|(home, shared)| (home, shared.lock()))
            .collect();
        let home = guards
            .iter_mut()
            .find(|(home, _)| *home)
            .map(|(_, state)| state)
            .expect("the home point is locked");
        if !change(home) {
            return false;
        }
        let requested = home.local_logical.frontier().to_owned();
        // The meet runs across every point, since a reader of any of them holds this one trace.
        let mut logical = Antichain::new();
        let mut physical = Antichain::new();
        for (_, state) in guards.iter() {
            logical = logical.meet(&state.holds_logical.frontier().to_owned());
            physical = physical.meet(&state.holds_physical.frontier().to_owned());
        }
        // The join keeps the spine from rewinding, which differential's spine would accept without
        // complaint: a hold below the spine's `since`, as a reader registered on a placeholder may
        // place, cannot be honoured, so it constrains nothing.
        let logical = logical.join(&current_logical);
        for (_, state) in guards.iter_mut() {
            state.logical = logical.clone();
            state.writer_logical = requested.clone();
        }
        drop(guards);

        if self.trace.get_logical_compaction() != logical.borrow() {
            self.trace.set_logical_compaction(logical.borrow());
        }
        // The spine refuses to rewind its physical frontier. A reader registers its physical hold
        // at the chain coverage it was seeded with, which is at or beyond the spine's frontier, so
        // the meet regresses only for a reader registered on a placeholder. The guard checks it.
        if self.trace.get_physical_compaction() != physical.borrow()
            && PartialOrder::less_equal(&self.trace.get_physical_compaction(), &physical.borrow())
        {
            self.trace.set_physical_compaction(physical.borrow());
        }
        true
    }

    /// Mirrors the spine's chain and frontiers into every reachable point.
    ///
    /// Also enqueues `arrived`, a batch the spine just accepted, to every importer that was live
    /// under the lock that swapped in the chain containing it. A reader registering concurrently
    /// therefore either seeds a chain containing that batch or is in that snapshot and receives it
    /// through its queue, never neither. The push itself happens after the lock is released.
    fn publish_chain(&mut self, arrived: Option<&Tr::Batch>) {
        let Some(publication) = &mut self.publication else {
            return;
        };
        // A point only this trace references has no reader and no owner left. Publishing into it
        // would cost a chain clone, a lock, and an `on_seal` per mutation for as long as the trace
        // lives, which for a point published over another collection's trace outlasts the point's
        // own collection.
        publication.others.retain(Attachment::reachable);
        if !publication.home.reachable() && publication.others.is_empty() {
            self.unpublish();
            return;
        }
        let chain = &mut publication.chain_scratch;
        chain.clear();
        self.trace.map_batches(|batch| chain.push(batch.clone()));
        let upper = chain_upper::<Tr::Batch>(chain);
        let logical = self.trace.get_logical_compaction().to_owned();
        let physical = self.trace.get_physical_compaction().to_owned();

        let attachments = std::iter::once(&publication.home).chain(publication.others.iter());
        for attachment in attachments.filter(|a| a.reachable()) {
            // Built before the lock and swapped in, so neither the allocation nor the old chain's
            // reference count drops happen while the lock is held.
            let mut next = chain.clone();
            let (advanced, live) = {
                let mut state = attachment.shared.lock();
                std::mem::swap(&mut state.chain, &mut next);
                let advanced = state.upper != upper;
                state.upper = upper.clone();
                state.logical = logical.clone();
                state.physical = physical.clone();
                let live = if arrived.is_some() {
                    state.live_queues()
                } else {
                    Vec::new()
                };
                (advanced, live)
            };
            drop(next);

            if let Some(batch) = arrived {
                for queue in &live {
                    queue.push([
                        Replay::Batch(batch.clone()),
                        Replay::Frontier(upper.clone()),
                    ]);
                    queue.activate();
                }
            }
            if advanced {
                (attachment.on_seal)();
            }
        }
        chain.clear();
    }

    /// Takes the holds back from a home point nothing else references and stops publishing.
    ///
    /// No reader references the point, so every hold on it is a same-thread agent's.
    fn unpublish(&mut self) {
        let Some(publication) = self.publication.take() else {
            return;
        };
        let mut state = publication.home.shared.lock();
        let logical: Vec<_> = state.holds_logical.updates().cloned().collect();
        let physical: Vec<_> = state.holds_physical.updates().cloned().collect();
        drop(state);
        self.logical.update_iter(logical);
        self.physical.update_iter(physical);
    }

    /// Publishes this trace to `shared`, in addition to any points it already backs.
    ///
    /// Mirrors the current chain and frontiers, and seeds every importer already registered on
    /// `shared` with them. The first point a trace publishes to becomes its home and takes over the
    /// holds of the trace's same-thread agents. `activator` must schedule the operator that drives
    /// this trace: a reader moves its hold from another thread, and only that operator's next
    /// `exert` applies it to the spine. `on_seal` fires after every publish that advanced `upper`,
    /// on this thread.
    fn attach(
        &mut self,
        shared: Arc<Shared<Tr::Batch>>,
        activator: SyncActivator,
        on_seal: impl Fn() + 'static,
    ) {
        if let Some(publication) = &mut self.publication {
            let attached = std::iter::once(&mut publication.home)
                .chain(publication.others.iter_mut())
                .find(|attached| Arc::ptr_eq(&attached.shared, &shared));
            if let Some(attached) = attached {
                // Several points over one trace is expected, the logging error arrangement being
                // published under every log id. The same point twice is not: it would publish the
                // chain there twice per mutation and fire two `on_seal`s per seal, and keeping the
                // newest callback discards one whose loss costs a lost wakeup.
                mz_ore::soft_assert_or_log!(
                    false,
                    "trace re-attached to a publication point it already backs"
                );
                attached.on_seal = Box::new(on_seal);
                return;
            }
        }
        let home = self.publication.is_none();
        let mut chain = Vec::new();
        self.trace.map_batches(|batch| chain.push(batch.clone()));
        let upper = chain_upper::<Tr::Batch>(&chain);
        let (writer, live) = {
            let mut state = shared.lock();
            state.writer_activator = Some(activator);
            state.writers += 1;
            let writer = state.writers;
            state.chain = chain.clone();
            state.upper = upper.clone();
            state.logical = self.trace.get_logical_compaction().to_owned();
            state.physical = self.trace.get_physical_compaction().to_owned();
            if home {
                let logical: Vec<_> = self.logical.updates().cloned().collect();
                let physical: Vec<_> = self.physical.updates().cloned().collect();
                state.local_logical.update_iter(logical.iter().cloned());
                state.holds_logical.update_iter(logical);
                state.holds_physical.update_iter(physical);
            }
            // An importer seeded by an earlier writer has acknowledged that writer's frontiers, and
            // this trace's chain regresses below them, which an importer cannot follow: a batch
            // straddling its acknowledged frontier panics its capability downgrade. Such an
            // importer reads a collection the earlier writer's dataflow maintained, so it is about
            // to be dropped too, and it keeps what it has.
            if writer > 1 {
                state.queues.clear();
            }
            (writer, state.live_queues())
        };
        for queue in live {
            queue.push(
                chain
                    .iter()
                    .cloned()
                    .map(Replay::Batch)
                    .chain(std::iter::once(Replay::Frontier(upper.clone()))),
            );
            queue.activate();
        }
        let attachment = Attachment {
            shared,
            writer,
            on_seal: Box::new(on_seal),
        };
        match &mut self.publication {
            Some(publication) => publication.others.push(attachment),
            None => {
                self.logical = MutableAntichain::new();
                self.physical = MutableAntichain::new();
                self.publication = Some(Publication {
                    home: attachment,
                    others: Vec::new(),
                    chain_scratch: Vec::new(),
                });
            }
        }
        // Holds readers registered on the point before it was attached take effect now.
        self.sync_holds(|_| true);
    }

    fn insert(&mut self, batch: Tr::Batch) {
        if self.publication.is_none() {
            self.trace.insert(batch);
            return;
        }
        self.trace.insert(batch.clone());
        self.publish_chain(Some(&batch));
    }

    fn exert(&mut self) {
        if self.publication.is_none() {
            self.trace.exert();
            return;
        }
        self.sync_holds(|_| true);
        self.trace.exert();
        self.publish_chain(None);
    }
}

impl<Tr: Trace> Drop for SharedBox<Tr> {
    fn drop(&mut self) {
        // NOTE: Readers keep the `upper` this trace last published rather than advancing to the
        // empty frontier. A dropped trace says nothing about the times past its `upper`, and
        // completing would present them as empty. An importing dataflow is dropped with
        // `drop_dataflow`, which does not need its inputs to complete.
        let Some(publication) = self.publication.take() else {
            return;
        };
        for attachment in std::iter::once(publication.home).chain(publication.others) {
            let mut state = attachment.shared.lock();
            // A later writer owns the activator once it attaches.
            if state.writers == attachment.writer {
                state.writer_activator = None;
            }
        }
    }
}

/// The frontier a chain covers: its last batch's upper, or the minimum for an empty chain.
fn chain_upper<B: BatchReader>(chain: &[B]) -> Antichain<B::Time> {
    chain.last().map_or_else(
        || Antichain::from_elem(B::Time::minimum()),
        |b| b.upper().clone(),
    )
}

type LocalQueue<Tr> = VecDeque<TraceReplayInstruction<Tr>>;
type LocalQueueReader<Tr> = Rc<(Activator, RefCell<LocalQueue<Tr>>)>;
type LocalQueueWriter<Tr> = RcWeak<(Activator, RefCell<LocalQueue<Tr>>)>;

/// A same-thread handle to a trace that can be published to other threads.
///
/// The counterpart of differential's `TraceAgent`: it reads the spine directly, registers its
/// compaction holds with the trace's [`SharedBox`], and imports into dataflows on its own worker.
/// Each clone is an independent hold.
pub struct SharedAgent<Tr: Trace> {
    trace: Rc<RefCell<SharedBox<Tr>>>,
    queues: RcWeak<RefCell<Vec<LocalQueueWriter<Tr>>>>,
    logical_compaction: Antichain<Tr::Time>,
    physical_compaction: Antichain<Tr::Time>,
    temp_antichain: Antichain<Tr::Time>,
    operator: OperatorInfo,
    logging: Option<Logger>,
}

impl<Tr: Trace> SharedAgent<Tr> {
    /// Wraps `trace`, maintained by `operator`, into an agent and the writer that feeds it.
    pub fn new(
        trace: Tr,
        operator: OperatorInfo,
        logging: Option<Logger>,
    ) -> (Self, SharedWriter<Tr>) {
        let trace = Rc::new(RefCell::new(SharedBox::new(trace)));
        let queues = Rc::new(RefCell::new(Vec::new()));
        if let Some(logging) = &logging {
            logging.log(TraceShare {
                operator: operator.global_id,
                diff: 1,
            });
        }
        let (logical_compaction, physical_compaction) = {
            let trace = trace.borrow();
            (
                trace.logical.frontier().to_owned(),
                trace.physical.frontier().to_owned(),
            )
        };
        let agent = SharedAgent {
            trace: Rc::clone(&trace),
            queues: Rc::downgrade(&queues),
            logical_compaction,
            physical_compaction,
            temp_antichain: Antichain::new(),
            operator,
            logging,
        };
        let writer = SharedWriter {
            upper: Antichain::from_elem(Tr::Time::minimum()),
            trace: Rc::downgrade(&trace),
            queues,
        };
        (agent, writer)
    }

    /// The [`OperatorInfo`] of the operator that maintains the trace.
    pub fn operator(&self) -> &OperatorInfo {
        &self.operator
    }

    /// A handle that observes the trace's batches without holding the trace.
    pub fn observer(&self) -> TraceObserver<Tr> {
        TraceObserver {
            trace: Rc::downgrade(&self.trace),
        }
    }

    /// Publishes the trace to `shared`. See [`SharedBox`] for what publishing does.
    ///
    /// `activator` must schedule the operator that maintains the trace, and `on_seal` fires on this
    /// thread after every publish that advanced the published `upper`.
    pub fn publish(
        &self,
        shared: Arc<Shared<Tr::Batch>>,
        activator: SyncActivator,
        on_seal: impl Fn() + 'static,
    ) {
        self.trace.borrow_mut().attach(shared, activator, on_seal);
    }

    /// Attaches a queue that replays the trace's batches and then follows its writer.
    fn new_listener(&mut self, activator: Activator) -> LocalQueueReader<Tr> {
        let mut new_queue = VecDeque::new();
        let mut upper = None;
        self.trace.borrow().trace.map_batches(|batch| {
            new_queue.push_back(TraceReplayInstruction::Batch(
                batch.clone(),
                Some(Tr::Time::minimum()),
            ));
            upper = Some(batch.upper().clone());
        });
        if let Some(upper) = upper {
            new_queue.push_back(TraceReplayInstruction::Frontier(upper));
        }
        let reference = Rc::new((activator, RefCell::new(new_queue)));
        if let Some(queue) = self.queues.upgrade() {
            queue.borrow_mut().push(Rc::downgrade(&reference));
        }
        reference.0.activate();
        reference
    }

    /// Imports the trace into `scope`, restricted to `[since, until)`.
    ///
    /// The port of `TraceAgent::import_frontier_core`: updates are advanced to `since`, those at or
    /// beyond `until` are suppressed, and the capability drops once the trace passes `until`.
    pub fn import_frontier_core<'scope>(
        &mut self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
        since: Antichain<Tr::Time>,
        until: Antichain<Tr::Time>,
    ) -> (
        Arranged<'scope, TraceFrontier<SharedAgent<Tr>>>,
        ShutdownButton<CapabilitySet<Tr::Time>>,
    )
    where
        Tr: 'static,
    {
        let trace = self.clone();
        let trace = TraceFrontier::make_from(trace, since.borrow(), until.borrow());

        let mut shutdown_button = None;

        let stream = {
            let shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {
                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(Rc::clone(&info.address));
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(info.address);
                *shutdown_button_ref =
                    Some(ShutdownButton::new(Rc::clone(&capabilities), activator));

                capabilities
                    .borrow_mut()
                    .as_mut()
                    .expect("just created")
                    .insert(capability);

                move |output| {
                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {
                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            // Without capabilities, `until` was reached and this import is done.
                            if !capabilities.is_empty() {
                                match instruction {
                                    TraceReplayInstruction::Frontier(frontier) => {
                                        if PartialOrder::less_equal(&until, &frontier) {
                                            capabilities.downgrade(&[]);
                                        } else {
                                            capabilities.downgrade(&frontier.borrow()[..]);
                                        }
                                    }
                                    TraceReplayInstruction::Batch(batch, hint) => {
                                        if let Some(time) = hint {
                                            if !batch.is_empty() {
                                                let delayed = capabilities.delayed(&time);
                                                output.session(&delayed).give(
                                                    BatchFrontier::make_from(
                                                        batch,
                                                        since.borrow(),
                                                        until.borrow(),
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };

        (
            Arranged { stream, trace },
            shutdown_button.expect("source constructor ran"),
        )
    }
}

impl<Tr: Trace> TraceReader for SharedAgent<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        // As `TraceAgent`: a hold is the joint consequence of every frontier it has been asked to
        // hold, so the request is joined rather than assigned.
        differential_dataflow::lattice::antichain_join_into(
            &self.logical_compaction.borrow()[..],
            &frontier[..],
            &mut self.temp_antichain,
        );
        self.trace.borrow_mut().adjust_logical(
            self.logical_compaction.borrow(),
            self.temp_antichain.borrow(),
        );
        std::mem::swap(&mut self.logical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical_compaction.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        differential_dataflow::lattice::antichain_join_into(
            &self.physical_compaction.borrow()[..],
            &frontier[..],
            &mut self.temp_antichain,
        );
        self.trace.borrow_mut().adjust_physical(
            self.physical_compaction.borrow(),
            self.temp_antichain.borrow(),
        );
        std::mem::swap(&mut self.physical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.physical_compaction.borrow()
    }

    fn batches_through(&mut self, frontier: AntichainRef<'_, Tr::Time>) -> Option<Vec<Tr::Batch>> {
        self.trace.borrow_mut().trace.batches_through(frontier)
    }

    fn map_batches<F: FnMut(&Tr::Batch)>(&self, f: F) {
        self.trace.borrow().trace.map_batches(f)
    }
}

impl<Tr: Trace> Clone for SharedAgent<Tr> {
    fn clone(&self) -> Self {
        if let Some(logging) = &self.logging {
            logging.log(TraceShare {
                operator: self.operator.global_id,
                diff: 1,
            });
        }
        let empty = Antichain::new();
        let mut trace = self.trace.borrow_mut();
        trace.adjust_logical(empty.borrow(), self.logical_compaction.borrow());
        trace.adjust_physical(empty.borrow(), self.physical_compaction.borrow());
        drop(trace);
        SharedAgent {
            trace: Rc::clone(&self.trace),
            queues: RcWeak::clone(&self.queues),
            logical_compaction: self.logical_compaction.clone(),
            physical_compaction: self.physical_compaction.clone(),
            temp_antichain: Antichain::new(),
            operator: self.operator.clone(),
            logging: self.logging.clone(),
        }
    }
}

impl<Tr: Trace> Drop for SharedAgent<Tr> {
    fn drop(&mut self) {
        if let Some(logging) = &self.logging {
            logging.log(TraceShare {
                operator: self.operator.global_id,
                diff: -1,
            });
        }
        let empty = Antichain::new();
        let mut trace = self.trace.borrow_mut();
        trace.adjust_logical(self.logical_compaction.borrow(), empty.borrow());
        trace.adjust_physical(self.physical_compaction.borrow(), empty.borrow());
    }
}

impl<Tr: Trace + 'static> Agent<Tr> for SharedAgent<Tr> {
    type Writer = SharedWriter<Tr>;

    fn new(trace: Tr, operator: OperatorInfo, logging: Option<Logger>) -> (Self, Self::Writer) {
        SharedAgent::new(trace, operator, logging)
    }
}

/// Observes a trace's batches without keeping the trace alive.
pub struct TraceObserver<Tr: Trace> {
    trace: RcWeak<RefCell<SharedBox<Tr>>>,
}

impl<Tr: Trace> TraceObserver<Tr> {
    /// Calls `f` on every batch of the trace, and returns `false` if the trace is gone.
    pub fn map_batches<F: FnMut(&Tr::Batch)>(&self, f: F) -> bool {
        match self.trace.upgrade() {
            Some(trace) => {
                trace.borrow().trace.map_batches(f);
                true
            }
            None => false,
        }
    }
}

/// The write endpoint of a [`SharedAgent`]'s trace.
///
/// The counterpart of differential's `TraceWriter`. It holds the trace weakly, so a trace whose
/// agents have all dropped is gone even while its arranging operator runs.
pub struct SharedWriter<Tr: Trace> {
    upper: Antichain<Tr::Time>,
    trace: RcWeak<RefCell<SharedBox<Tr>>>,
    queues: Rc<RefCell<Vec<LocalQueueWriter<Tr>>>>,
}

impl<Tr: Trace> SharedWriter<Tr> {
    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().exert();
        }
    }

    /// Advances the trace by `batch`.
    ///
    /// `hint` is `None` for an empty batch, or `Some(time)` for a time less or equal to all
    /// updates in the batch, suitable for use as a capability.
    pub fn insert(&mut self, batch: Tr::Batch, hint: Option<Tr::Time>) {
        assert!(&self.upper == batch.lower(), "batches must be contiguous");
        assert!(
            batch.lower() != batch.upper(),
            "batches must be non-empty intervals"
        );
        self.upper.clone_from(batch.upper());

        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            if let Some(pair) = queue.upgrade() {
                pair.1
                    .borrow_mut()
                    .push_back(TraceReplayInstruction::Batch(batch.clone(), hint.clone()));
                pair.1
                    .borrow_mut()
                    .push_back(TraceReplayInstruction::Frontier(batch.upper().clone()));
                pair.0.activate();
            }
        }
        borrow.retain(|w| w.upgrade().is_some());

        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().insert(batch);
        }
    }

    /// Inserts an empty batch up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            self.insert(Tr::Batch::empty(self.upper.clone(), upper), None);
        }
    }
}

impl<Tr: Trace> AgentWriter<Tr> for SharedWriter<Tr> {
    fn insert(&mut self, batch: Tr::Batch, hint: Option<Tr::Time>) {
        SharedWriter::insert(self, batch, hint)
    }

    fn seal(&mut self, upper: Antichain<Tr::Time>) {
        SharedWriter::seal(self, upper)
    }

    fn exert(&mut self) {
        SharedWriter::exert(self)
    }
}

impl<Tr: Trace> Drop for SharedWriter<Tr> {
    fn drop(&mut self) {
        self.seal(Antichain::new())
    }
}

/// A `Clone + Send` reader of a [`Shared`] publication point.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone is an independent hold, as each [`SharedAgent`] clone is, so
/// two consumers of one import cannot release each other's holds.
pub struct SharedReader<B: BatchReader> {
    shared: Arc<Shared<B>>,
    /// This reader's logical hold. Kept locally so the setter and `Drop` adjust the accumulation by a
    /// delta, and so the getter can return a borrow.
    logical: Antichain<B::Time>,
    /// This reader's physical hold, seeded at the chain coverage. See the module docs.
    physical: Antichain<B::Time>,
}

impl<B: BatchReader> SharedReader<B> {
    /// The publication point.
    pub fn shared(&self) -> &Arc<Shared<B>> {
        &self.shared
    }

    /// A reader at this reader's logical hold that also holds the chain physically at the coverage
    /// it is seeded with, as an import must.
    pub fn fresh_reader(&self) -> SharedReader<B> {
        self.shared
            .reader_at(&self.logical)
            .expect("the publication's since is at or below every hold registered on it")
    }
}

impl<B: BatchReader> Clone for SharedReader<B> {
    fn clone(&self) -> Self {
        {
            let mut state = self.shared.lock();
            adjust(&mut state.holds_logical, &[], &self.logical);
            adjust(&mut state.holds_physical, &[], &self.physical);
        }
        SharedReader {
            shared: Arc::clone(&self.shared),
            logical: self.logical.clone(),
            physical: self.physical.clone(),
        }
    }
}

impl<B: BatchReader> Drop for SharedReader<B> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            let logical = adjust(&mut state.holds_logical, &self.logical, &[]);
            let physical = adjust(&mut state.holds_physical, &self.physical, &[]);
            if logical || physical {
                Shared::wake_writer(&state);
            }
        }
    }
}

impl<B> TraceReader for SharedReader<B>
where
    B: BatchReader + Clone + 'static,
    // `batches_through` stops at the first batch whose lower is beyond the cut and takes every
    // later batch to be past it too, which holds because the chain is totally ordered.
    B::Time: TotalOrder,
{
    type Time = B::Time;
    type Batch = B;

    fn batches_through(&mut self, upper: AntichainRef<B::Time>) -> Option<Vec<B>> {
        let state = self.shared.lock();
        // NOTE: `Spine::batches_through` asserts that the cut is at or beyond the spine's physical
        // frontier. That does not hold for a shared reader, whose cut can sit below the meet the
        // writer applied while it drains a seed. The straddle check is the guard instead: a batch
        // straddling the cut means a merge ate a boundary this reader still needed, and returning it
        // would hand back updates at times not before `upper`.
        Some(chain_through(&state.chain, upper))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<B::Time>) {
        // Join rather than overwrite, as `SharedAgent` does: a hold is the joint consequence of
        // every frontier it has been asked to hold, and the getter reports what is held. Joining
        // with the empty antichain is absorbing, so an empty request still releases.
        let next = self.logical.join(&frontier.to_owned());
        let previous = std::mem::replace(&mut self.logical, next);
        let mut state = self.shared.lock();
        if adjust(&mut state.holds_logical, &previous, &self.logical) {
            Shared::wake_writer(&state);
        }
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, B::Time> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, B::Time>) {
        // Join, never assign: the hold starts at the chain coverage, and a request below it must not
        // pull it down to where a merge could eat the boundary this reader was seeded with.
        let next = self.physical.join(&frontier.to_owned());
        let previous = std::mem::replace(&mut self.physical, next);
        let mut state = self.shared.lock();
        if adjust(&mut state.holds_physical, &previous, &self.physical) {
            Shared::wake_writer(&state);
        }
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, B::Time> {
        self.physical.borrow()
    }

    fn map_batches<F: FnMut(&B)>(&self, mut f: F) {
        let state = self.shared.lock();
        for batch in state.chain.iter() {
            f(batch);
        }
    }
}

impl<B> SharedReader<B>
where
    B: Batch + Navigable + Clone + 'static,
    B::Time: TotalOrder,
{
    /// Imports the publication restricted to `[as_of, until)`, presented at `as_of`.
    ///
    /// The port of `TraceAgent::import_frontier_core` onto a publication point. The source operator
    /// registers a queue seeded with the current chain and drains it as the writer appends,
    /// wrapping batches in [`BatchFrontier`] and the trace in [`TraceFrontier`], both advanced to
    /// `as_of` and bounded by `until`. Pre-`as_of` updates therefore coalesce to `as_of`.
    ///
    /// The returned arrangement's stream frontier tracks the trace's `upper`, so the trace never
    /// runs ahead of the stream, which a join relies on to count each match once. For a
    /// single-time read pass `until = as_of.step_forward()`: the capability drops once `upper`
    /// passes `as_of` and the read completes.
    ///
    /// The import owns a hold that follows the frontiers it acknowledges, so a long-lived import
    /// does not stop the writer compacting behind it. A consumer that keeps the returned trace
    /// holds its own registration for the times its own progress governs.
    pub fn import_frontier_core<'scope>(
        &self,
        scope: Scope<'scope, B::Time>,
        name: &str,
        as_of: Antichain<B::Time>,
        until: Antichain<B::Time>,
    ) -> Arranged<'scope, TraceFrontier<SharedReader<B>>> {
        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        let shared = Arc::clone(&self.shared);
        let mut hold = Some(self.clone());

        let stream = source(scope, name, move |capability, info| {
            let activator = scope.worker().sync_activator_for(info.address.to_vec());

            // Register under one lock acquisition: seed the queue with the chain and the frontier it
            // covers, then publish the queue weakly. Later batches append, earlier ones are seeded.
            let (queue, seed) = {
                let mut state = shared.lock();
                let mut instructions = VecDeque::new();
                instructions.extend(state.chain.iter().cloned().map(Replay::Batch));
                let seed = state.upper.clone();
                instructions.push_back(Replay::Frontier(seed.clone()));
                let queue = Arc::new(ImportQueue {
                    instructions: Mutex::new(instructions),
                    activator,
                });
                state.queues.push(Arc::downgrade(&queue));
                (queue, seed)
            };

            // Emptied once the read is over, which is the operator's "done" state.
            let mut capabilities = CapabilitySet::from_elem(capability);
            let mut acknowledged = seed.clone();
            // The seeded instructions come first and are emitted as-is. Everything after the seed's
            // own `Frontier` is live and is filtered against `seed` below.
            let mut draining_seed = true;

            move |output| {
                // Drains this importer's own queue only, never the publication point's lock.
                let drained: Vec<_> = {
                    let mut instructions =
                        queue.instructions.lock().expect("import queue poisoned");
                    instructions.drain(..).collect()
                };
                if capabilities.is_empty() {
                    return;
                }
                for instruction in drained {
                    match instruction {
                        Replay::Frontier(frontier) => {
                            // A capability set cannot be downgraded backwards, and the seeded
                            // coverage is already correct.
                            if !PartialOrder::less_equal(&acknowledged, &frontier) {
                                continue;
                            }
                            acknowledged = frontier.clone();
                            draining_seed = false;
                            // Everything at or below `acknowledged` has been delivered and will never
                            // be replayed, so this import will not read there again. Both axes: the
                            // physical hold otherwise pins the spine at the coverage this import
                            // registered at and the chain grows one batch per seal for its life.
                            if let Some(hold) = hold.as_mut() {
                                hold.set_logical_compaction(acknowledged.borrow());
                                hold.set_physical_compaction(acknowledged.borrow());
                            }
                            // Bound the read at `until`, and complete on the empty frontier a closed
                            // trace publishes. Otherwise track `upper`, keeping the stream frontier
                            // equal to the trace's upper.
                            if frontier.is_empty() || PartialOrder::less_equal(&until, &frontier) {
                                capabilities.downgrade(std::iter::empty::<B::Time>());
                                hold = None;
                                break;
                            }
                            capabilities.downgrade(&frontier.borrow()[..]);
                        }
                        Replay::Batch(batch) => {
                            // The writer enqueues a batch only to importers live under the lock
                            // that published the chain containing it, so a batch in the seed cannot
                            // also arrive live. This guards the invariant: emitting a covered batch
                            // twice would double count it under a capability already moved past.
                            if !draining_seed
                                && PartialOrder::less_equal(&batch.upper().borrow(), &seed.borrow())
                            {
                                continue;
                            }
                            if batch.is_empty() {
                                continue;
                            }
                            // The chain is contiguous and totally ordered, so `lower` has one
                            // element, at or beyond the frontier the capability sits at.
                            let time = batch.lower().elements()[0].clone();
                            let cap = capabilities.delayed(&time);
                            output.session(&cap).give(BatchFrontier::make_from(
                                batch,
                                as_of.borrow(),
                                until.borrow(),
                            ));
                        }
                    }
                }
            }
        });

        Arranged { stream, trace }
    }
}

#[cfg(test)]
mod tests;
