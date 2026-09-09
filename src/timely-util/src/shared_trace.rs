// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Trace`] wrapper whose contents are readable from other threads.
//!
//! An arrangement's trace is owned by the worker that arranges into it and is not `Send`: its
//! handles are `Rc<RefCell<..>>`, and its spine does merge work whenever it is touched, including
//! inside `roll_up` and `complete_at`, which finish merges synchronously as part of structural
//! changes. A spine behind a mutex would therefore hold that mutex for whole merges. The batches,
//! though, are immutable and reference counted, so a chain of them plus the trace's frontiers is a
//! consistent, self-describing view that any thread can read.
//!
//! [`SharedSpine`] is a [`Trace`] that owns an inner trace on its worker's thread and, once
//! [attached](SharedSpine::attach) to a [`Shared`] publication point, mirrors the inner trace's
//! batch chain, `upper`, and compaction frontiers into the point after every mutation. The lock is
//! held for a chain rebuild, one reference count per spine level, never for merge work.
//!
//! [`SharedReader`]s on any thread read the view through [`TraceReader`], register compaction
//! holds against it, and [import](SharedReader::import_frontier_core) it as an arrangement. Their
//! holds accumulate in the point as differential's `TraceBox` accumulates its agents' holds, and the
//! writer applies the meet of its local `TraceBox` frontier and the readers' to the inner trace. A
//! reader that moves a hold wakes the writer so the inner trace learns of it without waiting for
//! the arrangement's next input.
//!
//! Logical compaction decides which times stay distinguishable, physical compaction which batches
//! may merge. A reader needs distinguishability at the times it reads, and a batch boundary at each
//! frontier it passes to `cursor_through`. Its physical hold therefore starts at the chain's
//! coverage, which is where an import seeded with that chain makes its first cut, and follows the
//! frontiers it acknowledges from there.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::Navigable;
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{Batch, BatchReader, ExertionLogic, Trace, TraceReader};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::{OperatorInfo, source};
use timely::order::TotalOrder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
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

/// The view readers see, plus the holds they register.
struct State<B: BatchReader> {
    /// The inner trace's batches in time order, as `map_batches` yields them.
    chain: Vec<B>,
    /// Upper of the last batch in `chain`, or the minimum time for an empty chain.
    upper: Antichain<B::Time>,
    /// The inner trace's logical compaction frontier. Reads at times not beyond it are not accurate.
    logical: Antichain<B::Time>,
    /// The inner trace's physical compaction frontier.
    physical: Antichain<B::Time>,
    /// Readers' logical holds.
    remote_logical: MutableAntichain<B::Time>,
    /// Readers' physical holds.
    remote_physical: MutableAntichain<B::Time>,
    queues: Vec<Weak<ImportQueue<B>>>,
    /// Wakes the writer's arrange operator so a changed hold reaches the inner trace.
    writer_activator: Option<SyncActivator>,
    /// Set when the writer drops. Readers then complete after draining what was published.
    closed: bool,
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

/// A publication point: the view of one [`SharedSpine`] and the holds its readers register.
///
/// Created unattached, with an empty chain and frontiers at the minimum time. Readers may register
/// and import before a writer [attaches](SharedSpine::attach); their imports produce nothing until
/// then and are seeded from the writer's chain at attachment.
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
                physical: minimum,
                remote_logical: MutableAntichain::new(),
                remote_physical: MutableAntichain::new(),
                queues: Vec::new(),
                writer_activator: None,
                closed: false,
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

    /// The published `(since, upper)`, read together.
    pub fn frontiers(&self) -> (Antichain<B::Time>, Antichain<B::Time>) {
        let state = self.lock();
        (state.logical.clone(), state.upper.clone())
    }

    /// Whether the writer has dropped.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// The meet of the readers' logical holds. Empty when no reader holds.
    pub fn logical_holds(&self) -> Antichain<B::Time> {
        self.lock().remote_logical.frontier().to_owned()
    }

    /// The meet of the readers' physical holds. Empty when no reader holds.
    pub fn physical_holds(&self) -> Antichain<B::Time> {
        self.lock().remote_physical.frontier().to_owned()
    }

    /// A reader holding logical compaction at the published `since` and physical compaction at the
    /// chain's coverage.
    pub fn reader(self: &Arc<Self>) -> SharedReader<B> {
        let mut state = self.lock();
        let logical = state.logical.clone();
        let physical = state.upper.clone();
        adjust(&mut state.remote_logical, &Antichain::new(), &logical);
        adjust(&mut state.remote_physical, &Antichain::new(), &physical);
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
        adjust(&mut state.remote_logical, &Antichain::new(), as_of);
        adjust(&mut state.remote_physical, &Antichain::new(), &physical);
        drop(state);
        Ok(SharedReader {
            shared: Arc::clone(self),
            logical: as_of.clone(),
            physical,
        })
    }

    fn wake_writer(state: &State<B>) {
        if let Some(activator) = &state.writer_activator {
            let _ = activator.activate();
        }
    }
}

/// Replaces the elements of `lower` with those of `upper` in `accumulated`.
fn adjust<T: Timestamp>(
    accumulated: &mut MutableAntichain<T>,
    lower: &Antichain<T>,
    upper: &Antichain<T>,
) {
    accumulated.update_iter(upper.iter().cloned().map(|time| (time, 1)));
    accumulated.update_iter(lower.iter().cloned().map(|time| (time, -1)));
}

/// What a [`SharedSpine`] publishes into once attached.
struct Attachment<B: BatchReader> {
    shared: Arc<Shared<B>>,
    /// Fires after every publish that advanced `upper`, outside the state lock.
    on_seal: Box<dyn Fn()>,
}

/// A [`Trace`] that publishes its contents for [`SharedReader`]s once attached.
///
/// Unattached it delegates to the inner trace at the cost of one branch per call. It lives on the
/// arranging worker's thread only.
pub struct SharedSpine<Tr: Trace> {
    inner: Tr,
    /// The publication points this trace backs. One trace may be published under several ids, as
    /// the logging dataflow's single shared error arrangement is, so this is a list rather than a
    /// slot: attaching a second point must not silently detach the first.
    attachment: RefCell<Vec<Attachment<Tr::Batch>>>,
    /// The frontiers the local `TraceBox` last requested. The inner trace gets their meet with the
    /// readers' holds.
    local_logical: Antichain<Tr::Time>,
    local_physical: Antichain<Tr::Time>,
    chain_scratch: Vec<Tr::Batch>,
}

impl<Tr: Trace> SharedSpine<Tr> {
    /// Attaches this trace to `shared`, in addition to any points it is already attached to.
    ///
    /// Publishes the current chain and frontiers, and seeds every importer already registered on
    /// `shared` with them. `activator` should schedule the operator that drives this trace, so a
    /// reader moving a hold reaches the inner trace promptly; without it the hold applies at the
    /// operator's next activation. `on_seal` fires after every publish that advanced `upper`.
    ///
    /// Takes `&self` because the trace is reached through `TraceAgent::trace_box_unstable`, which
    /// exposes it by shared reference.
    pub fn attach(
        &self,
        shared: Arc<Shared<Tr::Batch>>,
        activator: Option<SyncActivator>,
        on_seal: impl Fn() + 'static,
    ) {
        let mut chain = Vec::new();
        self.inner.map_batches(|batch| chain.push(batch.clone()));
        let upper = chain_upper::<Tr::Batch>(&chain);
        let live = {
            let mut state = shared.lock();
            state.writer_activator = activator;
            state.chain = chain.clone();
            state.upper = upper.clone();
            // The frontiers the `TraceReader` impl below last saw. Reading them from `inner` here
            // would need `&mut`, and they are equal.
            state.logical = self.local_logical.clone();
            state.physical = self.local_physical.clone();
            state.live_queues()
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
        let entry = Attachment {
            shared: Arc::clone(&shared),
            on_seal: Box::new(on_seal),
        };
        let mut attachments = self.attachment.borrow_mut();
        match attachments
            .iter()
            .position(|attached| Arc::ptr_eq(&attached.shared, &shared))
        {
            Some(index) => attachments[index] = entry,
            None => attachments.push(entry),
        }
    }

    /// The first publication point this trace backs, if any.
    pub fn shared(&self) -> Option<Arc<Shared<Tr::Batch>>> {
        self.attachment
            .borrow()
            .first()
            .map(|attachment| Arc::clone(&attachment.shared))
    }

    /// Mirrors the inner trace's frontiers into the view.
    fn publish_frontiers(&mut self) {
        let attachments = self.attachment.borrow();
        if attachments.is_empty() {
            return;
        }
        let logical = self.inner.get_logical_compaction().to_owned();
        let physical = self.inner.get_physical_compaction().to_owned();
        for attachment in attachments.iter() {
            let mut state = attachment.shared.lock();
            state.logical = logical.clone();
            state.physical = physical.clone();
        }
    }

    /// Mirrors the inner trace's chain and frontiers into the view.
    ///
    /// Also enqueues `arrived`, a batch the inner trace just accepted, to every live importer, under
    /// the same lock as the chain, so a reader registering concurrently either seeds a chain
    /// containing that batch or receives it through its queue, never neither.
    fn publish_chain(&mut self, arrived: Option<Tr::Batch>) {
        let attachments = self.attachment.borrow();
        if attachments.is_empty() {
            return;
        }
        let chain = &mut self.chain_scratch;
        chain.clear();
        self.inner.map_batches(|batch| chain.push(batch.clone()));
        let upper = chain_upper::<Tr::Batch>(chain);
        let logical = self.inner.get_logical_compaction().to_owned();
        let physical = self.inner.get_physical_compaction().to_owned();

        for attachment in attachments.iter() {
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

            if let Some(batch) = &arrived {
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

    /// Applies the meet of the local and the readers' holds to the inner trace.
    fn apply_holds(&mut self) {
        let attachment = self.attachment.borrow();
        // Reading the holds and publishing the frontier they produce happen under one acquisition,
        // and the inner trace is touched only afterwards. A reader that registers before us is
        // counted in the meet. One that registers after us sees the frontier we are about to apply,
        // and `reader_at` refuses an `as_of` below it. So no reader is ever admitted at a time the
        // trace is about to coalesce away, which a read-then-release-then-apply order permits for
        // the length of the merge it runs.
        //
        // Publishing the target before applying it means the point can briefly advertise a `since`
        // ahead of the trace's own. That refuses a reader the trace could still have served, which
        // is the safe direction. The reverse admits a reader the trace cannot serve.
        //
        // Points are locked in address order, so two traces sharing a pair of points cannot
        // deadlock. The critical section is antichain arithmetic, never merge work.
        let mut ordered: Vec<_> = attachment.iter().collect();
        ordered.sort_unstable_by_key(|attached| Arc::as_ptr(&attached.shared));
        let mut guards: Vec<_> = ordered
            .iter()
            .map(|attached| attached.shared.lock())
            .collect();

        // The meet runs across every point, since a reader of any of them holds this one trace.
        let mut remote_logical = Antichain::new();
        let mut remote_physical = Antichain::new();
        for state in guards.iter() {
            remote_logical = remote_logical.meet(&state.remote_logical.frontier().to_owned());
            remote_physical = remote_physical.meet(&state.remote_physical.frontier().to_owned());
        }
        // The empty antichain is the identity of `meet`: a side with no holds constrains nothing.
        let logical = self.local_logical.meet(&remote_logical);
        for state in guards.iter_mut() {
            state.logical = logical.clone();
        }
        drop(guards);
        drop(attachment);

        if self.inner.get_logical_compaction() != logical.borrow() {
            self.inner.set_logical_compaction(logical.borrow());
        }
        let physical = self.local_physical.meet(&remote_physical);
        // The spine refuses to rewind its physical frontier. A reader registers its physical hold
        // at the chain coverage it was seeded with, which is at or beyond the spine's frontier, so
        // the meet cannot regress. The guard documents that rather than trusting it.
        if self.inner.get_physical_compaction() != physical.borrow()
            && PartialOrder::less_equal(&self.inner.get_physical_compaction(), &physical.borrow())
        {
            self.inner.set_physical_compaction(physical.borrow());
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

impl<Tr: Trace> TraceReader for SharedSpine<Tr> {
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Self::Time>) -> Option<Vec<Self::Batch>> {
        self.inner.batches_through(upper)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Self::Time>) {
        self.local_logical = frontier.to_owned();
        self.apply_holds();
        self.publish_frontiers();
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_logical_compaction()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Self::Time>) {
        self.local_physical = frontier.to_owned();
        self.apply_holds();
        // Physical compaction introduces pending batches and can complete merges.
        self.publish_chain(None);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Self::Time> {
        self.inner.get_physical_compaction()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.inner.map_batches(f)
    }
}

impl<Tr: Trace> Trace for SharedSpine<Tr> {
    fn new(
        info: OperatorInfo,
        logging: Option<differential_dataflow::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {
        let minimum = Antichain::from_elem(Tr::Time::minimum());
        SharedSpine {
            inner: Tr::new(info, logging, activator),
            attachment: RefCell::new(Vec::new()),
            local_logical: minimum.clone(),
            local_physical: minimum,
            chain_scratch: Vec::new(),
        }
    }

    fn exert(&mut self) {
        self.apply_holds();
        self.inner.exert();
        self.publish_chain(None);
    }

    fn set_exert_logic(&mut self, logic: ExertionLogic) {
        self.inner.set_exert_logic(logic)
    }

    fn insert(&mut self, batch: Self::Batch) {
        self.inner.insert(batch.clone());
        self.publish_chain(Some(batch));
    }

    fn close(&mut self) {
        let mut upper = Antichain::new();
        self.inner.read_upper(&mut upper);
        if !upper.is_empty() {
            self.insert(Tr::Batch::empty(upper, Antichain::new()));
        }
    }
}

impl<Tr: Trace> Drop for SharedSpine<Tr> {
    fn drop(&mut self) {
        for attachment in self.attachment.take() {
            let live = {
                let mut state = attachment.shared.lock();
                state.closed = true;
                state.writer_activator = None;
                state.live_queues()
            };
            for queue in live {
                queue.push([Replay::Frontier(Antichain::new())]);
                queue.activate();
            }
        }
    }
}

/// A `Clone + Send` reader of a [`Shared`] publication point.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone is an independent hold, as each `TraceAgent` clone is, so two
/// consumers of one import cannot release each other's holds.
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
}

impl<B: BatchReader> Clone for SharedReader<B> {
    fn clone(&self) -> Self {
        {
            let mut state = self.shared.lock();
            adjust(&mut state.remote_logical, &Antichain::new(), &self.logical);
            adjust(
                &mut state.remote_physical,
                &Antichain::new(),
                &self.physical,
            );
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
            adjust(&mut state.remote_logical, &self.logical, &Antichain::new());
            adjust(
                &mut state.remote_physical,
                &self.physical,
                &Antichain::new(),
            );
            Shared::wake_writer(&state);
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
        let mut out = Vec::new();
        for batch in state.chain.iter() {
            if PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
                break;
            }
            if !batch.is_empty() {
                assert!(
                    PartialOrder::less_equal(&batch.upper().borrow(), &upper),
                    "batches_through: upper straddles batch"
                );
                out.push(batch.clone());
            }
        }
        Some(out)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<B::Time>) {
        // Join rather than overwrite, as `TraceAgent` does: a hold is the joint consequence of every
        // frontier it has been asked to hold, and the getter reports what is held. Joining with the
        // empty antichain is absorbing, so an empty request still releases.
        let next = self.logical.join(&frontier.to_owned());
        let previous = std::mem::replace(&mut self.logical, next);
        let mut state = self.shared.lock();
        adjust(&mut state.remote_logical, &previous, &self.logical);
        Shared::wake_writer(&state);
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
        adjust(&mut state.remote_physical, &previous, &self.physical);
        Shared::wake_writer(&state);
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
                // A closed writer's terminal frontier has been and gone, so seed our own.
                if state.closed {
                    instructions.push_back(Replay::Frontier(Antichain::new()));
                }
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
                            // Bound the read at `until`, and complete on the writer's terminal empty
                            // frontier. Otherwise track `upper`, keeping the stream frontier equal
                            // to the trace's upper.
                            if frontier.is_empty() || PartialOrder::less_equal(&until, &frontier) {
                                capabilities.downgrade(std::iter::empty::<B::Time>());
                                hold = None;
                                break;
                            }
                            capabilities.downgrade(&frontier.borrow()[..]);
                        }
                        Replay::Batch(batch) => {
                            // The writer enqueues a batch under the lock that publishes the chain
                            // containing it, so a batch in the seed cannot also arrive live. This
                            // guards the invariant rather than trusting it: emitting a covered batch
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
