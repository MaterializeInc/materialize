// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Publishing a trace to readers on other threads.
//!
//! An arrangement's trace is owned by the worker that arranges into it and is not `Send`: its
//! handles are `Rc<RefCell<..>>`, and its spine does merge work whenever it is touched, including
//! inside `roll_up` and `complete_at`, which finish merges synchronously as part of structural
//! changes. A spine behind a mutex would therefore hold that mutex for whole merges. The batches,
//! though, are immutable and reference counted, so a chain of spans plus the trace's frontiers is a
//! consistent, self-describing view that any thread can read.
//!
//! The [`TraceAgent`](crate::arrange_agent::TraceAgent)s of [`crate::arrange_agent`] carry this
//! capability. Once [published](crate::arrange_agent::TraceAgent::publish) to a [`Shared`] point,
//! the agent's [`TraceBox`] mirrors the spine's span chain, `upper`, and compaction frontiers into
//! the point after every mutation its [`TraceWriter`](crate::arrange_agent::TraceWriter) makes. The
//! lock is held for a chain swap, never for merge work. A trace nobody published pays no mirror
//! cost and takes no lock.
//!
//! Every handle holds the trace through one accounting. While unpublished, the box keeps the holds
//! of its same-thread agents, as differential's `TraceBox` does. Publishing moves them into the
//! point the trace first publishes to, its home, where [`SharedReader`]s on any thread register
//! theirs next to them. The box applies the meet to the spine. A reader that moves the meet wakes
//! the writer so the spine learns of it without waiting for the arrangement's next input.
//!
//! Logical compaction decides which times stay distinguishable, physical compaction which spans
//! may merge. A reader needs distinguishability at the times it reads, and a span boundary at each
//! frontier it passes to `cursor_through`. Its physical hold therefore starts at the chain's
//! coverage, which is where an import seeded with that chain makes its first cut, and follows the
//! frontiers it acknowledges from there.
//!
//! A publication point is a [`Rendezvous`] whose items are spans and whose hold kinds are the two
//! compaction axes. The rendezvous owns the synchronization: the lock, the one trace a point accepts
//! in its life, the hold accumulations, the importer queues, and freezing readers when the writer
//! goes. This module owns what a span chain means.

use std::convert::Infallible;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::wrappers::frontier::{BatchFrontier, TraceFrontier};
use differential_dataflow::trace::{Description, Span, Trace, TraceReader};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::source;
use timely::order::TotalOrder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::activate::SyncActivator;

use crate::arrange_agent::trace_box::TraceBox;
use crate::rendezvous::{self, Attachment, Event, Locked, Rendezvous, adjust};

/// The hold kind of logical compaction.
const LOGICAL: usize = 0;
/// The hold kind of physical compaction.
const PHYSICAL: usize = 1;

/// The compaction frontiers a publication point carries next to its chain.
pub struct TraceFrontiers<T> {
    /// The spine's logical compaction frontier. Reads at times not beyond it are not accurate.
    logical: Antichain<T>,
    /// The logical compaction frontier the publishing trace's same-thread agents asked for, before
    /// the readers' holds were met into it.
    ///
    /// Equal to `logical` when nothing holds the trace back. Above it by exactly the amount the
    /// readers on other threads keep the writer from compacting. A closed trace stops updating
    /// both.
    requested_logical: Antichain<T>,
    /// The spine's physical compaction frontier.
    physical: Antichain<T>,
}

/// A publication point: the view of one published trace and the holds its handles register.
///
/// Created unattached, with an empty chain and frontiers at the minimum time. Readers may register
/// and import before a trace [publishes](crate::arrange_agent::TraceAgent::publish) to it. Their
/// imports produce nothing until then and are seeded from the trace's chain at attachment.
///
/// The logical holds of every handle on a point form its first hold kind, the physical holds its
/// second. On the home point of a published trace they include the trace's same-thread agents.
pub type Shared<T, B> = Rendezvous<T, Span<T, B>, TraceFrontiers<T>, 2>;

/// The primitive's attachment of a trace to one point.
type TraceAttachment<T, B> = Attachment<T, Span<T, B>, TraceFrontiers<T>, 2>;

impl<T: Timestamp + Lattice, B> Default for Shared<T, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, B: Clone> Shared<T, B> {
    /// The published chain. Holding it pins its batches.
    pub fn chain(&self) -> Vec<Span<T, B>> {
        self.read(|published| published.items.clone())
    }
}

impl<T: Timestamp + Lattice, B> Shared<T, B> {
    /// An unattached publication point.
    pub fn new() -> Self {
        let minimum = Antichain::from_elem(T::minimum());
        Rendezvous::with_meta(TraceFrontiers {
            logical: minimum.clone(),
            requested_logical: minimum.clone(),
            physical: minimum,
        })
    }

    /// The published `upper`: spans strictly below it are complete and readable.
    pub fn upper(&self) -> Antichain<T> {
        self.read(|published| published.upper.clone())
    }

    /// The published logical compaction frontier. Reads at times not beyond it are not accurate.
    pub fn since(&self) -> Antichain<T> {
        self.read(|published| published.meta.logical.clone())
    }

    /// The published `(since, upper)`, read together.
    pub fn frontiers(&self) -> (Antichain<T>, Antichain<T>) {
        self.read(|published| (published.meta.logical.clone(), published.upper.clone()))
    }

    /// The published `since`, and the logical compaction frontier the publishing trace's agents
    /// asked for before the readers' holds were met into it, read together. The second is at or
    /// beyond the first, and equal to it when no reader holds this point.
    pub fn logical_frontiers(&self) -> (Antichain<T>, Antichain<T>) {
        self.read(|published| {
            (
                published.meta.logical.clone(),
                published.meta.requested_logical.clone(),
            )
        })
    }

    /// The meet of every logical hold on this point, the publishing trace's agents included.
    /// Empty when nothing holds.
    pub fn logical_holds(&self) -> Antichain<T> {
        self.holds(LOGICAL)
    }

    /// The meet of every physical hold on this point, as [`Self::logical_holds`].
    pub fn physical_holds(&self) -> Antichain<T> {
        self.holds(PHYSICAL)
    }

    /// A reader holding logical compaction at the published `since` and physical compaction at the
    /// chain's coverage.
    pub fn reader(self: &Arc<Self>) -> SharedReader<T, B> {
        let reader = self
            .reader_with(|published| {
                Ok::<_, Infallible>([published.meta.logical.clone(), published.upper.clone()])
            })
            .unwrap_or_else(|never| match never {});
        SharedReader { reader }
    }

    /// A reader holding logical compaction at `as_of`, or `Err` with the published `since` when
    /// that is already beyond `as_of`.
    ///
    /// Checks and registers under one lock acquisition, so a returned reader holds a frontier the
    /// trace can still honour.
    pub fn reader_at(
        self: &Arc<Self>,
        as_of: &Antichain<T>,
    ) -> Result<SharedReader<T, B>, Antichain<T>> {
        let reader = self.reader_with(|published| {
            if !PartialOrder::less_equal(&published.meta.logical, as_of) {
                return Err(published.meta.logical.clone());
            }
            Ok([as_of.clone(), published.upper.clone()])
        })?;
        Ok(SharedReader { reader })
    }

    /// A reader holding logical compaction at the join of `as_of` and the published `since`, and
    /// nothing physically.
    ///
    /// Never refuses. It is the hold a runtime keeps on a collection its peer publishes, whose
    /// `as_of` can sit below the published `since`, and which reads only through readers it mints
    /// with [`SharedReader::fresh_reader`].
    pub fn reader_at_least(self: &Arc<Self>, as_of: &Antichain<T>) -> SharedReader<T, B> {
        let reader = self
            .reader_with(|published| {
                Ok::<_, Infallible>([as_of.join(&published.meta.logical), Antichain::new()])
            })
            .unwrap_or_else(|never| match never {});
        SharedReader { reader }
    }
}

/// The spans with updates of the totally ordered `chain` before the cut `upper`.
///
/// Panics if a span with updates straddles the cut, which means a merge ate a boundary the reader
/// needed.
fn chain_through<T: Timestamp, B: Clone>(
    chain: &[Span<T, B>],
    upper: AntichainRef<T>,
) -> Vec<Span<T, B>> {
    let mut out = Vec::new();
    for span in chain {
        if PartialOrder::less_equal(&upper, &span.lower().borrow()) {
            break;
        }
        if span.has_updates() {
            assert!(
                PartialOrder::less_equal(&span.upper().borrow(), &upper),
                "chain_through: upper straddles span"
            );
            out.push(span.clone());
        }
    }
    out
}

/// The frontier a chain covers: its last span's upper, or the minimum for an empty chain.
fn chain_upper<T: Timestamp, B>(chain: &[Span<T, B>]) -> Antichain<T> {
    chain.last().map_or_else(
        || Antichain::from_elem(T::minimum()),
        |span| span.upper().clone(),
    )
}

/// A point a [`TraceBox`] publishes into.
struct Attached<T, B> {
    attachment: TraceAttachment<T, B>,
    /// Fires after every publish that advanced `upper`, outside the state lock.
    on_seal: Box<dyn Fn()>,
}

/// The points a published trace backs, held by its [`TraceBox`].
pub struct Publication<T, B> {
    /// The first point the trace published to. Its holds include the trace's same-thread agents.
    home: Attached<T, B>,
    /// Further points over the same trace, as the logging dataflow's single shared error
    /// arrangement is published under every log id. They carry their readers' holds only.
    others: Vec<Attached<T, B>>,
    /// The logical holds of the trace's same-thread agents alone. The home point's logical holds
    /// count them too, next to the readers', and are what compaction follows. This copy only
    /// separates out what the agents asked for, which every point reports as `requested_logical`.
    agents_logical: MutableAntichain<T>,
    chain_scratch: Vec<Span<T, B>>,
    /// The descriptions of the chain last published, which `exert` compares the spine against.
    published_shape: Vec<Description<T>>,
}

impl<T, B> Publication<T, B> {
    fn attachments(&self) -> impl Iterator<Item = &Attached<T, B>> {
        std::iter::once(&self.home).chain(self.others.iter())
    }

    fn attachments_mut(&mut self) -> impl Iterator<Item = &mut Attached<T, B>> {
        std::iter::once(&mut self.home).chain(self.others.iter_mut())
    }
}

impl<Tr: TraceReader> TraceBox<Tr> {
    /// The published counterpart of `adjust_logical_compaction`: the agents' holds live in the
    /// home point.
    pub(crate) fn adjust_published_logical(
        &mut self,
        lower: AntichainRef<Tr::Time>,
        upper: AntichainRef<Tr::Time>,
    ) {
        if let Some(publication) = &mut self.publication {
            adjust(&mut publication.agents_logical, &lower[..], &upper[..]);
        }
        self.sync_holds(|locked| locked.adjust(0, LOGICAL, &lower[..], &upper[..]));
    }

    /// The published counterpart of `adjust_physical_compaction`.
    pub(crate) fn adjust_published_physical(
        &mut self,
        lower: AntichainRef<Tr::Time>,
        upper: AntichainRef<Tr::Time>,
    ) {
        // Compute's maintenance sets this on every trace every few milliseconds, mostly to the
        // frontier it already has, and publishing then costs locks and clones per trace. A moved
        // hold introduces pending merges and is published, and a reader's moved hold reaches the
        // spine through `exert`, which the hold's activation schedules.
        let moved = self.sync_holds(|locked| locked.adjust(0, PHYSICAL, &lower[..], &upper[..]));
        if moved {
            self.publish_chain(None);
        }
    }

    /// Applies `change` to the locked points, the home point at index zero, and when it reports
    /// the meet moved, applies the meet of every point's holds to the spine. Returns whether it
    /// applied. Unpublished, it does nothing.
    fn sync_holds(
        &mut self,
        change: impl FnOnce(
            &mut Locked<'_, Tr::Time, Span<Tr::Time, Tr::Batch>, TraceFrontiers<Tr::Time>, 2>,
        ) -> bool,
    ) -> bool {
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
        // The critical section is antichain arithmetic, never merge work.
        let attachments: Vec<_> = publication
            .attachments()
            .map(|attached| &attached.attachment)
            .collect();
        let mut locked = Attachment::lock_all(&attachments);
        let moved = change(&mut locked);
        // An agent's move that a reader's lower hold keeps from moving the meet still changes what
        // the agents asked for, so the requested frontier is published before the early return.
        // Only where this trace has not closed: a closed trace publishes nothing.
        let requested = publication.agents_logical.frontier();
        locked.update_open(|frontiers| {
            if frontiers.requested_logical.borrow() != requested {
                frontiers.requested_logical = requested.to_owned();
            }
        });
        if !moved {
            return false;
        }
        // The meet runs across every point, since a reader of any of them holds this one trace.
        let logical = locked.meet(LOGICAL);
        let physical = locked.meet(PHYSICAL);
        // The join keeps the spine from rewinding, which differential's spine would accept without
        // complaint: a hold below the spine's `since`, as a reader registered on a placeholder may
        // place, cannot be honoured, so it constrains nothing.
        let logical = logical.join(&current_logical);
        locked.update_open(|frontiers| frontiers.logical = logical.clone());
        drop(locked);

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
    /// Also enqueues `arrived`, a span the spine just accepted, to every importer that was live
    /// under the lock that swapped in the chain containing it. A reader registering concurrently
    /// therefore either seeds a chain containing that span or is in that snapshot and receives it
    /// through its queue, never neither. The push itself happens after the lock is released.
    fn publish_chain(&mut self, arrived: Option<&Span<Tr::Time, Tr::Batch>>) {
        let Some(publication) = &mut self.publication else {
            return;
        };
        // A point only this trace references has no reader and no owner left. Publishing into it
        // would cost a chain clone, a lock, and an `on_seal` per mutation for as long as the trace
        // lives, which for a point published over another collection's trace outlasts the point's
        // own collection.
        publication
            .others
            .retain(|attached| attached.attachment.reachable());
        if !publication.home.attachment.reachable() && publication.others.is_empty() {
            self.unpublish();
            return;
        }
        // Every point closes together, in `close`.
        if publication.home.attachment.is_closed() {
            return;
        }
        let mut chain = std::mem::take(&mut publication.chain_scratch);
        chain.clear();
        self.trace.map_spans(|span| chain.push(span.clone()));
        let upper = chain_upper(&chain);
        let logical = self.trace.get_logical_compaction().to_owned();
        let physical = self.trace.get_physical_compaction().to_owned();

        for attached in publication
            .attachments_mut()
            .filter(|attached| attached.attachment.reachable())
        {
            // Built before the lock and swapped in, so neither the allocation nor the old chain's
            // reference count drops happen while the lock is held.
            let advanced = attached.attachment.publish(
                chain.clone(),
                upper.clone(),
                |frontiers| {
                    frontiers.logical = logical.clone();
                    frontiers.physical = physical.clone();
                },
                arrived,
            );
            if advanced {
                (attached.on_seal)();
            }
        }
        publication.published_shape.clear();
        publication
            .published_shape
            .extend(chain.iter().map(|span| span.desc.clone()));
        chain.clear();
        publication.chain_scratch = chain;
    }

    /// Whether the spine's chain differs from the one last published, span by span.
    fn chain_changed(&self) -> bool {
        let Some(publication) = &self.publication else {
            return false;
        };
        let shape = &publication.published_shape;
        let mut index = 0;
        let mut changed = false;
        self.trace.map_spans(|span| {
            changed |= shape.get(index).is_none_or(|desc| {
                desc.lower() != span.lower()
                    || desc.upper() != span.upper()
                    || desc.since() != span.desc.since()
            });
            index += 1;
        });
        changed || index != shape.len()
    }

    /// Takes the holds back from a home point nothing else references and stops publishing.
    ///
    /// No reader references the point, and no other trace is attached to it, so every hold on it is
    /// one of this trace's same-thread agents'.
    fn unpublish(&mut self) {
        let Some(publication) = self.publication.take() else {
            return;
        };
        let [logical, physical] = publication.home.attachment.into_holds();
        self.logical_compaction.update_iter(logical);
        self.physical_compaction.update_iter(physical);
    }

    /// Publishes this trace to `shared`, in addition to any points it already backs.
    ///
    /// Mirrors the current chain and frontiers, and seeds every importer already registered on
    /// `shared` with them. The first point a trace publishes to becomes its home and takes over the
    /// holds of the trace's same-thread agents. `activator` must schedule the operator that drives
    /// this trace: a reader moves its hold from another thread, and only that operator's next
    /// `exert` applies it to the spine. `on_seal` fires after every publish that advanced `upper`,
    /// on this thread. No other trace may ever have attached to `shared`, which
    /// `Rendezvous::attach` panics on.
    pub(crate) fn attach(
        &mut self,
        shared: Arc<Shared<Tr::Time, Tr::Batch>>,
        activator: SyncActivator,
        on_seal: impl Fn() + 'static,
    ) {
        if let Some(publication) = &mut self.publication {
            let attached = std::iter::once(&mut publication.home)
                .chain(publication.others.iter_mut())
                .find(|attached| Arc::ptr_eq(attached.attachment.point(), &shared));
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
        self.trace.map_spans(|span| chain.push(span.clone()));
        let upper = chain_upper(&chain);
        let logical = self.trace.get_logical_compaction().to_owned();
        let physical = self.trace.get_physical_compaction().to_owned();
        let holds = if home {
            [
                self.logical_compaction.updates().cloned().collect(),
                self.physical_compaction.updates().cloned().collect(),
            ]
        } else {
            [Vec::new(), Vec::new()]
        };
        // An importer seeded by an earlier writer has acknowledged that writer's frontiers, and
        // this trace's chain regresses below them, which an importer cannot follow: a span
        // straddling its acknowledged frontier panics its capability downgrade. That includes an
        // importer registered after the earlier writer dropped, which was seeded from its frozen
        // view. So `Rendezvous::attach` refuses a point that ever had a writer, and the caller
        // hands this trace a point of its own.
        let attachment = shared.attach(
            activator,
            chain,
            upper,
            |frontiers| {
                frontiers.logical = logical;
                frontiers.physical = physical;
            },
            holds,
        );
        let attached = Attached {
            attachment,
            on_seal: Box::new(on_seal),
        };
        match &mut self.publication {
            Some(publication) => publication.others.push(attached),
            None => {
                let mut agents_logical = MutableAntichain::new();
                agents_logical.update_iter(self.logical_compaction.updates().cloned());
                self.logical_compaction = MutableAntichain::new();
                self.physical_compaction = MutableAntichain::new();
                self.publication = Some(Publication {
                    home: attached,
                    others: Vec::new(),
                    agents_logical,
                    chain_scratch: Vec::new(),
                    published_shape: Vec::new(),
                });
            }
        }
        // Holds readers registered on the point before it was attached take effect now.
        self.sync_holds(|_| true);
    }
}

impl<Tr: Trace> TraceBox<Tr> {
    /// Introduces `span` to the spine, and mirrors the result into the points it is published to.
    pub(crate) fn insert(&mut self, span: Span<Tr::Time, Tr::Batch>) {
        if self.publication.is_none() {
            self.trace.insert(span);
            return;
        }
        self.trace.insert(span.clone());
        self.publish_chain(Some(&span));
    }

    /// Introduces the empty-frontier seal a dropped writer makes to the spine, and stops mirroring
    /// the spine into the points. The points keep what they last received. Holds keep moving, so
    /// the agents still release theirs.
    pub(crate) fn close(&mut self, span: Span<Tr::Time, Tr::Batch>) {
        // NOTE: Readers keep the `upper` this trace last published, whichever of the trace and its
        // writer drops first. A trace dropped first publishes nothing more. A writer dropped first
        // seals the spine to the empty frontier, and closing the attachments keeps that seal from
        // the points. `Attachment::close` records why readers must not see it. An importing
        // dataflow is dropped with `drop_dataflow` too.
        self.trace.insert(span);
        if let Some(publication) = &mut self.publication {
            publication.home.attachment.close();
            for attached in &mut publication.others {
                attached.attachment.close();
            }
        }
    }

    /// Exerts merge effort on the spine, after applying any hold a reader moved since the last
    /// activation.
    ///
    /// Reads the points' holds only when a reader moved a meet, and publishes only when that moved
    /// the spine's frontiers, the merge work changed the chain, or a point became unreachable, so an
    /// activation that changes nothing takes no lock.
    pub(crate) fn exert(&mut self) {
        let Some(publication) = &self.publication else {
            self.trace.exert();
            return;
        };
        // Every flag is taken, so none stays set for a move this sync already applies.
        let readers_moved = publication.attachments().fold(false, |moved, attached| {
            attached.attachment.take_holds_moved() | moved
        });
        let synced = readers_moved && self.sync_holds(|_| true);
        self.trace.exert();
        let unreachable = self.publication.as_ref().is_some_and(|publication| {
            publication
                .attachments()
                .any(|attached| !attached.attachment.reachable())
        });
        if synced || unreachable || self.chain_changed() {
            self.publish_chain(None);
        }
    }
}

/// A `Clone + Send` reader of a [`Shared`] publication point.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone is an independent hold, as each
/// [`TraceAgent`](crate::arrange_agent::TraceAgent) clone is, so two consumers of one import cannot
/// release each other's holds.
pub struct SharedReader<T: Timestamp, B> {
    /// Holds logical compaction as its first kind and physical compaction, seeded at the chain
    /// coverage, as its second. See the module docs.
    reader: rendezvous::Reader<T, Span<T, B>, TraceFrontiers<T>, 2>,
}

impl<T: Timestamp + Lattice, B> SharedReader<T, B> {
    /// The publication point.
    pub fn shared(&self) -> &Arc<Shared<T, B>> {
        self.reader.point()
    }

    /// A reader at this reader's logical hold that also holds the chain physically at the chain's
    /// coverage when minted, as an import must.
    ///
    /// # Panics
    ///
    /// Panics if the published `since` is beyond this reader's logical hold. That happens only for a
    /// hold registered on an unattached point below the `since` of the trace that later attaches,
    /// see [`Shared::reader_at_least`]. Compute registers a peer's hold at its index's `as_of`, and
    /// the publishing trace's `since` is at or below that `as_of` when it attaches, because the
    /// controller selects a dataflow's `as_of` at or beyond the `since` of its inputs and allows no
    /// compaction past a read hold.
    pub fn fresh_reader(&self) -> SharedReader<T, B> {
        self.shared()
            .reader_at(&self.reader.hold(LOGICAL).to_owned())
            .expect("the publication's since is at or below this reader's logical hold")
    }
}

impl<T: Timestamp, B> Clone for SharedReader<T, B> {
    fn clone(&self) -> Self {
        SharedReader {
            reader: self.reader.clone(),
        }
    }
}

impl<T, B> TraceReader for SharedReader<T, B>
where
    // `spans_through` stops at the first span whose lower is beyond the cut and takes every
    // later span to be past it too, which holds because the chain is totally ordered.
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    type Time = T;
    type Batch = B;

    fn spans_through(&mut self, upper: AntichainRef<T>) -> Option<Vec<Span<T, B>>> {
        // NOTE: `Spine::spans_through` asserts that the cut is at or beyond the spine's physical
        // frontier. That does not hold for a shared reader, whose cut can sit below the meet the
        // writer applied while it drains a seed. The straddle check is the guard instead: a span
        // straddling the cut means a merge ate a boundary this reader still needed, and returning it
        // would hand back updates at times not before `upper`.
        Some(
            self.reader
                .point()
                .read(|published| chain_through(&published.items, upper)),
        )
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<T>) {
        // Join rather than overwrite, as `TraceAgent` does: a hold is the joint consequence of
        // every frontier it has been asked to hold, and the getter reports what is held. Joining
        // with the empty antichain is absorbing, so an empty request still releases.
        let next = self
            .reader
            .hold(LOGICAL)
            .to_owned()
            .join(&frontier.to_owned());
        // An unchanged hold would lock the point for nothing.
        if self.reader.hold(LOGICAL) != next.borrow() {
            self.reader.set_hold(LOGICAL, next);
        }
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.reader.hold(LOGICAL)
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, T>) {
        // Join, never assign: the hold starts at the chain coverage, and a request below it must not
        // pull it down to where a merge could eat the boundary this reader was seeded with.
        let next = self
            .reader
            .hold(PHYSICAL)
            .to_owned()
            .join(&frontier.to_owned());
        if self.reader.hold(PHYSICAL) != next.borrow() {
            self.reader.set_hold(PHYSICAL, next);
        }
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.reader.hold(PHYSICAL)
    }

    fn read_upper(&mut self, target: &mut Antichain<T>) {
        // The published `upper` is the chain's coverage, so this skips the chain's clone.
        self.shared()
            .read(|published| target.clone_from(&published.upper));
    }

    fn map_spans<F: FnMut(&Span<T, B>)>(&self, f: F) {
        // Iterates a clone, so `f` runs outside the point's lock and may touch the point.
        self.shared().chain().iter().for_each(f);
    }
}

impl<T, B> SharedReader<T, B>
where
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
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
        scope: Scope<'scope, T>,
        name: &str,
        as_of: Antichain<T>,
        until: Antichain<T>,
    ) -> Arranged<'scope, TraceFrontier<SharedReader<T, B>>> {
        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        let shared = Arc::clone(self.shared());
        let mut hold = Some(self.clone());

        let stream = source(scope, name, move |capability, info| {
            let activator = scope.worker().sync_activator_for(info.address.to_vec());

            // Register under one lock acquisition: seed the queue with the chain and the frontier it
            // covers, then publish the queue weakly. Later spans append, earlier ones are seeded.
            let (queue, seed) = shared.subscribe(activator);

            // Emptied once the read is over, which is the operator's "done" state.
            let mut capabilities = CapabilitySet::from_elem(capability);
            let mut acknowledged = seed.clone();
            // The seeded instructions come first and are emitted as-is. Everything after the seed's
            // own `Upper` is live and is filtered against `seed` below.
            let mut draining_seed = true;

            move |output| {
                // Drains this importer's own queue only, never the publication point's lock.
                let drained = queue.drain();
                if capabilities.is_empty() {
                    return;
                }
                for instruction in drained {
                    match instruction {
                        Event::Upper(frontier) => {
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
                            // registered at and the chain grows one span per seal for its life.
                            if let Some(hold) = hold.as_mut() {
                                hold.set_logical_compaction(acknowledged.borrow());
                                hold.set_physical_compaction(acknowledged.borrow());
                            }
                            // Bound the read at `until`, and complete on the empty frontier a writer
                            // whose collection is complete publishes. Otherwise track `upper`,
                            // keeping the stream frontier equal to the trace's upper.
                            if frontier.is_empty() || PartialOrder::less_equal(&until, &frontier) {
                                capabilities.downgrade(std::iter::empty::<T>());
                                hold = None;
                                break;
                            }
                            capabilities.downgrade(&frontier.borrow()[..]);
                        }
                        Event::Item(span) => {
                            // The writer enqueues a span only to importers live under the lock
                            // that published the chain containing it, so a span in the seed cannot
                            // also arrive live. This guards the invariant: emitting a covered span
                            // twice would double count it under a capability already moved past.
                            if !draining_seed
                                && PartialOrder::less_equal(&span.upper().borrow(), &seed.borrow())
                            {
                                continue;
                            }
                            if !span.has_updates() {
                                continue;
                            }
                            // The chain is contiguous and totally ordered, so `lower` has one
                            // element, at or beyond the frontier the capability sits at.
                            let time = span.lower().elements()[0].clone();
                            let cap = capabilities.delayed(&time);
                            let wrapped = Span::new(
                                span.desc,
                                span.inner.map(|b| {
                                    BatchFrontier::make_from(b, as_of.borrow(), until.borrow())
                                }),
                            );
                            output.session(&cap).give(wrapped);
                        }
                    }
                }
            }
        });

        Arranged { stream, trace }
    }
}
