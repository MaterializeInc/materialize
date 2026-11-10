// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A reader of a publication point that is chosen after the reader exists.
//!
//! An interactive read can be rendered before the maintenance runtime published the incarnation it
//! reads, so its reader cannot name a point yet. A [`Binding`] stands in for the point until the
//! point is [bound](Binding::bind), and a [`BindingReader`] reads through it: as an empty chain at
//! the minimum before the bind, and as a [`SharedReader`] would after.
//!
//! Logical holds do not live in the point. Every reader of a binding registers its logical hold in
//! the binding's [`HoldAccumulation`], which a [`ProgressPusher`] carries to the writer as a stream.
//! Physical holds stay in the point, registered once it is bound.

use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::wrappers::frontier::TraceFrontier;
use differential_dataflow::trace::{Span, TraceReader};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::source;
use timely::order::TotalOrder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::Activator;

use crate::capture::ProgressPusher;
use crate::rendezvous::adjust;
use crate::shared_trace::{ImportDrain, ImportHold, Shared, SharedReader, chain_through};

/// The logical holds of every reader of one or more [`Binding`]s.
///
/// Clones share the accumulation. Not `Send`: it lives on the thread of its readers.
pub struct HoldAccumulation<T: Timestamp> {
    inner: Rc<RefCell<HoldsInner<T>>>,
}

struct HoldsInner<T: Timestamp> {
    holds: MutableAntichain<T>,
    pusher: Option<ProgressPusher<T>>,
}

impl<T: Timestamp> Clone for HoldAccumulation<T> {
    fn clone(&self) -> Self {
        HoldAccumulation {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T: Timestamp> Default for HoldAccumulation<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Timestamp> HoldAccumulation<T> {
    /// An accumulation holding nothing, connected to no pusher.
    pub fn new() -> Self {
        HoldAccumulation {
            inner: Rc::new(RefCell::new(HoldsInner {
                holds: MutableAntichain::new(),
                pusher: None,
            })),
        }
    }

    /// The meet of every hold. Empty when nothing holds.
    pub fn frontier(&self) -> Antichain<T> {
        self.inner.borrow().holds.frontier().to_owned()
    }

    /// Pushes the current frontier to `pusher`, and every changed frontier after.
    ///
    /// A hold registered below the frontier already pushed regresses it, which the pusher refuses.
    /// Connecting while nothing holds is a bug, which is logged.
    ///
    /// # Panics
    ///
    /// Panics if the accumulation is already connected.
    pub fn connect(&self, mut pusher: ProgressPusher<T>) {
        let mut inner = self.inner.borrow_mut();
        assert!(
            inner.pusher.is_none(),
            "a hold accumulation connects to one pusher"
        );
        // NOTE: An empty frontier completes the replayed stream for good, and a hold registered
        // later would regress it, which the pusher refuses. The first hold must be registered
        // before connecting.
        mz_ore::soft_assert_or_log!(
            !inner.holds.frontier().is_empty(),
            "a hold accumulation connected while it holds nothing"
        );
        pusher.advance_to(inner.holds.frontier());
        inner.pusher = Some(pusher);
    }

    /// Replaces the hold `lower` with `upper`, pushing the frontier if that moved it.
    fn update(&self, lower: &[T], upper: &[T]) {
        let mut inner = self.inner.borrow_mut();
        let HoldsInner { holds, pusher } = &mut *inner;
        if adjust(holds, lower, upper)
            && let Some(pusher) = pusher
        {
            pusher.advance_to(holds.frontier());
        }
    }
}

/// The publication point a group of readers reads, chosen once after the readers exist.
///
/// Clones share the binding. Not `Send`: it lives on the thread of its readers.
pub struct Binding<T: Timestamp, B> {
    inner: Rc<RefCell<BindingInner<T, B>>>,
}

struct BindingInner<T: Timestamp, B> {
    point: Option<Arc<Shared<T, B>>>,
    /// Activated once at the bind.
    waiters: Vec<Activator>,
    /// The readers minted while unbound, registered on the point at the bind.
    unbound: Vec<Weak<RefCell<ReaderState<T, B>>>>,
    holds: HoldAccumulation<T>,
}

impl<T: Timestamp, B> Clone for Binding<T, B> {
    fn clone(&self) -> Self {
        Binding {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T, B> Binding<T, B>
where
    // A held reader cuts a totally ordered chain, see `SharedReader`.
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    /// An unbound binding whose readers register their logical holds in `holds`.
    pub fn new(holds: HoldAccumulation<T>) -> Self {
        Binding {
            inner: Rc::new(RefCell::new(BindingInner {
                point: None,
                waiters: Vec::new(),
                unbound: Vec::new(),
                holds,
            })),
        }
    }

    /// Binds `point`, registers the physical hold of every live reader on it, and activates every
    /// [`Self::on_bind`] waiter.
    ///
    /// # Panics
    ///
    /// Panics if the binding is already bound, or if the published `since` is beyond the logical
    /// hold of a live reader, which the hold stream rules out.
    pub fn bind(&self, point: Arc<Shared<T, B>>) {
        let (waiters, unbound) = {
            let mut inner = self.inner.borrow_mut();
            assert!(inner.point.is_none(), "a binding binds one point");
            inner.point = Some(Arc::clone(&point));
            (
                std::mem::take(&mut inner.waiters),
                std::mem::take(&mut inner.unbound),
            )
        };
        let unbound: Vec<_> = unbound.iter().filter_map(Weak::upgrade).collect();
        if !unbound.is_empty() {
            // One read of the point serves every reader, so all hold at the same coverage, which
            // is at or below anything their consumers read once `bind` returns. A consumer that
            // read the coverage before its reader registered could cut at a boundary the writer
            // merged away in between.
            let mut meet = Antichain::new();
            for state in &unbound {
                meet.extend(state.borrow().logical.iter().cloned());
            }
            let base = Self::mint(&point, &meet);
            for state in &unbound {
                let mut state = state.borrow_mut();
                // Only readers minted while unbound are tracked, and only the bind registers them.
                if let Physical::Unbound(request) = &state.physical {
                    let mut reader = base.clone();
                    // Joins, so the hold starts at the request joined with the chain coverage.
                    reader.set_physical_compaction(request.borrow());
                    state.physical = Physical::Held(reader);
                }
            }
        }
        for waiter in waiters {
            waiter.activate();
        }
    }

    /// Whether a point is bound.
    pub fn is_bound(&self) -> bool {
        self.inner.borrow().point.is_some()
    }

    /// Activates `activator` once a point is bound, at once if one already is.
    pub fn on_bind(&self, activator: Activator) {
        let mut inner = self.inner.borrow_mut();
        if inner.point.is_some() {
            drop(inner);
            activator.activate();
        } else {
            inner.waiters.push(activator);
        }
    }

    /// A reader holding logical compaction at `logical` in this binding's [`HoldAccumulation`],
    /// and physical compaction at the chain's coverage from the moment the binding is bound.
    ///
    /// NOTE: A reader that never moves its physical request pins merging at that coverage until
    /// it is released or dropped. A holder that reads nothing, and only needs its logical hold,
    /// must call `set_physical_compaction(&[])`.
    ///
    /// # Panics
    ///
    /// Panics if the binding is bound and the published `since` is beyond `logical`.
    pub fn reader(&self, logical: Antichain<T>) -> BindingReader<T, B> {
        self.holds().update(&[], &logical);
        let physical = match self.point() {
            Some(point) => Physical::Held(Self::mint(&point, &logical)),
            None => Physical::Unbound(Antichain::from_elem(T::minimum())),
        };
        self.track(ReaderState { logical, physical })
    }

    /// Wraps `state` in a reader, tracking it for the bind while unbound.
    fn track(&self, state: ReaderState<T, B>) -> BindingReader<T, B> {
        let unbound = matches!(state.physical, Physical::Unbound(_));
        let state = Rc::new(RefCell::new(state));
        if unbound {
            let mut inner = self.inner.borrow_mut();
            inner.unbound.retain(|weak| weak.strong_count() > 0);
            inner.unbound.push(Rc::downgrade(&state));
        }
        BindingReader {
            binding: self.clone(),
            state,
            view: Antichain::new(),
        }
    }

    /// A reader of `point` holding physical compaction at the chain coverage.
    fn mint(point: &Arc<Shared<T, B>>, logical: &Antichain<T>) -> SharedReader<T, B> {
        point.reader_physical_at(logical).expect(
            "the hold stream keeps the published since at or below every reader's logical hold",
        )
    }
}

impl<T: Timestamp, B> Binding<T, B> {
    fn point(&self) -> Option<Arc<Shared<T, B>>> {
        self.inner.borrow().point.clone()
    }

    fn holds(&self) -> HoldAccumulation<T> {
        self.inner.borrow().holds.clone()
    }
}

/// A reader's physical hold.
enum Physical<T: Timestamp, B> {
    /// The request while unbound, starting at the minimum. Registered on the point at the bind.
    Unbound(Antichain<T>),
    /// Registered on the bound point.
    Held(SharedReader<T, B>),
}

/// The holds of one [`BindingReader`], shared with its binding so the bind can register them.
struct ReaderState<T: Timestamp, B> {
    logical: Antichain<T>,
    physical: Physical<T, B>,
}

/// A reader of a [`Binding`]'s point, see the module docs.
///
/// Its logical hold is registered in the binding's [`HoldAccumulation`], never in the point. Each
/// clone registers an independent copy of both holds, and dropping a reader releases them.
///
/// Unbound, it reads as an empty chain at the minimum. Its physical hold starts at the minimum,
/// as a trace handle's does, and moves as requested. From the bind on it is registered on the
/// point, at the request joined with the chain's coverage at the bind.
pub struct BindingReader<T: Timestamp, B> {
    binding: Binding<T, B>,
    state: Rc<RefCell<ReaderState<T, B>>>,
    /// A copy of a hold, which the `TraceReader` getters lend out of the shared state.
    view: Antichain<T>,
}

impl<T, B> Clone for BindingReader<T, B>
where
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    fn clone(&self) -> Self {
        let state = self.state.borrow();
        self.binding.holds().update(&[], &state.logical);
        // A held reader's clone inherits its physical hold, never the current coverage, which can
        // be beyond a boundary the original's consumer already relies on.
        let physical = match &state.physical {
            Physical::Unbound(request) => Physical::Unbound(request.clone()),
            Physical::Held(reader) => Physical::Held(reader.clone()),
        };
        self.binding.track(ReaderState {
            logical: state.logical.clone(),
            physical,
        })
    }
}

impl<T: Timestamp, B> Drop for BindingReader<T, B> {
    fn drop(&mut self) {
        let state = self.state.borrow();
        self.binding.holds().update(&state.logical, &[]);
    }
}

impl<T, B> TraceReader for BindingReader<T, B>
where
    // `spans_through` relies on a totally ordered chain, see `SharedReader`.
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    type Time = T;
    type Batch = B;

    fn spans_through(&mut self, upper: AntichainRef<T>) -> Option<Vec<Span<T, B>>> {
        match self.binding.point() {
            Some(point) => Some(point.read(|published| chain_through(&published.items, upper))),
            None => {
                // An import of an unbound binding holds its capability at the minimum, so a
                // consumer that follows its stream never cuts past it.
                let minimum = Antichain::from_elem(T::minimum());
                assert!(
                    PartialOrder::less_equal(&upper, &minimum.borrow()),
                    "read through an unbound binding past the minimum"
                );
                Some(Vec::new())
            }
        }
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<T>) {
        let mut state = self.state.borrow_mut();
        // Join rather than overwrite, as `SharedReader` does.
        let next = state.logical.join(&frontier.to_owned());
        if next != state.logical {
            self.binding.holds().update(&state.logical, &next);
            state.logical = next;
        }
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.view.clone_from(&self.state.borrow().logical);
        self.view.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, T>) {
        match &mut self.state.borrow_mut().physical {
            Physical::Held(reader) => reader.set_physical_compaction(frontier),
            // Join, as `SharedReader` does. Once released, the request stays empty.
            Physical::Unbound(request) => *request = request.join(&frontier.to_owned()),
        }
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, T> {
        match &mut self.state.borrow_mut().physical {
            Physical::Held(reader) => self.view = reader.get_physical_compaction().to_owned(),
            Physical::Unbound(request) => self.view.clone_from(request),
        }
        self.view.borrow()
    }

    fn read_upper(&mut self, target: &mut Antichain<T>) {
        match self.binding.point() {
            Some(point) => point.read(|published| target.clone_from(&published.upper)),
            None => *target = Antichain::from_elem(T::minimum()),
        }
    }

    fn map_spans<F: FnMut(&Span<T, B>)>(&self, f: F) {
        // Iterates a clone, so `f` runs outside the point's lock and may touch the point.
        if let Some(point) = self.binding.point() {
            point.chain().iter().for_each(f);
        }
    }
}

impl<T, B> ImportHold<T> for BindingReader<T, B>
where
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    fn acknowledge(&mut self, frontier: AntichainRef<'_, T>) {
        self.set_logical_compaction(frontier);
        self.set_physical_compaction(frontier);
    }
}

impl<T, B> BindingReader<T, B>
where
    T: Timestamp + Lattice + TotalOrder,
    B: Clone + 'static,
{
    /// Imports the bound point restricted to `[as_of, until)`, presented at `as_of`, as
    /// [`SharedReader::import_frontier_core`] does.
    ///
    /// The import holds `as_of` logically in the accumulation from this call on, and physically
    /// in the point from the bind on, both following the frontiers it acknowledges. Until the
    /// binding binds, it produces nothing and holds its capability at the minimum. At its first
    /// activation after the bind it subscribes.
    ///
    /// # Panics
    ///
    /// Panics, here or at the bind, if the bound point's `since` is beyond `as_of`.
    pub fn import_frontier_core<'scope>(
        &self,
        scope: Scope<'scope, T>,
        name: &str,
        as_of: Antichain<T>,
        until: Antichain<T>,
    ) -> Arranged<'scope, TraceFrontier<BindingReader<T, B>>> {
        let trace = TraceFrontier::make_from(self.clone(), as_of.borrow(), until.borrow());
        let binding = self.binding.clone();
        let hold = binding.reader(as_of.clone());

        let stream = source(scope, name, move |capability, info| {
            binding.on_bind(scope.worker().activator_for(Rc::clone(&info.address)));
            let activator = scope.worker().sync_activator_for(info.address.to_vec());
            // Taken at the first activation after the bind. Until then the capability holds the
            // stream at the minimum.
            let mut unbound = Some((capability, activator, hold, as_of, until));
            let mut import = None;

            move |output| {
                if import.is_none() {
                    let Some(point) = binding.point() else {
                        return;
                    };
                    let (capability, activator, hold, as_of, until) = unbound
                        .take()
                        .expect("an unbound import keeps its capability");
                    // The hold registered at the chain coverage at the bind, or at construction
                    // when the binding was bound already. Both are at or below the seed.
                    import = Some(ImportDrain::subscribe(
                        &point, activator, capability, hold, as_of, until,
                    ));
                }
                if let Some(import) = import.as_mut() {
                    import.drain(|cap, span| output.session(cap).give(span));
                }
            }
        });

        Arranged { stream, trace }
    }
}
