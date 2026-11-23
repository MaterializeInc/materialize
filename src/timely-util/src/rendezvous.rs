// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A meeting point between one writer thread and readers on any thread.
//!
//! A [`Rendezvous`] carries a view a writer publishes, [`Published`]: a list of items, the `upper`
//! they cover, and writer-defined metadata. Next to it sit `N` hold accumulations, one per hold
//! kind, into which every [`Reader`] registers a frontier per kind. The writer reads the meet of
//! each kind to decide what it may discard, and a reader that moves a meet wakes the writer.
//! Subscribers receive the view as a seed and then every item the writer reports as arrived.
//!
//! A writer [attaches](Rendezvous::attach) to a point and publishes through the returned
//! [`Attachment`]. A writer may attach to several points, but a point accepts one writer in its
//! life. Once that writer closes or drops, the view is final and the point is
//! [frozen](Rendezvous::is_frozen): its readers keep the view as last published, and a later
//! writer needs a point of its own. The writer keeps adjusting holds after it closes, so the holds
//! it registered still release.
//!
//! Everything a point owns sits behind one lock. Critical sections are frontier arithmetic, vector
//! swaps, the seed a new subscription clones, which must be taken under the lock that registers
//! the subscription for the seed and the deltas to leave no gap, and the closures callers pass to
//! [`Rendezvous::read`], [`Rendezvous::reader_with`] and the metadata updates. The items publishing
//! pushes to queues are cloned, and the items it replaces dropped, outside the lock.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use itertools::Itertools;
use timely::PartialOrder;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::activate::SyncActivator;

/// The view a writer publishes on a [`Rendezvous`].
pub struct Published<T, I, M> {
    /// The writer's latest list of items, replaced wholesale on every publish.
    pub items: Vec<I>,
    /// The frontier `items` covers. Starts at the minimum time, and only the writer moves it.
    pub upper: Antichain<T>,
    /// Writer-defined metadata, published together with `items`.
    pub meta: M,
}

/// One entry of a [`Subscription`]'s queue.
pub enum Event<T, I> {
    /// An item, in the order the writer published it.
    Item(I),
    /// The `upper` the view had once every preceding item was in it.
    Upper(Antichain<T>),
}

/// The queue and wakeup for one subscriber. Its own lock, so a subscriber draining it never
/// contends with the writer publishing.
struct Queue<T, I> {
    events: Mutex<VecDeque<Event<T, I>>>,
    activator: SyncActivator,
}

impl<T, I> Queue<T, I> {
    fn push(&self, events: impl IntoIterator<Item = Event<T, I>>) {
        let mut queue = self.events.lock().expect("subscription queue poisoned");
        queue.extend(events);
    }

    fn activate(&self) {
        let _ = self.activator.activate();
    }
}

struct State<T, I, M, const N: usize> {
    published: Published<T, I, M>,
    /// The holds of every reader, plus whatever the writer registered, per kind.
    holds: [MutableAntichain<T>; N],
    /// Held weakly: the subscriber owns the only strong reference, and its entry is pruned once
    /// it drops.
    queues: Vec<Weak<Queue<T, I>>>,
    /// Wakes the writer.
    writer_activator: Option<SyncActivator>,
    /// Where the point is in its one writer's life.
    writer: WriterState,
}

/// The life of a point's one writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterState {
    /// No writer attached yet.
    Unattached,
    /// The writer attached and has neither closed nor dropped.
    Attached,
    /// The writer closed or dropped. The view is final.
    Frozen,
}

impl<T, I, M, const N: usize> State<T, I, M, N> {
    /// The live subscriber queues, dropping entries whose subscriber has gone. Returns strong
    /// references so the caller can push and activate after releasing the lock.
    fn live_queues(&mut self) -> Vec<Arc<Queue<T, I>>> {
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

    fn wake_writer(&self) {
        if let Some(activator) = &self.writer_activator {
            let _ = activator.activate();
        }
    }
}

/// A point one writer publishes a view to and readers on any thread hold.
///
/// Created unattached, with no items, `upper` at the minimum time, and the given metadata. Readers
/// and subscribers may register before a writer attaches. Subscribers are seeded from the writer's
/// view at attachment.
pub struct Rendezvous<T, I, M, const N: usize> {
    state: Mutex<State<T, I, M, N>>,
    /// Whether a reader moved a meet since the writer last took the flag. Outside the lock, so the
    /// writer can skip reading the holds without taking it.
    holds_moved: AtomicBool,
}

impl<T, I, M, const N: usize> Rendezvous<T, I, M, N> {
    fn lock(&self) -> MutexGuard<'_, State<T, I, M, N>> {
        self.state.lock().expect("rendezvous poisoned")
    }

    /// Calls `f` on the published view, under the point's lock.
    pub fn read<R>(&self, f: impl FnOnce(&Published<T, I, M>) -> R) -> R {
        f(&self.lock().published)
    }
}

impl<T: Timestamp, I, M, const N: usize> Rendezvous<T, I, M, N> {
    /// An unattached point publishing `meta`.
    pub fn with_meta(meta: M) -> Self {
        Rendezvous {
            state: Mutex::new(State {
                published: Published {
                    items: Vec::new(),
                    upper: Antichain::from_elem(T::minimum()),
                    meta,
                },
                holds: std::array::from_fn(|_| MutableAntichain::new()),
                queues: Vec::new(),
                writer_activator: None,
                writer: WriterState::Unattached,
            }),
            holds_moved: AtomicBool::new(false),
        }
    }

    /// Whether this point's writer attached and has since closed or dropped. The view of a frozen
    /// point never changes again, and the point refuses another writer.
    pub fn is_frozen(&self) -> bool {
        self.lock().writer == WriterState::Frozen
    }

    /// The meet of every hold of `kind` on this point. Empty when nothing holds.
    pub fn holds(&self, kind: usize) -> Antichain<T> {
        self.lock().holds[kind].frontier().to_owned()
    }

    /// A reader holding the frontiers `holds` computes from the published view, or the error it
    /// returns.
    ///
    /// Reads the view and registers the holds under one lock acquisition, so the writer cannot
    /// publish between the two.
    pub fn reader_with<E>(
        self: &Arc<Self>,
        holds: impl FnOnce(&Published<T, I, M>) -> Result<[Antichain<T>; N], E>,
    ) -> Result<Reader<T, I, M, N>, E> {
        let mut state = self.lock();
        let holds = holds(&state.published)?;
        for (accumulated, hold) in state.holds.iter_mut().zip_eq(holds.iter()) {
            adjust(accumulated, &[], hold);
        }
        drop(state);
        Ok(Reader {
            point: Arc::clone(self),
            holds,
        })
    }
}

impl<T: Timestamp, I: Clone, M, const N: usize> Rendezvous<T, I, M, N> {
    /// A subscription seeded with the published items and `upper`, and the seeded `upper`.
    ///
    /// Seeding and registering happen under one lock acquisition. An item the writer publishes as
    /// arrived is therefore either in the seed or delivered afterwards, never both and never
    /// neither.
    pub fn subscribe(&self, activator: SyncActivator) -> (Subscription<T, I>, Antichain<T>) {
        let mut state = self.lock();
        let mut events = VecDeque::new();
        events.extend(state.published.items.iter().cloned().map(Event::Item));
        let seed = state.published.upper.clone();
        events.push_back(Event::Upper(seed.clone()));
        let queue = Arc::new(Queue {
            events: Mutex::new(events),
            activator,
        });
        state.queues.push(Arc::downgrade(&queue));
        (Subscription { queue }, seed)
    }

    /// Attaches the point's writer and publishes its view.
    ///
    /// Replaces the items and `upper`, applies `meta` to the metadata, adds `holds` to the
    /// accumulations, and installs the writer wakeup `activator`. `activator` must schedule the
    /// code that reads the holds: a reader moving its hold from another thread fires it. Live
    /// subscribers, all registered before any writer attached, are seeded with the view.
    ///
    /// # Panics
    ///
    /// Panics if a writer attached to this point before, even one that has since dropped. Its
    /// subscribers acknowledged its `upper`, and a second writer's view can regress below it,
    /// which a subscriber cannot follow.
    pub fn attach(
        self: &Arc<Self>,
        activator: SyncActivator,
        items: Vec<I>,
        upper: Antichain<T>,
        meta: impl FnOnce(&mut M),
        holds: [Vec<(T, i64)>; N],
    ) -> Attachment<T, I, M, N> {
        let mut next = items.clone();
        let live = {
            let mut state = self.lock();
            if state.writer != WriterState::Unattached {
                // Released first, so the panic does not poison the point for its readers.
                drop(state);
                panic!("rendezvous: attach to a point that already had a writer");
            }
            state.writer = WriterState::Attached;
            state.writer_activator = Some(activator);
            std::mem::swap(&mut state.published.items, &mut next);
            state.published.upper = upper.clone();
            meta(&mut state.published.meta);
            for (accumulated, updates) in state.holds.iter_mut().zip_eq(holds) {
                accumulated.update_iter(updates);
            }
            state.live_queues()
        };
        drop(next);
        for queue in live {
            queue.push(
                items
                    .iter()
                    .cloned()
                    .map(Event::Item)
                    .chain(std::iter::once(Event::Upper(upper.clone()))),
            );
            queue.activate();
        }
        Attachment {
            point: Arc::clone(self),
            closed: false,
        }
    }
}

/// Replaces the elements of `lower` with those of `upper` in `accumulated`, and returns whether
/// that moved the frontier.
///
/// The writer reads the holds only when it runs, so a change that leaves the frontier in place
/// gives it nothing to do and needs no wake.
pub(crate) fn adjust<T: Timestamp>(
    accumulated: &mut MutableAntichain<T>,
    lower: &[T],
    upper: &[T],
) -> bool {
    // The drained changes are consolidated, so they are empty exactly when the frontier is.
    let updates = upper
        .iter()
        .cloned()
        .map(|time| (time, 1))
        .chain(lower.iter().cloned().map(|time| (time, -1)));
    accumulated.update_iter(updates).next().is_some()
}

/// A writer's attachment to one [`Rendezvous`].
pub struct Attachment<T, I, M, const N: usize> {
    point: Arc<Rendezvous<T, I, M, N>>,
    /// Whether the writer has closed. See [`Attachment::close`].
    closed: bool,
}

impl<T, I, M, const N: usize> Attachment<T, I, M, N> {
    /// The point this attachment publishes to.
    pub fn point(&self) -> &Arc<Rendezvous<T, I, M, N>> {
        &self.point
    }

    /// Whether anything besides this attachment references the point. Minting a reader or a
    /// subscription needs a reference, so an unreachable point cannot gain one.
    ///
    /// A [`Subscription`] does not reference the point. A writer that stops publishing to an
    /// unreachable point starves a subscriber that holds no [`Reader`] of it.
    pub fn reachable(&self) -> bool {
        Arc::strong_count(&self.point) > 1
    }

    /// Whether a reader moved a meet of this point since the last call, which resets it.
    ///
    /// A reader sets the flag after it moved the meet and before it wakes the writer, so a writer
    /// that reads `false` and skips the holds is woken again for any later move.
    pub fn take_holds_moved(&self) -> bool {
        self.point.holds_moved.swap(false, Ordering::AcqRel)
    }

    /// Whether [`Attachment::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Stops this attachment publishing, leaving the point's items, `upper` and metadata as last
    /// published, and freezes the point. Hold changes still apply.
    ///
    /// NOTE: A closing writer must not publish an empty `upper`. A dropped writer says nothing
    /// about the times past its last `upper`, and advancing readers to the empty frontier lets
    /// their dataflows complete work on times that never happened. `drop_dataflow` reclaims a
    /// dataflow without letting it finish its computation, so a writer's drop is no evidence that
    /// its collection is complete. Dropping an attachment without closing it leaves the view as
    /// last published too.
    pub fn close(&mut self) {
        self.closed = true;
        self.point.lock().writer = WriterState::Frozen;
    }
}

impl<T: Timestamp, I, M, const N: usize> Attachment<T, I, M, N> {
    /// The point's accumulated holds, as updates per kind, consuming the attachment.
    ///
    /// For a writer that registered holds on a point it alone references: no reader can hold the
    /// point then, so every hold on it is one the writer registered.
    pub fn into_holds(self) -> [Vec<(T, i64)>; N] {
        // Another reference could mint a reader, whose holds this would hand to the writer.
        mz_ore::soft_assert_or_log!(
            Arc::strong_count(&self.point) == 1,
            "into_holds: the point is referenced elsewhere"
        );
        let mut state = self.point.lock();
        std::array::from_fn(|kind| state.holds[kind].updates().cloned().collect())
    }

    /// Locks the points of `attachments` together, in address order.
    ///
    /// Every point has one writer, so no other thread locks more than one of these points.
    ///
    /// Panics if two attachments name the same point, which would deadlock instead.
    pub fn lock_all<'a>(attachments: &[&'a Self]) -> Locked<'a, T, I, M, N> {
        let mut order: Vec<usize> = (0..attachments.len()).collect();
        order.sort_unstable_by_key(|&index| Arc::as_ptr(&attachments[index].point));
        assert!(
            order
                .windows(2)
                .all(|pair| !Arc::ptr_eq(&attachments[pair[0]].point, &attachments[pair[1]].point)),
            "lock_all: a point is named twice"
        );
        let mut slots: Vec<Option<Guard<'a, T, I, M, N>>> =
            (0..attachments.len()).map(|_| None).collect();
        for index in order {
            let attachment = attachments[index];
            let state = attachment.point.lock();
            slots[index] = Some(Guard {
                publishing: !attachment.closed,
                state,
            });
        }
        Locked {
            guards: slots
                .into_iter()
                .map(|slot| slot.expect("every point is locked"))
                .collect(),
        }
    }
}

impl<T: Timestamp, I: Clone, M, const N: usize> Attachment<T, I, M, N> {
    /// Publishes a view, unless this attachment is closed. Returns whether it published and that
    /// moved `upper`.
    ///
    /// Replaces the items and `upper` and applies `meta` to the metadata. When `arrived` is given,
    /// it is an item `items` contains that no earlier publish did, and every live subscriber
    /// receives it followed by `upper`. Without it, a moved `upper` reaches every live subscriber
    /// on its own. `upper` must not regress below the published one.
    pub fn publish(
        &mut self,
        items: Vec<I>,
        upper: Antichain<T>,
        meta: impl FnOnce(&mut M),
        arrived: Option<&I>,
    ) -> bool {
        if self.closed {
            return false;
        }
        let mut next = items;
        let (advanced, live, regressed_from) = {
            let mut state = self.point.lock();
            let regressed_from = (!PartialOrder::less_equal(&state.published.upper, &upper))
                .then(|| state.published.upper.clone());
            std::mem::swap(&mut state.published.items, &mut next);
            let advanced = state.published.upper != upper;
            state.published.upper = upper.clone();
            meta(&mut state.published.meta);
            let live = if arrived.is_some() || advanced {
                state.live_queues()
            } else {
                Vec::new()
            };
            (advanced, live, regressed_from)
        };
        // A subscriber that acknowledged the published `upper` cannot follow a regression. Checked
        // outside the lock, so the panic does not poison the point for its readers.
        mz_ore::soft_assert_or_log!(
            regressed_from.is_none(),
            "rendezvous: publish regresses upper from {regressed_from:?} to {upper:?}",
        );
        // The previous items drop here, outside the lock.
        drop(next);
        for queue in &live {
            match arrived {
                Some(item) => queue.push([Event::Item(item.clone()), Event::Upper(upper.clone())]),
                None => queue.push([Event::Upper(upper.clone())]),
            }
            queue.activate();
        }
        advanced
    }
}

impl<T, I, M, const N: usize> Drop for Attachment<T, I, M, N> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.point.state.lock() {
            state.writer_activator = None;
            state.writer = WriterState::Frozen;
        }
    }
}

/// The points of several attachments, locked together by [`Attachment::lock_all`].
///
/// Indices are positions in the slice passed to `lock_all`.
pub struct Locked<'a, T, I, M, const N: usize> {
    guards: Vec<Guard<'a, T, I, M, N>>,
}

/// One point locked by [`Attachment::lock_all`].
struct Guard<'a, T, I, M, const N: usize> {
    /// Whether the attachment may change the published view, which it may until it closes.
    publishing: bool,
    state: MutexGuard<'a, State<T, I, M, N>>,
}

impl<'a, T: Timestamp, I, M, const N: usize> Locked<'a, T, I, M, N> {
    /// Replaces `lower` with `upper` in the holds of `kind` on the point at `index`, and returns
    /// whether that moved the meet. Applies whether or not the attachment has closed.
    ///
    /// A moved meet wakes nobody: the caller is the point's writer, and reads the meet under these
    /// locks.
    pub fn adjust(&mut self, index: usize, kind: usize, lower: &[T], upper: &[T]) -> bool {
        adjust(&mut self.guards[index].state.holds[kind], lower, upper)
    }

    /// The meet of the holds of `kind` across every locked point.
    pub fn meet(&self, kind: usize) -> Antichain<T> {
        let mut meet = Antichain::new();
        for guard in &self.guards {
            for time in guard.state.holds[kind].frontier().iter() {
                meet.insert_ref(time);
            }
        }
        meet
    }

    /// Applies `f` to the metadata of every point where the attachment has not closed.
    pub fn update_open(&mut self, mut f: impl FnMut(&mut M)) {
        for guard in &mut self.guards {
            if guard.publishing {
                f(&mut guard.state.published.meta);
            }
        }
    }
}

/// A `Send` handle holding one frontier per hold kind on a [`Rendezvous`].
///
/// Each clone registers its own holds, so two owners cannot release each other's. Dropping a
/// reader releases its holds and wakes the writer if that moved a meet.
pub struct Reader<T: Timestamp, I, M, const N: usize> {
    point: Arc<Rendezvous<T, I, M, N>>,
    /// Kept locally so changes adjust the accumulation by a delta, and so `hold` can borrow.
    holds: [Antichain<T>; N],
}

impl<T: Timestamp, I, M, const N: usize> Reader<T, I, M, N> {
    /// The point this reader holds.
    pub fn point(&self) -> &Arc<Rendezvous<T, I, M, N>> {
        &self.point
    }

    /// This reader's hold of `kind`.
    pub fn hold(&self, kind: usize) -> AntichainRef<'_, T> {
        self.holds[kind].borrow()
    }

    /// Replaces this reader's hold of `kind` with `frontier`, waking the writer if that moved the
    /// meet.
    ///
    /// A hold below one the writer already applied brings back nothing the writer discarded.
    pub fn set_hold(&mut self, kind: usize, frontier: Antichain<T>) {
        let previous = std::mem::replace(&mut self.holds[kind], frontier);
        let mut state = self.point.lock();
        if adjust(&mut state.holds[kind], &previous, &self.holds[kind]) {
            self.point.holds_moved.store(true, Ordering::Release);
            state.wake_writer();
        }
    }
}

impl<T: Timestamp, I, M, const N: usize> Clone for Reader<T, I, M, N> {
    fn clone(&self) -> Self {
        {
            let mut state = self.point.lock();
            for (accumulated, hold) in state.holds.iter_mut().zip_eq(self.holds.iter()) {
                adjust(accumulated, &[], hold);
            }
        }
        Reader {
            point: Arc::clone(&self.point),
            holds: self.holds.clone(),
        }
    }
}

impl<T: Timestamp, I, M, const N: usize> Drop for Reader<T, I, M, N> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.point.state.lock() {
            let mut moved = false;
            for (accumulated, hold) in state.holds.iter_mut().zip_eq(self.holds.iter()) {
                moved |= adjust(accumulated, hold, &[]);
            }
            if moved {
                self.point.holds_moved.store(true, Ordering::Release);
                state.wake_writer();
            }
        }
    }
}

/// A subscriber's queue on a [`Rendezvous`], from [`Rendezvous::subscribe`].
///
/// Dropping it unregisters the subscriber. It does not keep the point reachable, see
/// [`Attachment::reachable`].
pub struct Subscription<T, I> {
    queue: Arc<Queue<T, I>>,
}

impl<T, I> Subscription<T, I> {
    /// Takes every event queued so far, in order. Takes only the subscription's own lock, never
    /// the point's.
    pub fn drain(&self) -> Vec<Event<T, I>> {
        let mut events = self
            .queue
            .events
            .lock()
            .expect("subscription queue poisoned");
        events.drain(..).collect()
    }
}

#[cfg(test)]
mod tests;
