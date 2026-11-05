// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Push-based collection of frontier changes.
//!
//! A worker reports the frontiers of many objects to its controller. Instead of reading every
//! frontier periodically, the sources of frontiers push each change into a [`FrontierQueue`], and
//! the reporter drains it. A source is either a [`probe::Handle`](crate::probe::Handle), which
//! timely schedules when its input frontier changes, or a [`WatchedFrontier`], which an operator
//! sets explicitly.
//!
//! All types here are single-threaded. Pushes happen while timely runs operators, and the drain
//! happens between steps, so a frontier change is in the queue before the worker next decides
//! whether to park.

use std::cell::{Ref, RefCell};
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use timely::PartialOrder;
use timely::progress::frontier::{Antichain, AntichainRef};

/// A callback receiving a frontier after it changed.
pub type FrontierObserver<T> = Rc<dyn Fn(AntichainRef<T>)>;

/// Changed frontiers, keyed by what they belong to.
///
/// Pushing a frontier for a key that is already queued replaces the queued frontier, so a drain
/// sees the latest frontier of each key once.
pub struct FrontierQueue<K, T> {
    pending: RefCell<BTreeMap<K, Antichain<T>>>,
}

impl<K, T> Default for FrontierQueue<K, T> {
    fn default() -> Self {
        Self {
            pending: RefCell::new(BTreeMap::new()),
        }
    }
}

impl<K: fmt::Debug, T: fmt::Debug> fmt::Debug for FrontierQueue<K, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrontierQueue")
            .field("pending", &self.pending.borrow())
            .finish()
    }
}

impl<K: Ord, T: PartialOrder + Clone> FrontierQueue<K, T> {
    /// Records `frontier` as the latest frontier of `key`.
    pub fn push(&self, key: K, frontier: AntichainRef<T>) {
        let mut pending = self.pending.borrow_mut();
        match pending.get_mut(&key) {
            Some(queued) => {
                queued.clear();
                queued.extend(frontier.iter().cloned());
            }
            None => {
                pending.insert(key, frontier.to_owned());
            }
        }
    }

    /// Discards the queued frontiers of keys for which `keep` returns false.
    pub fn retain(&self, mut keep: impl FnMut(&K) -> bool) {
        self.pending.borrow_mut().retain(|key, _| keep(key));
    }

    /// Returns whether no frontier changed since the last drain.
    pub fn is_empty(&self) -> bool {
        self.pending.borrow().is_empty()
    }

    /// Takes all queued frontiers.
    pub fn drain(&self) -> BTreeMap<K, Antichain<T>> {
        std::mem::take(&mut *self.pending.borrow_mut())
    }
}

/// A frontier that an operator maintains and that notifies an observer when it changes.
///
/// This replaces an `Rc<RefCell<Antichain<T>>>` shared between an operator, which writes, and a
/// reporter, which would otherwise have to poll it.
pub struct WatchedFrontier<T> {
    frontier: RefCell<Antichain<T>>,
    observer: RefCell<Option<FrontierObserver<T>>>,
}

impl<T: fmt::Debug> fmt::Debug for WatchedFrontier<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchedFrontier")
            .field("frontier", &self.frontier.borrow())
            .field("observed", &self.observer.borrow().is_some())
            .finish()
    }
}

impl<T: PartialOrder + Clone> WatchedFrontier<T> {
    /// Creates a frontier without an observer.
    pub fn new(frontier: Antichain<T>) -> Self {
        Self {
            frontier: RefCell::new(frontier),
            observer: RefCell::new(None),
        }
    }

    /// Borrows the current frontier.
    pub fn borrow(&self) -> Ref<'_, Antichain<T>> {
        self.frontier.borrow()
    }

    /// Sets the frontier, notifying the observer if it changed.
    pub fn set(&self, frontier: AntichainRef<T>) {
        {
            let mut current = self.frontier.borrow_mut();
            if current.borrow() == frontier {
                return;
            }
            current.clear();
            current.extend(frontier.iter().cloned());
        }
        self.notify();
    }

    /// Sets the frontier to the empty frontier, notifying the observer if it changed.
    pub fn clear(&self) {
        self.set(Antichain::new().borrow());
    }

    /// Replaces the observer, and notifies the new observer of the current frontier.
    ///
    /// The immediate notification covers changes made before the observer was installed.
    pub fn set_observer(&self, observer: FrontierObserver<T>) {
        *self.observer.borrow_mut() = Some(observer);
        self.notify();
    }

    fn notify(&self) {
        // Clone the observer, so that it may replace itself or borrow the frontier.
        let observer = self.observer.borrow().clone();
        if let Some(observer) = observer {
            observer(self.frontier.borrow().borrow());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[mz_ore::test]
    fn queue_keeps_latest_frontier_per_key() {
        let queue = FrontierQueue::<u64, u64>::default();
        queue.push(1, Antichain::from_elem(3).borrow());
        queue.push(2, Antichain::from_elem(4).borrow());
        queue.push(1, Antichain::from_elem(5).borrow());
        let drained = queue.drain();
        assert_eq!(
            drained.into_iter().collect::<Vec<_>>(),
            vec![(1, Antichain::from_elem(5)), (2, Antichain::from_elem(4))],
        );
        assert!(queue.is_empty());
    }

    #[mz_ore::test]
    fn watched_frontier_notifies_on_change_only() {
        let calls = Rc::new(Cell::new(0));
        let frontier = WatchedFrontier::new(Antichain::from_elem(0u64));
        frontier.set(Antichain::from_elem(1).borrow());

        let seen = Rc::clone(&calls);
        frontier.set_observer(Rc::new(move |f| {
            assert_eq!(f, Antichain::from_elem(1).borrow());
            seen.set(seen.get() + 1);
        }));
        assert_eq!(
            calls.get(),
            1,
            "installing the observer reports the current frontier"
        );

        frontier.set(Antichain::from_elem(1).borrow());
        assert_eq!(calls.get(), 1, "an unchanged frontier does not notify");
    }
}
