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

use std::cell::RefCell;
use std::convert::Infallible;
use std::rc::Rc;

use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::{CapabilitySet, InspectCore};
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::Timestamp;
use timely::{Container, Data, PartialOrder};
use tokio::sync::Notify;

use crate::builder_async::OperatorBuilder as AsyncOperatorBuilder;

/// Monitors progress at a `Stream`.
pub trait ProbeNotify<G: Scope> {
    /// Inserts a collection of progress probe in a stream.
    fn probe_notify_with(&self, handles: Vec<Handle<G::Timestamp>>) -> Self;
}

impl<G, C> ProbeNotify<G> for StreamCore<G, C>
where
    G: Scope,
    C: Container + Data,
{
    fn probe_notify_with(&self, mut handles: Vec<Handle<G::Timestamp>>) -> Self {
        if handles.is_empty() {
            return self.clone();
        }
        // We need to reset the handle's frontier from the empty one to the minimal one, to enable
        // downgrading.
        for handle in &mut handles {
            handle.update_frontier(&[Timestamp::minimum()]);
        }

        // TODO: This operator observes but doesn't consume data.
        // Instead, it should only observe progress statements.
        self.inspect_container(move |update| {
            if let Err(frontier) = update {
                for handle in &mut handles {
                    handle.update_frontier(frontier);
                }
            }
        })
    }
}

#[derive(Debug)]
pub struct Handle<T: Timestamp> {
    /// The overall shared frontier managed by all the handles
    frontier: Rc<RefCell<MutableAntichain<T>>>,
    /// The private frontier containing the changes produced by this handle only
    handle_frontier: Antichain<T>,
    notify: Rc<Notify>,
}

impl<T: Timestamp> Default for Handle<T> {
    fn default() -> Self {
        // Initialize the handle frontier to the empty frontier, to prevent it from unintentionally
        // holding back the global frontier. Only when a handle is used to probe a stream do we
        // reset its frontier to the minimal one.
        Handle {
            frontier: Rc::new(RefCell::new(MutableAntichain::new())),
            handle_frontier: Antichain::new(),
            notify: Rc::new(Notify::new()),
        }
    }
}

impl<T: Timestamp> Handle<T> {
    /// Wait for the frontier monitored by this probe to progress
    pub async fn progressed(&self) {
        self.notify.notified().await
    }

    /// Returns true iff the frontier is strictly less than `time`.
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.frontier.borrow().less_than(time)
    }
    /// Returns true iff the frontier is less than or equal to `time`.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.frontier.borrow().less_equal(time)
    }
    /// Returns true iff the frontier is empty.
    #[inline]
    pub fn done(&self) -> bool {
        self.frontier.borrow().is_empty()
    }

    /// Invokes a method on the frontier, returning its result.
    ///
    /// This method allows inspection of the frontier, which cannot be returned by reference as
    /// it is on the other side of a `RefCell`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_timely_util::probe::Handle;
    ///
    /// let handle = Handle::<usize>::default();
    /// let frontier = handle.with_frontier(|frontier| frontier.to_vec());
    /// ```
    #[inline]
    pub fn with_frontier<R, F: FnMut(AntichainRef<T>) -> R>(&self, mut function: F) -> R {
        function(self.frontier.borrow().frontier())
    }

    #[inline]
    fn update_frontier(&mut self, new_frontier: &[T]) {
        let mut frontier = self.frontier.borrow_mut();
        let changes = frontier.update_iter(
            self.handle_frontier
                .iter()
                .map(|t| (t.clone(), -1))
                .chain(new_frontier.iter().map(|t| (t.clone(), 1))),
        );
        self.handle_frontier.clear();
        self.handle_frontier.extend(new_frontier.iter().cloned());
        if changes.count() > 0 {
            self.notify.notify_waiters();
        }
    }
}

impl<T: Timestamp> Drop for Handle<T> {
    fn drop(&mut self) {
        // This handle is being dropped so remove it from the overall calculation
        self.frontier
            .borrow_mut()
            .update_iter(self.handle_frontier.iter().map(|t| (t.clone(), -1)));
    }
}

impl<T: Timestamp> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Handle {
            frontier: Rc::clone(&self.frontier),
            handle_frontier: Antichain::new(),
            notify: Rc::clone(&self.notify),
        }
    }
}

/// Creates a stream that flows progress updates from a probe.
///
/// The returned stream is guaranteed to never yield any data updates, as is reflected by its type.
// TODO: Replace `Infallible` with `!` once the latter stabilizes.
pub fn source<G, T>(scope: G, name: String, handle: Handle<T>) -> Stream<G, Infallible>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    let mut builder = AsyncOperatorBuilder::new(name, scope);
    let (_output, output_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    builder.build(move |capabilities| async move {
        let mut cap_set = CapabilitySet::from(capabilities);
        let mut frontier = Antichain::from_elem(T::minimum());

        let mut downgrade_capability = |f: AntichainRef<T>| {
            if PartialOrder::less_than(&frontier.borrow(), &f) {
                frontier = f.to_owned();
                cap_set.downgrade(&f);
            }
            !frontier.is_empty()
        };

        while handle.with_frontier(&mut downgrade_capability) {
            handle.progressed().await;
        }
    });

    output_stream
}
