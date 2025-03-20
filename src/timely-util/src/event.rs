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

//! Traits and types for describing captured timely dataflow streams.
//!
//! This is roughly based on [timely::dataflow::operators::capture::event].

use timely::dataflow::operators::capture::{Event, EventPusher};

use crate::activator::RcActivator;

/// An event pusher wrapper that activates targets on push.
#[derive(Clone, Debug)]
pub struct ActivatedEventPusher<E> {
    /// Inner event pusher.
    pub inner: E,
    /// Activator used to prompt receipt of event.
    pub activator: RcActivator,
}

impl<E> ActivatedEventPusher<E> {
    /// Create a new activated event link wrapper.
    ///
    /// * inner: A wrapped event pusher/iterator.
    pub fn new(inner: E, activator: RcActivator) -> Self {
        Self { inner, activator }
    }
}

impl<T, D, E: EventPusher<T, D>> EventPusher<T, D> for ActivatedEventPusher<E> {
    fn push(&mut self, event: Event<T, D>) {
        self.inner.push(event);
        self.activator.activate();
    }
}
