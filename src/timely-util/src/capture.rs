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

use std::sync::Arc;

use mz_ore::channel::{InstrumentedChannelMetric, InstrumentedUnboundedSender};
use timely::communication::Push;
use timely::dataflow::operators::capture::{Event, EventPusher};

/// A thread-safe linked list for cross-thread event streaming.
///
/// Uses `Arc`/`Mutex` internally, unlike the `Rc`/`RefCell`-based `EventLink`
/// in `timely::capture`. Designed to be shared via `Arc<EventLink<T, C>>`:
/// the writer side implements [`EventPusher`] and the reader side implements
/// [`EventIterator`](timely::dataflow::operators::capture::event::EventIterator).
pub use timely::dataflow::operators::capture::event::link_sync::EventLink;

/// Creates a linked `(writer, reader)` pair for cross-thread event streaming.
///
/// Both handles begin pointing at the same empty sentinel node. The writer
/// appends events via [`EventPusher`]; the reader chases the list and yields
/// them via [`EventIterator`](timely::dataflow::operators::capture::event::EventIterator).
pub fn arc_event_link<T, C>() -> (Arc<EventLink<T, C>>, Arc<EventLink<T, C>>) {
    let shared = Arc::new(EventLink::new());
    (Arc::clone(&shared), shared)
}

pub struct UnboundedTokioCapture<T, C, M>(pub InstrumentedUnboundedSender<Event<T, C>, M>);

impl<T, C, M> EventPusher<T, C> for UnboundedTokioCapture<T, C, M>
where
    M: InstrumentedChannelMetric,
{
    fn push(&mut self, event: Event<T, C>) {
        // NOTE: An Err(x) result just means "data not accepted" most likely
        //       because the receiver is gone. No need to panic.
        let _ = self.0.send(event);
    }
}

/// A helper type to allow capturing timely streams into timely pushers
pub struct PusherCapture<P>(pub P);

impl<P: Push<Event<T, D>>, T, D> EventPusher<T, D> for PusherCapture<P> {
    fn push(&mut self, event: Event<T, D>) {
        self.0.send(event);
        self.0.done();
    }
}
