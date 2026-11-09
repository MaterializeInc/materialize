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

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use mz_ore::channel::{InstrumentedChannelMetric, InstrumentedUnboundedSender};
use timely::PartialOrder;
use timely::communication::Push;
use timely::dataflow::operators::capture::event::link_sync::EventLink;
use timely::dataflow::operators::capture::{Event, EventPusher};
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::activator::{ActivatorTrait, SharedSyncActivator};
use crate::replay::MzReplay;

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

/// Creates a link that carries a frontier from a pusher on any thread to a replay in a worker.
///
/// The replayed stream carries no data. Its frontier starts at `T::minimum()` and follows
/// [`ProgressPusher::advance_to`], so it equals a capture of an input that is advanced at the
/// time of each push. Both ends are `Send`.
pub fn progress_link<T: Timestamp>() -> (ProgressPusher<T>, ProgressSource<T>) {
    let head = Arc::new(EventLink::new());
    let activator = SharedSyncActivator::new();
    let pusher = ProgressPusher {
        link: Arc::clone(&head),
        frontier: Antichain::from_elem(T::minimum()),
        activator: activator.clone(),
    };
    let source = ProgressSource {
        link: head,
        activator,
    };
    (pusher, source)
}

/// The writing end of a [`progress_link`].
///
/// Dropping the pusher leaves the replayed frontier where the last [`ProgressPusher::advance_to`]
/// put it. Advance to the empty frontier to complete the replayed stream.
pub struct ProgressPusher<T: Timestamp> {
    link: Arc<EventLink<T, Vec<()>>>,
    /// The frontier last pushed to `link`.
    frontier: Antichain<T>,
    activator: SharedSyncActivator,
}

impl<T: Timestamp> ProgressPusher<T> {
    /// Advances the replayed frontier to `frontier` and wakes the replay operator.
    ///
    /// Does nothing if `frontier` equals the last pushed frontier. A `frontier` that is not
    /// beyond the last pushed one is a bug, which is logged and ignored.
    pub fn advance_to(&mut self, frontier: AntichainRef<'_, T>) {
        if frontier == self.frontier.borrow() {
            return;
        }
        if !PartialOrder::less_equal(&self.frontier.borrow(), &frontier) {
            mz_ore::soft_assert_or_log!(
                false,
                "progress link frontier regressed from {:?} to {:?}",
                self.frontier,
                frontier,
            );
            return;
        }
        let changes = frontier
            .iter()
            .map(|time| (time.clone(), 1))
            .chain(self.frontier.iter().map(|time| (time.clone(), -1)))
            .collect();
        self.link.push(Event::Progress(changes));
        self.frontier = frontier.to_owned();
        self.activator.activate();
    }
}

/// The reading end of a [`progress_link`].
pub struct ProgressSource<T: Timestamp> {
    link: Arc<EventLink<T, Vec<()>>>,
    activator: SharedSyncActivator,
}

impl<T: Timestamp> ProgressSource<T> {
    /// Replays the link's frontier into `scope`.
    ///
    /// Dropping the returned token shuts the replay operator down.
    pub fn replay<'s>(
        self,
        scope: Scope<'s, T>,
        name: &str,
    ) -> (Stream<'s, T, Vec<()>>, Rc<dyn Any>) {
        Some(self.link).mz_replay(scope, name, Duration::MAX, self.activator)
    }
}

#[cfg(test)]
mod tests;
