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

use mz_ore::channel::{InstrumentedChannelMetric, InstrumentedUnboundedSender};
use timely::communication::Push;
use timely::dataflow::operators::capture::{Event, EventPusher};

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
