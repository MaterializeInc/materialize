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

//! Parallelization contracts, describing requirements for data movement along dataflow edges.

use std::rc::Rc;
use timely::communication::{Pull, Push};
use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
use timely::dataflow::channels::{ContainerBytes, Message};
use timely::logging::TimelyLogger;
use timely::progress::Timestamp;
use timely::worker::AsWorker;
use timely::{Container, ExchangeData};

/// A connection that distributes containers to all workers in a round-robin fashion
#[derive(Debug)]
pub struct Distribute;

impl<T, C> ParallelizationContract<T, C> for Distribute
where
    T: Timestamp,
    C: Container + ContainerBytes + Send + 'static,
{
    type Pusher = DistributePusher<LogPusher<Box<dyn Push<Message<T, C>>>>>;
    type Puller = LogPuller<Box<dyn Pull<Message<T, C>>>>;

    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: Rc<[usize]>,
        logging: Option<TimelyLogger>,
    ) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, C>>(identifier, address);
        let senders = senders
            .into_iter()
            .enumerate()
            .map(|(i, x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone()))
            .collect::<Vec<_>>();
        (
            DistributePusher::new(senders),
            LogPuller::new(receiver, allocator.index(), identifier, logging.clone()),
        )
    }
}

/// Distributes records among target pushees.
///
/// It is more efficient than `Exchange` when the target worker doesn't matter
pub struct DistributePusher<P> {
    pushers: Vec<P>,
    next: usize,
}

impl<P> DistributePusher<P> {
    /// Allocates a new `DistributePusher` from a supplied set of pushers
    pub fn new(pushers: Vec<P>) -> DistributePusher<P> {
        Self { pushers, next: 0 }
    }
}

impl<T, C, P> Push<Message<T, C>> for DistributePusher<P>
where
    T: Eq + ExchangeData,
    C: Container,
    P: Push<Message<T, C>>,
{
    fn push(&mut self, message: &mut Option<Message<T, C>>) {
        let worker_idx = self.next;
        self.next = (self.next + 1) % self.pushers.len();
        self.pushers[worker_idx].push(message);
    }
}
