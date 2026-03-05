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

use mz_ore::cast::CastFrom;
use std::rc::Rc;
use timely::communication::{Pull, Push};
use timely::container::{CapacityContainerBuilder, DrainContainer, PushInto, SizableContainer};
use timely::dataflow::channels::pact::{
    DistributorPact, LogPuller, LogPusher, ParallelizationContract,
};
use timely::dataflow::channels::pushers::exchange::Distributor;
use timely::dataflow::channels::{ContainerBytes, Message};
use timely::logging::TimelyLogger;
use timely::progress::Timestamp;
use timely::worker::AsWorker;
use timely::{Container, ContainerBuilder, ExchangeData};

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

/// A pact that distributes produced containers in chunks to each target worker
/// in round-robin order. The `seed` determines the starting target worker.
pub type ChunkedExchange<CB> =
    DistributorPact<Box<dyn FnOnce(usize) -> ChunkedRoundRobinDistributor<CB>>>;

/// Creates a [`ChunkedExchange`] pact using [`CapacityContainerBuilder`].
///
/// Each sender ships `chunk_size` full containers to one target worker before
/// rotating to the next. The `seed` determines the initial target worker, so
/// that different sinks begin at different targets.
pub fn chunked_exchange<C>(
    chunk_size: usize,
    seed: u64,
) -> ChunkedExchange<CapacityContainerBuilder<C>>
where
    C: Container + SizableContainer + DrainContainer,
    CapacityContainerBuilder<C>:
        ContainerBuilder<Container = C> + for<'a> PushInto<<C as DrainContainer>::Item<'a>>,
{
    DistributorPact(Box::new(move |peers| {
        ChunkedRoundRobinDistributor::new(chunk_size, seed, peers)
    }))
}

/// Distributes produced containers in chunks of `chunk_size` to target workers
/// in round-robin order.
///
/// Items are pushed into a single [`ContainerBuilder`]. Each time the builder
/// produces a full container, it is shipped to the current target and a counter
/// increments. After `chunk_size` containers have been shipped to the current
/// target, the target rotates to the next peer.
///
/// On [`flush`](Distributor::flush), the builder is finished and any remaining
/// container is shipped to the current target. The target then resets to
/// `initial_target` to ensure all workers make consistent routing decisions,
/// since there is no guarantee that all workers flush the same number of times.
///
/// NOTE: Currently round-robins through all peers, including those on remote
/// processes. A future optimization could restrict targets to process-local
/// workers only, avoiding network traffic while still concentrating batches.
pub struct ChunkedRoundRobinDistributor<CB> {
    /// Container builder accumulating items for the current target.
    builder: CB,
    /// Number of full containers to send to one target before rotating.
    chunk_size: usize,
    /// Total number of full containers produced since the last flush.
    /// The current target is derived as `(initial_target + count / chunk_size) % peers`.
    count: usize,
    /// The initial target determined by the seed.
    initial_target: usize,
}

impl<CB: Default> ChunkedRoundRobinDistributor<CB> {
    /// Constructs a new distributor for the given chunk size, seed, and number of peers.
    pub fn new(chunk_size: usize, seed: u64, peers: usize) -> Self {
        let initial_target = usize::cast_from(seed) % peers;
        Self {
            builder: Default::default(),
            chunk_size,
            count: 0,
            initial_target,
        }
    }
}

impl<CB> Distributor<CB::Container> for ChunkedRoundRobinDistributor<CB>
where
    CB: ContainerBuilder<Container: DrainContainer>
        + for<'a> PushInto<<CB::Container as DrainContainer>::Item<'a>>,
{
    fn partition<T: Clone, P: Push<Message<T, CB::Container>>>(
        &mut self,
        container: &mut CB::Container,
        time: &T,
        pushers: &mut [P],
    ) {
        let peers = pushers.len();
        for datum in container.drain() {
            self.builder.push_into(datum);
            while let Some(produced) = self.builder.extract() {
                let target = (self.initial_target + self.count / self.chunk_size) % peers;
                Message::push_at(produced, time.clone(), &mut pushers[target]);
                self.count += 1;
            }
        }
    }

    fn flush<T: Clone, P: Push<Message<T, CB::Container>>>(&mut self, time: &T, pushers: &mut [P]) {
        let peers = pushers.len();
        while let Some(container) = self.builder.finish() {
            let target = (self.initial_target + self.count / self.chunk_size) % peers;
            Message::push_at(container, time.clone(), &mut pushers[target]);
        }
        // Reset count so all workers make consistent routing decisions
        // regardless of how many times each has flushed.
        self.count = 0;
    }

    fn relax(&mut self) {
        self.builder.relax();
    }
}
