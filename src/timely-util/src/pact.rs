// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parallelization contracts, describing requirements for data movement along dataflow edges.

use timely::communication::{Data, Pull, Push};
use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContractCore};
use timely::dataflow::channels::{BundleCore, Message};
use timely::logging::TimelyLogger;
use timely::progress::Timestamp;
use timely::worker::AsWorker;
use timely::Container;

/// A connection that distributes containers to all workers in a round-robin fashion
#[derive(Debug)]
pub struct Distribute;

impl<T: Timestamp, C: Container + Data> ParallelizationContractCore<T, C> for Distribute {
    type Pusher = DistributePusher<LogPusher<T, C, Box<dyn Push<BundleCore<T, C>>>>>;
    type Puller = LogPuller<T, C, Box<dyn Pull<BundleCore<T, C>>>>;

    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: &[usize],
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

impl<T: Eq + Data, C: Container, P: Push<BundleCore<T, C>>> Push<BundleCore<T, C>>
    for DistributePusher<P>
{
    fn push(&mut self, message: &mut Option<BundleCore<T, C>>) {
        let worker_idx = self.next;
        self.next = (self.next + 1) % self.pushers.len();
        self.pushers[worker_idx].push(message);
    }
}
