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

//! An operator that transforms a `source` collection that evolves with some timestamp `FromTime`
//! into a collection that evolve with some other timestamp `IntoTime`.
//!
//! The operator processes a `remap` collection, which is a map from a target timestamp `IntoTime`
//! to a set of `FromTime` times which form an antichain. The `remap` collection must maintain the
//! property of monotonicity: if `into1 <= into2` then `remap[into1] <= remap[into2]` where the
//! second inequality is the partial order on antichains.
//!
//! The operator produces the `reclocked` collection which is defined as a differential dataflow
//! collection that that contains at `into` all messages from `source` whose timestamp is not
//! greater than or equal to some element of `remap[into]`. The `reclocked` collection is created
//! in the same scope as the `remap` collection and therefore evolves according to `IntoTime`.
//!
//! The `source` collection is not possible to be connected to the reclock operator as a normal
//! timely input since that collection exists in a different scope whose timestamp is `FromTime`.
//! Instead, the `source` collection must be captured (e.g using timely's capture facilities) and
//! the raw data be sent into the `Pusher` constructed and returned by the reclock operator.

use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::iter::FromIterator;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};
use timely::communication::{Message, Pull, Push};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::Scope;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};

/// Constructs an operator that reclocks a `source` collection varying with some time `FromTime`
/// into the corresponding `reclocked` collection varying over some time `IntoTime` using the
/// provided `remap` collection.
///
/// Each record and its original `FromTime` timestamp are transformed through `logic` before being
/// produced in the `reclocked` collection.
///
/// In order for the operator to read the `source` collection a `Pusher` is returned which can be
/// used with timely's capture facilities to connect a collection from a foreign scope to this
/// operator.
pub fn reclock<G, D1, D2, FromTime, R, L>(
    remap_collection: &Collection<G, FromTime, i64>,
    as_of: Antichain<G::Timestamp>,
    mut logic: L,
) -> (
    Box<dyn Push<Message<Event<FromTime, (D1, FromTime, R)>>>>,
    Collection<G, D2, R>,
)
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder,
    D1: timely::Data,
    D2: timely::Data,
    FromTime: Timestamp,
    R: Semigroup,
    L: FnMut(D1, FromTime) -> D2 + 'static,
{
    let mut scope = remap_collection.scope();
    let mut builder = OperatorBuilder::new("Reclock".into(), scope.clone());
    let info = builder.operator_info();
    let channel_id = scope.new_identifier();
    let (pusher, mut puller) =
        scope.pipeline::<Event<FromTime, (D1, FromTime, R)>>(channel_id, &info.address);

    let mut remap_input = builder.new_input(&remap_collection.inner, Pipeline);
    let (mut output, reclocked) = builder.new_output();

    builder.build(move |caps| {
        let [cap]: [_; 1] = caps.try_into().expect("one output");

        // Remap updates beyond the upper
        let mut accepted_bindings = Vec::new();
        let mut replayer = BindingReplay::new(cap, as_of.borrow());

        // The upper frontier of updates in source_batches
        let mut source_upper = MutableAntichain::new_bottom(Timestamp::minimum());
        // Batches of data waiting to be reclocked
        let mut source_batches = VecDeque::new();

        let mut vector = Vec::new();
        move |frontiers| {
            // Accept new bindings
            while let Some((_, data)) = remap_input.next() {
                data.swap(&mut vector);
                accepted_bindings.append(&mut vector);
            }
            // Extract ready batches from accepted bindings and update the replayer
            let upper = frontiers[0].frontier();
            if PartialOrder::less_than(&replayer.upper(), &upper) {
                accepted_bindings.sort_unstable_by(|a, b| a.1.cmp(&b.1));
                // The times are totally ordered so we can binary search to find the prefix that is
                // not beyond the upper and extract it into a batch.
                let idx = accepted_bindings.partition_point(|(_, t, _)| !upper.less_equal(t));
                replayer.append_batch(accepted_bindings.drain(0..idx), upper.to_owned());
            }

            // Accept new data
            while let Some(event) = puller.pull() {
                match event.as_mut() {
                    Event::Progress(changes) => {
                        source_upper.update_iter(changes.drain(..));
                    }
                    Event::Messages(_, data) => {
                        // Sorting by the linear extension before computing the chain decomposition
                        // of the batch leads to an optimal chain decomposition for a number of
                        // concrete Timestamp implementations that we are interested in.
                        data.sort_unstable_by(|a: &(D1, FromTime, R), b| a.1.cmp(&b.1));
                        source_batches.push_back(ChainBatch::from_iter(data.drain(..)));
                    }
                }
            }

            // Reclock accepted data
            let mut output = output.activate();
            while let Some((frontier, cap)) = replayer.active_binding() {
                let mut session = output.session(cap);
                let into_t = cap.time();
                source_batches.retain_mut(|batch| {
                    session.give_iterator(
                        batch
                            .extract(frontier)
                            .map(|(d, t, r)| (logic(d, t), into_t.clone(), r)),
                    );
                    // Retain non-empty batches
                    !batch.is_empty()
                });
                // If we won't receive any more data for this binding we can go to the next one
                if PartialOrder::less_equal(&frontier, &source_upper.frontier()) {
                    replayer.step();
                } else {
                    // Otherwise yield and wait for more data
                    break;
                }
            }
        }
    });

    (Box::new(pusher), reclocked.as_collection())
}

/// A batch of differential updates that vary over some partial order. This type maintains the data
/// as a set of disjoint chains that allows for efficient extraction of batches given a frontier.
#[derive(Debug)]
struct ChainBatch<D, T, R> {
    /// A list of chains (sets of mutually comparable times) sorted by the partial order.
    chains: Vec<Vec<(D, T, R)>>,
}

impl<D, T: Timestamp, R> ChainBatch<D, T, R> {
    /// Extracts all updates with time not greater or equal to any time in `upper`.
    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'a, T>,
    ) -> impl Iterator<Item = (D, T, R)> + 'a {
        self.chains
            .iter_mut()
            .map(move |chain| {
                // A chain is a set of mutually comparable elements so we can binary search
                // to find the prefix that is not beyond the frontier and extract it.
                let idx = chain.partition_point(|(_, time, _)| !upper.less_equal(time));
                chain.drain(0..idx)
            })
            .flatten()
    }

    /// Indicates if this batch contains any update.
    fn is_empty(&self) -> bool {
        self.chains.iter().all(|chain| chain.is_empty())
    }
}

impl<D, T: Timestamp, R> FromIterator<(D, T, R)> for ChainBatch<D, T, R> {
    /// Computes the chain decomposition of updates according to the partial order `T`.
    fn from_iter<I: IntoIterator<Item = (D, T, R)>>(updates: I) -> Self {
        let mut chains = vec![];
        let mut updates = updates.into_iter();
        if let Some((d, t, r)) = updates.next() {
            let mut chain = vec![(d, t, r)];
            for (d, t, r) in updates {
                let prev_t = &chain[chain.len() - 1].1;
                if !PartialOrder::less_equal(prev_t, &t) {
                    chains.push(chain);
                    chain = vec![];
                }
                chain.push((d, t, r));
            }
            chains.push(chain);
        }
        Self { chains }
    }
}

/// A struct that reveals the frontiers encoded in a remap collection in time order.
struct BindingReplay<FromTime, IntoTime: Timestamp + TotalOrder> {
    /// The upper frontier of bindings received so far. An `Option<Capability>` is sufficient to
    /// describe a frontier because `IntoTime` is required to be totally ordered.
    upper_capability: Option<Capability<IntoTime>>,
    /// A queue of bindings sorted by time
    bindings: VecDeque<(FromTime, IntoTime, i64)>,
    /// An option representing whether an active binding exists. The option carries the capability
    /// that should be used to send all the source data corresponding to its time.
    active_binding: Option<Capability<IntoTime>>,
    /// The accumulated frontier of the current active binding.
    frontier: MutableAntichain<FromTime>,
}

impl<FromTime, IntoTime> BindingReplay<FromTime, IntoTime>
where
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
{
    /// Constructs a new replayer with a given initial capability. The provided capability must be
    /// at the minimum timestamp.
    fn new(cap: Capability<IntoTime>, as_of: AntichainRef<IntoTime>) -> Self {
        assert_eq!(
            cap.time(),
            &IntoTime::minimum(),
            "capability not the initial capability"
        );

        // The minimum timestamp advanced by the as_of frontier will accumulate to the minimum
        // FromTime binding and so that becomes the first binding this replayer will report.
        let mut min_ts = IntoTime::minimum();
        min_ts.advance_by(as_of);
        let first_binding = cap.delayed(&min_ts);

        BindingReplay {
            upper_capability: Some(cap),
            bindings: VecDeque::new(),
            frontier: MutableAntichain::new(),
            active_binding: Some(first_binding),
        }
    }

    /// The upper frontier of bindings received so far.
    fn upper(&self) -> AntichainRef<IntoTime> {
        let times = match self.upper_capability.as_deref() {
            Some(time) => std::slice::from_ref(time),
            None => &[],
        };
        AntichainRef::new(times)
    }

    /// Informs the replayer about the next batch of bindings in the `remap` collection. Each call
    /// to `append_batch` must provide complete batches, where complete means that all future calls
    /// to `append_batch` will contain updates that are beyond the provided `upper`.
    fn append_batch<I>(&mut self, data: I, upper: Antichain<IntoTime>)
    where
        I: IntoIterator<Item = (FromTime, IntoTime, i64)>,
    {
        self.bindings.extend(data.into_iter());
        if self.active_binding.is_none() {
            self.active_binding = self.upper_capability.clone();
            self.step();
        }
        match (self.upper_capability.as_mut(), upper.as_option()) {
            (Some(cap), Some(time)) => cap.downgrade(time),
            (_, None) => self.upper_capability = None,
            (None, Some(_)) => unreachable!(),
        }
    }

    /// Reveals the currently active binding and its accosiated `FromTime` frontier.
    fn active_binding(&mut self) -> Option<(AntichainRef<FromTime>, &Capability<IntoTime>)> {
        match self.active_binding.as_ref() {
            // When the replayer initializes the active binding to the minimum timestamp we don't
            // yet know the FromTime frontier that corresponds to it so we must wait for the first
            // batch to arrive.
            Some(cap) if self.upper().less_equal(cap.time()) => None,
            Some(cap) => {
                // Accumulate the bindings that are not beyond cap.time() into self.frontier to
                // compute the FromTime frontier that corresponds to this IntoTime.
                let idx = self
                    .bindings
                    .iter()
                    .position(|b| &b.1 != cap.time())
                    .unwrap_or(self.bindings.len());
                let updates = self.bindings.drain(0..idx).map(|(from, _, r)| (from, r));
                self.frontier.update_iter(updates);

                Some((self.frontier.frontier(), cap))
            }
            None => None,
        }
    }

    /// Steps the replayer to the next binding if one exists.
    fn step(&mut self) {
        match self.bindings.front().cloned() {
            Some((_, into, _)) => self.active_binding.as_mut().unwrap().downgrade(&into),
            None => self.active_binding = None,
        }
    }
}

impl<FromTime: Debug, IntoTime: Timestamp + TotalOrder> Debug
    for BindingReplay<FromTime, IntoTime>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindingReplay")
            .field("upper", &self.upper_capability.as_deref())
            .field("bindings", &self.bindings)
            .field("active_binding", &self.active_binding.as_deref())
            .field("frontier", &&*self.frontier.frontier())
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::{Receiver, TryRecvError};

    use differential_dataflow::consolidation;
    use differential_dataflow::input::{Input, InputSession};
    use timely::communication::allocator::Thread;
    use timely::dataflow::operators::capture::{Event, Extract};
    use timely::dataflow::operators::unordered_input::UnorderedHandle;
    use timely::dataflow::operators::{ActivateCapability, Capture, UnorderedInput};
    use timely::worker::Worker;

    use crate::capture::PusherCapture;
    use crate::order::Partitioned;

    use super::*;

    type FromTime = Partitioned<u64, u64>;
    type IntoTime = u64;
    type BindingHandle = InputSession<IntoTime, FromTime, i64>;
    type DataHandle<D> = (
        UnorderedHandle<FromTime, (D, FromTime, i64)>,
        ActivateCapability<FromTime>,
    );
    type ReclockedStream<D> = Receiver<Event<IntoTime, (D, IntoTime, i64)>>;

    /// A helper function that sets up a dataflow program to test the reclocking operator. Each
    /// test provides a test logic closure which accepts four arguments:
    ///
    /// * A reference to the worker that allows the test to step the computation
    /// * A `BindingHandle` that allows the test to manipulate the remap bindings
    /// * A `DataHandle` that allows the test to submit the data to be reclocked
    /// * A `ReclockedStream` that allows observing the result of the reclocking process
    fn harness<D, F, R>(as_of: Antichain<IntoTime>, test_logic: F) -> R
    where
        D: timely::Data + Debug,
        F: FnOnce(&mut Worker<Thread>, BindingHandle, DataHandle<D>, ReclockedStream<D>) -> R
            + Send
            + Sync
            + 'static,
        R: Send + 'static,
    {
        timely::execute_directly(move |worker| {
            let (bindings, data, data_cap, reclocked) = worker.dataflow::<(), _, _>(|scope| {
                let (bindings, data_pusher, reclocked) =
                    scope.scoped::<IntoTime, _, _>("IntoScope", move |scope| {
                        let (binding_handle, binding_collection) = scope.new_collection();
                        let (data_pusher, reclocked_collection) =
                            reclock(&binding_collection, as_of, |d, _| d);
                        let reclocked_capture = reclocked_collection.inner.capture();
                        (binding_handle, data_pusher, reclocked_capture)
                    });

                let (data, data_cap) = scope.scoped::<FromTime, _, _>("FromScope", move |scope| {
                    let ((handle, cap), data) = scope.new_unordered_input();
                    data.capture_into(PusherCapture(data_pusher));
                    (handle, cap)
                });

                (bindings, data, data_cap, reclocked)
            });

            test_logic(worker, bindings, (data, data_cap), reclocked)
        })
    }

    /// Steps the worker four times which is the required number of times for both data and
    /// frontier updates to propagate across the two scopes and into the probing channels.
    fn step(worker: &mut Worker<Thread>) {
        for _ in 0..4 {
            worker.step();
        }
    }

    #[mz_ore::test]
    fn basic_reclocking() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, bindings, (mut data, data_cap), reclocked| {
                // Reclock everything at the minimum IntoTime
                bindings.close();
                data.session(data_cap)
                    .give(("a", Partitioned::minimum(), 1));
                step(worker);
                let extracted = reclocked.extract();
                let expected = vec![(0, vec![("a", 0, 1)])];
                assert_eq!(extracted, expected);
            },
        )
    }

    /// Generates a `Partitioned<u64, u64>` Antichain where all the provided
    /// partitions are at the specified offset and the gaps in between are filled with range
    /// timestamps at offset zero.
    fn partitioned_frontier<I>(items: I) -> Antichain<Partitioned<u64, u64>>
    where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let mut frontier = Antichain::new();
        let mut prev = 0;
        for (pid, offset) in items {
            if prev < pid {
                frontier.insert(Partitioned::new_range(prev, pid - 1, 0));
            }
            frontier.insert(Partitioned::new_singleton(pid, offset));
            prev = pid + 1
        }
        frontier.insert(Partitioned::new_range(prev, u64::MAX, 0));
        frontier
    }

    #[mz_ore::test]
    fn test_basic_usage() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Reclock offsets 1 and 3 to timestamp 1000
                bindings.update_at(Partitioned::minimum(), 0, 1);
                bindings.update_at(Partitioned::minimum(), 1000, -1);
                for time in partitioned_frontier([(0, 4)]) {
                    bindings.update_at(time, 1000, 1);
                }
                bindings.advance_to(1001);
                bindings.flush();
                data.session(data_cap.clone()).give_content(&mut vec![
                    (1, Partitioned::new_singleton(0, 1), 1),
                    (1, Partitioned::new_singleton(0, 1), 1),
                    (3, Partitioned::new_singleton(0, 3), 1),
                ]);

                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        1000u64,
                        vec![(1, 1000, 1), (1, 1000, 1), (3, 1000, 1)]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );

                // Reclock more messages for offsets 3 to the same timestamp
                data.session(data_cap.clone()).give_content(&mut vec![
                    (3, Partitioned::new_singleton(0, 3), 1),
                    (3, Partitioned::new_singleton(0, 3), 1),
                ]);
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(1000u64, vec![(3, 1000, 1), (3, 1000, 1)]))
                );

                // Drop the capability which should advance the reclocked frontier to 1001.
                drop(data_cap);
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (1001, 1)]))
                );
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock_frontier() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness::<(), _, _>(
            as_of,
            |worker, mut bindings, (_data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, 1);
                bindings.advance_to(1);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1, 1)]))
                );

                // Mint a couple of bindings for multiple partitions
                bindings.update_at(Partitioned::minimum(), 1000, -1);
                for time in partitioned_frontier([(1, 10)]) {
                    bindings.update_at(time.clone(), 1000, 1);
                    bindings.update_at(time, 2000, -1);
                }
                for time in partitioned_frontier([(1, 10), (2, 10)]) {
                    bindings.update_at(time, 2000, 1);
                }
                bindings.advance_to(2001);
                bindings.flush();

                // The initial frontier should now map to the minimum between the two partitions
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1, -1), (1000, 1)]))
                );

                // Downgrade data frontier such that only one of the partitions is advanced
                let mut part1_cap = data_cap.delayed(&Partitioned::new_singleton(1, 9));
                let mut part2_cap = data_cap.delayed(&Partitioned::new_singleton(2, 0));
                let _rest_cap = data_cap.delayed(&Partitioned::new_range(3, u64::MAX, 0));
                drop(data_cap);
                step(worker);
                assert_eq!(reclocked.try_recv(), Err(TryRecvError::Empty));

                // Downgrade the data frontier past the first binding
                part1_cap.downgrade(&Partitioned::new_singleton(1, 10));
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (2000, 1)]))
                );

                // Downgrade the data frontier past the second binding
                part2_cap.downgrade(&Partitioned::new_singleton(2, 10));
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2000, -1), (2001, 1)]))
                );

                // Advance the binding frontier and confirm that we get to the next timestamp
                bindings.advance_to(3001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2001, -1), (3001, 1)]))
                );
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, 1);

                // Setup more precise capabilities for the rest of the test
                let mut part0_cap = data_cap.delayed(&Partitioned::new_singleton(0, 0));
                let rest_cap = data_cap.delayed(&Partitioned::new_range(1, u64::MAX, 0));
                drop(data_cap);

                // Reclock offsets 1 and 2 to timestamp 1000
                data.session(part0_cap.clone()).give_content(&mut vec![
                    (1, Partitioned::new_singleton(0, 1), 1),
                    (2, Partitioned::new_singleton(0, 2), 1),
                ]);

                part0_cap.downgrade(&Partitioned::new_singleton(0, 3));
                bindings.update_at(Partitioned::minimum(), 1000, -1);
                bindings.update_at(part0_cap.time().clone(), 1000, 1);
                bindings.update_at(rest_cap.time().clone(), 1000, 1);
                bindings.advance_to(1001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(1000, vec![(1, 1000, 1), (2, 1000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (1001, 1)]))
                );

                // Reclock offsets 3 and 4 to timestamp 2000
                data.session(part0_cap.clone()).give_content(&mut vec![
                    (3, Partitioned::new_singleton(0, 3), 1),
                    (3, Partitioned::new_singleton(0, 3), 1),
                    (4, Partitioned::new_singleton(0, 4), 1),
                ]);
                bindings.update_at(part0_cap.time().clone(), 2000, -1);
                part0_cap.downgrade(&Partitioned::new_singleton(0, 5));
                bindings.update_at(part0_cap.time().clone(), 2000, 1);
                bindings.advance_to(2001);
                bindings.flush();
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(
                        2000,
                        vec![(3, 2000, 1), (3, 2000, 1), (4, 2000, 1)]
                    ))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1001, -1), (2000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(2000, -1), (2001, 1)]))
                );
            },
        );
    }

    #[mz_ore::test]
    fn test_reclock_gh16318() {
        let as_of = Antichain::from_elem(IntoTime::minimum());
        harness(
            as_of,
            |worker, mut bindings, (mut data, data_cap), reclocked| {
                // Initialize the bindings such that the minimum IntoTime contains the minimum FromTime
                // frontier.
                bindings.update_at(Partitioned::minimum(), 0, 1);
                // First mint bindings for 0 at timestamp 1000
                bindings.update_at(Partitioned::minimum(), 1000, -1);
                for time in partitioned_frontier([(0, 50)]) {
                    bindings.update_at(time, 1000, 1);
                }
                // Then only for 1 at timestamp 2000
                for time in partitioned_frontier([(0, 50)]) {
                    bindings.update_at(time, 2000, -1);
                }
                for time in partitioned_frontier([(0, 50), (1, 50)]) {
                    bindings.update_at(time, 2000, 1);
                }
                // Then again only for 0 at timestamp 3000
                for time in partitioned_frontier([(0, 50), (1, 50)]) {
                    bindings.update_at(time, 3000, -1);
                }
                for time in partitioned_frontier([(0, 100), (1, 50)]) {
                    bindings.update_at(time, 3000, 1);
                }
                bindings.advance_to(3001);
                bindings.flush();

                // Reclockng (0, 50) must ignore the updates on the FromTime frontier that happened at
                // timestamp 2000 since those are completely unrelated
                data.session(data_cap)
                    .give((50, Partitioned::new_singleton(0, 50), 1));
                step(worker);
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Messages(3000, vec![(50, 3000, 1),]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(1000, -1), (3001, 1)]))
                );
            },
        );
    }

    /// Test that compact(reclock(remap, source)) == reclock(compact(remap), source)
    #[mz_ore::test]
    fn test_compaction() {
        let mut remap = vec![];
        remap.push((Partitioned::minimum(), 0, 1));
        // Reclock offsets 1 and 2 to timestamp 1000
        remap.push((Partitioned::minimum(), 1000, -1));
        for time in partitioned_frontier([(0, 3)]) {
            remap.push((time, 1000, 1));
        }
        // Reclock offsets 3 and 4 to timestamp 2000
        for time in partitioned_frontier([(0, 3)]) {
            remap.push((time, 2000, -1));
        }
        for time in partitioned_frontier([(0, 5)]) {
            remap.push((time, 2000, 1));
        }

        let source_updates = vec![
            (1, Partitioned::new_singleton(0, 1), 1),
            (2, Partitioned::new_singleton(0, 2), 1),
            (3, Partitioned::new_singleton(0, 3), 1),
            (4, Partitioned::new_singleton(0, 4), 1),
        ];

        let since = Antichain::from_elem(1500);

        // Compute reclock(remap, source)
        let as_of = Antichain::from_elem(IntoTime::minimum());
        let remap1 = remap.clone();
        let source_updates1 = source_updates.clone();
        let reclock_remap = harness(
            as_of,
            move |worker, mut bindings, (mut data, data_cap), reclocked| {
                for (from_ts, into_ts, diff) in remap1 {
                    bindings.update_at(from_ts, into_ts, diff);
                }
                bindings.close();
                data.session(data_cap)
                    .give_iterator(source_updates1.iter().cloned());
                step(worker);
                reclocked.extract()
            },
        );
        // Compute compact(reclock(remap, source))
        let mut compact_reclock_remap = reclock_remap;
        for (t, updates) in compact_reclock_remap.iter_mut() {
            t.advance_by(since.borrow());
            for (_, t, _) in updates.iter_mut() {
                t.advance_by(since.borrow());
            }
        }

        // Compute compact(remap)
        let mut compact_remap = remap;
        for (_, t, _) in compact_remap.iter_mut() {
            t.advance_by(since.borrow());
        }
        consolidation::consolidate_updates(&mut compact_remap);
        // Compute reclock(compact(remap), source)
        let reclock_compact_remap = harness(
            since,
            move |worker, mut bindings, (mut data, data_cap), reclocked| {
                for (from_ts, into_ts, diff) in compact_remap {
                    bindings.update_at(from_ts, into_ts, diff);
                }
                bindings.close();
                data.session(data_cap)
                    .give_iterator(source_updates.iter().cloned());
                step(worker);
                reclocked.extract()
            },
        );

        let expected = vec![
            (1500, vec![(1, 1500, 1), (2, 1500, 1)]),
            (2000, vec![(3, 2000, 1), (4, 2000, 1)]),
        ];
        assert_eq!(expected, reclock_compact_remap);
        assert_eq!(expected, compact_reclock_remap);
    }
}
