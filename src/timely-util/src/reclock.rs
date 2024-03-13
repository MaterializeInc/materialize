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

use std::iter::FromIterator;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{consolidation, AsCollection, Collection};
use timely::communication::{Message, Pull, Push};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::Scope;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};

/// Constructs an operator that reclocks a `source` collection varying with some time `FromTime`
/// into the corresponding `reclocked` collection varying over some time `IntoTime` using the
/// provided `remap` collection.
///
/// In order for the operator to read the `source` collection a `Pusher` is returned which can be
/// used with timely's capture facilities to connect a collection from a foreign scope to this
/// operator.
pub fn reclock<G, D, FromTime, IntoTime, R>(
    remap_collection: &Collection<G, FromTime, i64>,
    as_of: Antichain<G::Timestamp>,
) -> (
    Box<dyn Push<Message<Event<FromTime, (D, FromTime, R)>>>>,
    Collection<G, D, R>,
)
where
    G: Scope<Timestamp = IntoTime>,
    D: timely::Data,
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + TotalOrder,
    R: Semigroup,
{
    let mut scope = remap_collection.scope();
    let mut builder = OperatorBuilder::new("Reclock".into(), scope.clone());
    let info = builder.operator_info();
    let channel_id = scope.new_identifier();
    // Here we create a channel that can be used to send data from a foreign scope into this
    // operator. The channel is associated with this operator's address so that it is activated
    // every time events are availalbe for consumption. This mechanism is similar to Timely's input
    // handles where data can be introduced into a timely scope from an exogenous source.
    let (pusher, mut puller) =
        scope.pipeline::<Event<FromTime, (D, FromTime, R)>>(channel_id, &info.address);

    let mut remap_input = builder.new_input(&remap_collection.inner, Pipeline);
    let (mut output, reclocked) = builder.new_output();

    builder.build(move |mut caps| {
        let mut capset = CapabilitySet::from_elem(caps.pop().unwrap());
        capset.downgrade(&as_of.borrow());

        // ## Definitions
        //
        // remap_frontier: The IntoTime frontier of `remap`
        // pending_remap: Received remap bindings that are beyond `remap_frontier`
        //
        // remap_trace: A collection trace of `remap`
        // remap_upper: The upper frontier of `remap_trace`
        // remap_since: The frontier beyond which `remap_trace` accumulates to correct values
        //
        // reclockable_frontier:
        //   The FromTime frontier with the property that all source updates not beyond it can be
        //   immediately reclocked and emitted to the output. Because IntoTime is totally ordered
        //   the value of this frontier is simply the accumulation of `remap_trace`, which will
        //   accumulate to the most "recent" FromTime frontier available.
        //
        // pending_source_updates: Received source updates beyond `reclockable_frontier`
        //
        //
        // ## Core logic
        //
        // 1. Receive new `remap` updates and put them in `pending_remap`
        // 2. Extract bindings not beyond `remap_frontier` and append them in `remap_trace`
        // 3. Set `remap_upper = remap_frontier`
        // 4. Update `reclockable_frontier`
        // 5. Receive new
        // 5. stash =
        //
        //
        // The updates in pending_source_updates have already been processed
        //
        //
        // source_frontier: The FromTime frontier of `pending_source_updates`
        //
        // reclocked_source_data: The reclocked source updates, i.e the output of this operator
        // reclocked_source_frontier: The IntoTime frontier of `reclocked_source_data`
        //
        //
        //
        //   The value of `reclockable_frontier` is
        //   `remap_trace[t_into]` at the maximum `t_into` that is beyond `remap_since` and not
        //   beyond `remap_upper`. If `remap_upper <= remap_since` then this frontier is not
        //   defined. When this frontier is defined is has the property that a source update is
        //   ready to be reclocked iff it happens at a `t_from` that is not beyond it.
        //     reclockable_frontier = remap[max{t_into: remap_since <= t_into < remap_upper}]
        //
        // reclockable_updates:
        //   The set of updates in `pending_source_updates` that are not beyond `reclockable_frontier`.
        //   For these updates the operator has all the information needed to reclock them.
        //     reclockable_updates = {(data, t_from, diff) ∈ pending_source_updates: !(t_from <= reclockable_frontier) }
        //
        // reclock(t_from) -> t_into
        //   The reclock function computes the `t_into` that a time `t_from` should be
        //   reclocked to given our current knowledge of `remap_trace`. This function is only
        //   defined for `t_from` times that are not beyond `reclockable_frontier`. Its value
        //   is the minimum `t_time` that is beyond `remap_since` such that `t_from` is not beyond
        //   `remap_trace[t_into]`.
        //     reclock(t_from) = \min_{t_into: remap_since <= t_into} !(remap_trace[t_into] <= t_from)
        //
        // ### Core logic
        //
        // On each execution the operator will:
        // 1. Accept new bindings into `pending_remap` and new source data into `pending_source_updates`
        // 2. Extract bindings not beyond `remap_upper` and commit them into `remap_trace`.
        // 3. Compute `reclockable_frontier`
        // 4. Extract `reclockable_updates` from `deferred_source_updates`, transform them using the `reclock`
        //    function, and output the result into `reclocked`
        // 5. Advance `reclocked_frontier`
        // 6. Advance `remap_since` to `reclocked_frontier` and compact `remap_trace`
        //
        // Each of these steps are marked below in the code

        let mut remap_upper = Antichain::from_elem(IntoTime::minimum());
        let mut remap_since = as_of;
        let mut pending_remap = Vec::new();
        let mut remap_trace = Vec::new();

        let mut deferred_source_updates: Vec<ChainBatch<_, _, _>> = Vec::new();
        let mut source_frontier = MutableAntichain::new_bottom(FromTime::minimum());

        let mut stash = Vec::new();
        let mut tmp_updates = vec![];
        let mut vector = Vec::new();
        move |frontiers| {
            let Some(cap) = capset.get(0) else {
                return;
            };
            let mut output = output.activate();
            let mut session = output.session(cap);

            // STEP 1. Accept new bindings into `pending_remap` and new source data into `deferred_source_updates`
            while let Some((_, data)) = remap_input.next() {
                data.swap(&mut vector);
                pending_remap.append(&mut vector);
            }

            // STEP 2. Extract bindings not beyond `remap_frontier` and commit them into `remap_trace`.
            let prev_remap_upper = std::mem::replace(&mut remap_upper, frontiers[0].frontier().to_owned());
            if PartialOrder::less_than(&prev_remap_upper, &remap_upper) {
                consolidation::consolidate_updates(&mut pending_remap);
                pending_remap.sort_unstable_by(
                    |(_, t1, _): &(FromTime, IntoTime, i64), (_, t2, _)| t1.cmp(t2),
                );
                let idx = pending_remap.partition_point(|(_, t, _)| !remap_upper.less_equal(t));
                remap_trace.extend(pending_remap.drain(0..idx));
            }

            // Receive new source data and reclock with our current knowledge of remap_trace
            while let Some(event) = puller.pull() {
                match event.as_mut() {
                    Event::Progress(changes) => {
                        source_frontier.update_iter(changes.drain(..));
                    }
                    Event::Messages(_, data) => stash.extend(data.drain(..)),
                }
            }
            stash.sort_unstable_by(|(_, t1, _): &(D, FromTime, R), (_, t2, _)| t1.cmp(t2));
            let mut new_source_updates = ChainBatch::from_iter(stash.drain(..));

            // If any timestamp of the since frontier is beyond the upper we cannot proceed
            if remap_since.iter().any(|t| remap_upper.less_equal(t)) {
                return;
            }

            // We are now ready to step through the remap bindings in time order and perform the
            // following actions:
            // 1. Consider the entirety of the bindings to peel updates off of `new_source_updates`
            // 2. Consider only newly added bindings to peel updates off of `deferred_source_updates`
            // 3. Reclock `source_frontier` to calculate the new since frontier of the remap trace
            let mut cur_time = IntoTime::minimum();
            cur_time.advance_by(remap_since.borrow());
            let mut cur_binding = MutableAntichain::new();

            let mut remap = remap_trace.iter().peekable();
            let mut reclocked_source_frontier = remap_upper.clone();
            loop {
                // 0. Load relevant updates for current time into `cur_binding`
                while let Some((t_from, _, diff)) = remap.next_if(|(_, t, _)| t == &cur_time) {
                    tmp_updates.push((t_from.clone(), *diff));
                }
                cur_binding.update_iter(tmp_updates.drain(..));
                let cur_binding = cur_binding.frontier();

                // 1. Extract updates from `new_source_updates`
                for (data, _, diff) in new_source_updates.extract(cur_binding) {
                    session.give((data, cur_time.clone(), diff));
                }

                // 2. Extract updates from `deferred_source_updates` if we're looking at a new binding
                if prev_remap_upper.less_equal(&cur_time) {
                    deferred_source_updates.retain_mut(|batch| {
                        for (data, _, diff) in batch.extract(cur_binding) {
                            session.give((data, cur_time.clone(), diff));
                        }
                        // Retain non-empty batches
                        !batch.is_empty()
                    })
                }

                // 3. Reclock `source_frontier`
                if !PartialOrder::less_equal(&cur_binding, &source_frontier.frontier()) {
                    reclocked_source_frontier.insert(cur_time);
                }

                // Advance to the next binding if there is any
                match remap.peek() {
                    Some((_, t, _)) => cur_time = t.clone(),
                    None => break,
                }
            }
            capset.downgrade(&reclocked_source_frontier.borrow());

            // Compact remap trace
            remap_since = reclocked_source_frontier;
            for (_, t, _) in remap_trace.iter_mut() {
                t.advance_by(remap_since.borrow());
            }
            consolidation::consolidate_updates(&mut remap_trace);
            remap_trace
                .sort_unstable_by(|(_, t1, _): &(FromTime, IntoTime, i64), (_, t2, _)| t1.cmp(t2));

            // If source updates remain they get deferred for the future.
            if !new_source_updates.is_empty() {
                deferred_source_updates.push(new_source_updates);
            }
            // TODO: Tidy up deferred chain batches
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
        self.chains.retain(|chain| !chain.is_empty());
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
        D: timely::Data,
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
                            reclock(&binding_collection, as_of);
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
                        0u64,
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
                    Ok(Event::Messages(0, vec![(1, 1000, 1), (2, 1000, 1)]))
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
                        1001,
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
                    Ok(Event::Messages(0, vec![(50, 3000, 1),]))
                );
                assert_eq!(
                    reclocked.try_recv(),
                    Ok(Event::Progress(vec![(0, -1), (1000, 1)]))
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

        let expected = vec![(
            1500,
            vec![(1, 1500, 1), (2, 1500, 1), (3, 2000, 1), (4, 2000, 1)],
        )];
        assert_eq!(expected, reclock_compact_remap);
        assert_eq!(expected, compact_reclock_remap);
    }
}
