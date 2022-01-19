// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Modular Timely Dataflow operators that can persist and seal updates in streams.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;

use persist_types::Codec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::{Branch, Concat, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::Data as TimelyData;
use timely::PartialOrder;

use crate::client::StreamWriteHandle;
use crate::error::Error;
use crate::storage::SeqNo;

/// Extension trait for [`Stream`].
pub trait Persist<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Passes through each element of the stream and persists it.
    ///
    /// This does not wait for persistence before passing through the data. We do, however, wait
    /// for data to be persisted before allowing the frontier to advance. In other words, this
    /// operator is holding on to capabilities as long as data belonging to their timestamp is not
    /// persisted.
    ///
    /// Use this together with [`seal`](Seal::seal)/[`conditional_seal`](Seal::conditional_seal)
    /// and [`await_frontier`](AwaitFrontier::await_frontier) if you want to make sure that data only
    /// becomes available downstream when persisted and sealed.
    ///
    /// **Note:** If you need to also replay persisted data when restarting, concatenate the output
    /// of this operator with the output of `replay()`.
    ///
    // TODO: The goal for the persistence operators is to have one combined output stream of
    // `Result<T, E`. However, for operators that need to pass through all input updates, this
    // seems excessively expensive if the input updates are not already wrapped in `Result`.  We
    // therefore return two separate output streams for now but might want to reconsider this
    // holistically, when/if we can already wrap all updates in `Result` at the source.
    fn persist(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K, V> Persist<G, K, V> for Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec,
    V: TimelyData + Codec,
{
    fn persist(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
        let scope = self.scope();
        let operator_name = format!("persist({})", name);
        let mut persist_op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut input = persist_op.new_input(&self, Pipeline);

        let (mut data_output, data_output_stream) = persist_op.new_output();
        let (mut error_output, error_output_stream) = persist_op.new_output();

        let mut buffer = Vec::new();
        let error_output_port = error_output_stream.name().port;

        // An activator that allows futures to re-schedule this operator when ready.
        let activator = Arc::new(scope.sync_activator_for(&persist_op.operator_info().address[..]));

        let mut pending_futures = VecDeque::new();

        persist_op.build(move |_capabilities| {
            move |_frontiers| {
                let mut data_output = data_output.activate();
                let mut error_output = error_output.activate();

                // Write out everything and forward, keeping the write futures.
                input.for_each(|cap, data| {
                    data.swap(&mut buffer);

                    let write_future = write.write(buffer.iter().as_ref());

                    let mut session = data_output.session(&cap);
                    session.give_vec(&mut buffer);

                    // We are not using the capability for the main output later, but we are
                    // holding on to it to keep the frontier from advancing because that frontier
                    // is used downstream to track how far we have persisted. This is used, for
                    // example, by seal()/conditional_seal() operators and await_frontier().
                    pending_futures.push_back((
                        cap.delayed(cap.time()),
                        cap.retain_for_output(error_output_port),
                        write_future,
                    ));
                });

                // Swing through all pending futures and see if they're ready. Ready futures will
                // invoke the Activator, which will make sure that we arrive here, even when there
                // are no changes in the input frontier or new input.
                let waker = futures_util::task::waker_ref(&activator);
                let mut context = Context::from_waker(&waker);

                while let Some((cap, error_cap, pending_future)) = pending_futures.front_mut() {
                    match Pin::new(pending_future).poll(&mut context) {
                        std::task::Poll::Ready(result) => {
                            match result {
                                Ok(seq_no) => {
                                    tracing::trace!(
                                        "In {}, finished writing for time: {}, seq_no: {:?}",
                                        &operator_name,
                                        cap.time(),
                                        seq_no,
                                    );
                                }
                                Err(e) => {
                                    let mut session = error_output.session(&error_cap);
                                    let error = format!(
                                        "In {}, error writing data for time {}: {}",
                                        &operator_name,
                                        error_cap.time(),
                                        e
                                    );
                                    tracing::error!("{}", error);

                                    // TODO: make error retractable? Probably not...
                                    session.give((error, *error_cap.time(), 1));
                                }
                            }

                            let _ = pending_futures.pop_front().expect("known to exist");
                        }
                        std::task::Poll::Pending => {
                            // We assume that write requests are worked off in order and stop
                            // trying for the first write that is not done.
                            break;
                        }
                    }
                }
            }
        });

        (data_output_stream, error_output_stream)
    }
}

/// Extension trait for [`Stream`].
pub trait Seal<G: Scope<Timestamp = u64>, D: TimelyData> {
    /// Passes through each element of the stream and seals the given collection (the `write`
    /// handle) when the input frontier advances.
    ///
    /// This does not wait for the seal to succeed before passing through the data. We do, however,
    /// wait for the seal to be successful before allowing the frontier to advance. In other words,
    /// this operator is holding on to capabilities as long as seals corresponding to their
    /// timestamp are not done.
    fn seal<K, V>(&self, name: &str, write: StreamWriteHandle<K, V>) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec;

    /// Passes through each element of the stream and seals the given primary and condition
    /// collections, respectively, when their frontier advances. The primary collection is only
    /// sealed up to a time `t` when the condition collection has also been sealed up to `t`.
    ///
    /// This does not wait for the seals to succeed before passing through the data. We do,
    /// however, wait for the seals to be successful before allowing the frontier to advance. In
    /// other words, this operator is holding on to capabilities as long as seals corresponding to
    /// their timestamp are not done.
    fn conditional_seal<D2: TimelyData, K, V, K2, V2>(
        &self,
        name: &str,
        condition_input: &Stream<G, (D2, u64, isize)>,
        primary_write: StreamWriteHandle<K, V>,
        condition_write: StreamWriteHandle<K2, V2>,
    ) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec,
        K2: TimelyData + Codec,
        V2: TimelyData + Codec;
}

impl<G, D> Seal<G, D> for Stream<G, (D, u64, isize)>
where
    G: Scope<Timestamp = u64>,
    D: TimelyData,
{
    fn seal<K, V>(&self, name: &str, write: StreamWriteHandle<K, V>) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec,
    {
        let operator_name = format!("seal({})", name);
        let mut seal_op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut data_input = seal_op.new_input(&self, Pipeline);
        let (mut data_output, data_output_stream) = seal_op.new_output();
        let mut data_buffer = Vec::new();
        let mut input_frontier = Antichain::from_elem(<G::Timestamp as TimelyTimestamp>::minimum());
        // We only seal from one worker because sealing from multiple workers could lead to a race
        // conditions where one worker seals up to time `t` while another worker is still trying to
        // write data with timestamps that are not beyond `t`.
        //
        // Upstream persist() operators will only advance their frontier when writes are succesful.
        // With timely progress tracking we are therefore sure that when the frontier advances for
        // worker 0, it has advanced to at least that point for all upstream operators.
        //
        // Alternative solutions would be to "teach" persistence to work with seals from multiple
        // workers, or to use a non-timely solution for keeping track of outstanding write
        // capabilities.
        let active_seal_operator = self.scope().index() == 0;
        // An activator that allows futures to re-schedule this operator when ready.
        let activator = Arc::new(
            self.scope()
                .sync_activator_for(&seal_op.operator_info().address[..]),
        );

        let mut pending_futures = VecDeque::new();

        seal_op.build_reschedule(move |mut capabilities| {
            let mut cap_set = if active_seal_operator {
                CapabilitySet::from_elem(capabilities.pop().expect("missing capability"))
            } else {
                CapabilitySet::new()
            };

            move |frontiers| {
                let mut data_output = data_output.activate();

                // Pass through all data.
                data_input.for_each(|cap, data| {
                    data.swap(&mut data_buffer);

                    let mut session = data_output.session(&cap);
                    session.give_vec(&mut data_buffer);
                });

                if !active_seal_operator {
                    // We are always complete if we're not the active seal operator.
                    return false;
                }

                let mut new_input_frontier = Antichain::new();
                new_input_frontier.extend(frontiers[0].frontier().into_iter().cloned());

                // We seal for every element in the new frontier that represents progress compared
                // to the old frontier. Alternatively, we could always seal to the current
                // frontier, because sealing is idempotent or seal to the current frontier if there
                // is any progress compared to the previous frontier.
                //
                // The current solution is the one that does the least amount of expected work.
                // However, with frontiers of Antichain<u64> we will always only have a single
                // element in the frontier/antichain, so the optimization is somewhat unnecessary.
                // This way, we are prepared for a future of multi-dimensional frontiers, though.
                for frontier_element in new_input_frontier.iter() {
                    if input_frontier.less_than(&frontier_element) {
                        tracing::trace!(
                            "In {}, sealing collection up to {}...",
                            &operator_name,
                            frontier_element,
                        );

                        let future = write.seal(*frontier_element);

                        pending_futures.push_back(SealFuture {
                            time: *frontier_element,
                            future,
                        });
                    }
                }

                // Swing through all pending futures and see if they're ready. Ready futures will
                // invoke the Activator, which will make sure that we arrive here, even when there
                // are no changes in the input frontier or new input.
                let waker = futures_util::task::waker_ref(&activator);
                let mut context = Context::from_waker(&waker);

                while let Some(mut pending_future) = pending_futures.pop_front() {
                    match Pin::new(&mut pending_future.future).poll(&mut context) {
                        std::task::Poll::Ready(Ok(_)) => {
                            tracing::trace!(
                                "In {}, finished sealing collection up to {}",
                                &operator_name,
                                pending_future.time,
                            );

                            // Explicitly downgrade the capability to the new time.
                            cap_set.downgrade(Some(pending_future.time));
                        }
                        std::task::Poll::Ready(Err(e)) => {
                            tracing::trace!(
                                "Error sealing {} up to {}: {}",
                                &operator_name,
                                pending_future.time,
                                e
                            );

                            // Only retry this seal if there is no other pending conditional
                            // seal at a time >= this seal's time.
                            let retry = {
                                let mut retry = true;
                                let seal_ts = pending_future.time;
                                for seal_future in pending_futures.iter() {
                                    if seal_future.time >= seal_ts {
                                        retry = false;
                                        break;
                                    }
                                }
                                retry
                            };

                            if retry {
                                tracing::trace!(
                                    "Adding seal to queue again: {}",
                                    pending_future.time
                                );

                                let future = write.seal(pending_future.time);
                                pending_futures.push_front(SealFuture {
                                    time: pending_future.time,
                                    future,
                                });
                            }
                        }
                        std::task::Poll::Pending => {
                            // We assume that seal requests are worked off in order and stop
                            // trying for the first seal that is not done.
                            // Push the future back to the front of the queue. We have to
                            // do this dance of popping and pushing because we're modifying
                            // the queue while we work on a future. This prevents us from
                            // just getting a reference to the front of the queue and then
                            // popping once we know that a future is done.
                            pending_futures.push_front(pending_future);
                            break;
                        }
                    }
                }

                input_frontier.clone_from(&new_input_frontier);
                // We need to downgrade when the input frontier is empty. This basically releases
                // all the capabilities so that downstream operators and eventually the worker can
                // shut down. We also need to clear all pending futures to make sure we never
                // attempt to downgrade any more capabilities.
                if input_frontier.is_empty() {
                    cap_set.downgrade(input_frontier.iter());
                    pending_futures.clear();
                }

                !pending_futures.is_empty()
            }
        });

        data_output_stream
    }

    fn conditional_seal<D2: TimelyData, K, V, K2, V2>(
        &self,
        name: &str,
        condition_input: &Stream<G, (D2, u64, isize)>,
        primary_write: StreamWriteHandle<K, V>,
        condition_write: StreamWriteHandle<K2, V2>,
    ) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec,
        K2: TimelyData + Codec,
        V2: TimelyData + Codec,
    {
        let condition = condition_input.seal(&format!("condition_seal_{}", name), condition_write);

        // Create a data dependency between `condition_ok` and `self`, so that
        // the data in `self` is only sealed when `condition` has successfully
        // sealed.
        let condition = condition.flat_map(|_| None);
        self.concat(&condition)
            .seal(&format!("primary_seal_{}", name), primary_write)
    }
}

struct SealFuture<F: Future<Output = Result<SeqNo, Error>>> {
    time: u64,
    future: F,
}

/// Extension trait for [`Stream`].
pub trait AwaitFrontier<G: Scope<Timestamp = u64>, D> {
    /// Stashes data until it is no longer beyond the input frontier.
    ///
    /// This is similar, in spirit, to what `consolidate()` does for differential collections and
    /// what `delay()` does for timely streams. However, `consolidate()` does more work than what we
    /// need and `delay()` deals with changing the timestamp while the behaviour we want is to wait for
    /// the frontier to pass. The latter is an implementation detail of `delay()` that is not
    /// advertised in its documentation. We therefore have our own implementation that we control
    /// to be sure we don't break if `delay()` ever changes.
    fn await_frontier(&self, name: &str) -> Stream<G, (D, u64, isize)>;
}

impl<G, D> AwaitFrontier<G, D> for Stream<G, (D, u64, isize)>
where
    G: Scope<Timestamp = u64>,
    D: TimelyData,
{
    // Note: This is mostly a copy of the timely delay() operator without the delaying part.
    fn await_frontier(&self, name: &str) -> Stream<G, (D, u64, isize)> {
        let operator_name = format!("await_frontier({})", name);

        // The values here are Vecs of Vecs. That's how the original timely code does it, to re-use
        // allocations and not have to keep extending a single Vec.
        let mut elements = HashMap::new();

        self.unary_notify(
            Pipeline,
            &operator_name,
            vec![],
            move |input, output, notificator| {
                input.for_each(|time, data| {
                    elements
                        .entry(time.clone())
                        .or_insert_with(|| {
                            notificator.notify_at(time.retain());
                            Vec::new()
                        })
                        .push(data.replace(Vec::new()));
                });

                // For each available notification, send corresponding set.
                notificator.for_each(|time, _, _| {
                    if let Some(mut datas) = elements.remove(&time) {
                        for mut data in datas.drain(..) {
                            output.session(&time).give_vec(&mut data);
                        }
                    } else {
                        panic!("Missing data for time {}", time.time());
                    }
                });
            },
        )
    }
}

/// Extension trait for [`Stream`].
// NOTE: This is not named AllowCompaction because there are already too many things with that
// name...
pub trait AllowPersistCompaction<G: Scope<Timestamp = u64>, D: TimelyData> {
    /// Passes through each element of the stream and allows compaction on the given collection
    /// (the `write` handle) when the input frontier combined with the allowed compaction frontier
    /// (the effective input frontier) advance.
    ///
    /// NOTE: This does not allow compaction right up to the effective input frontier. Instead,
    /// when the frontier advances, we allow compaction up to the latest previous frontier that is
    /// strictly dominated (`PartialOrder::less()`), than the effective input frontier. We do this,
    /// because the input frontier is most likely a proxy for an upstream persist or seal frontier
    /// and we don't want to advance up to the seal, so that we can still distinguish between
    /// updates that are in front of or beyond the seal frontier.
    fn allow_compaction<K, V>(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        allowed_compaction_frontier: Rc<RefCell<Antichain<u64>>>,
    ) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec;
}

impl<G, D> AllowPersistCompaction<G, D> for Stream<G, (D, u64, isize)>
where
    G: Scope<Timestamp = u64>,
    D: TimelyData,
{
    fn allow_compaction<K, V>(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        allowed_compaction_frontier: Rc<RefCell<Antichain<u64>>>,
    ) -> Stream<G, (D, u64, isize)>
    where
        K: Codec,
        V: Codec,
    {
        let operator_name = format!("allow_compaction({})", name);
        let mut op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut data_input = op.new_input(&self, Pipeline);
        let (mut data_output, data_output_stream) = op.new_output();
        let mut data_buffer = Vec::new();

        // We only allow compaction from one worker because doing so from multiple workers could
        // lead to a race conditions where one worker allows compaction up to time `t` while
        // another worker is still trying to write data with timestamps that are not beyond `t`.
        //
        // Upstream operators will only advance their frontier when writes are succesful. With
        // timely progress tracking we are therefore sure that when the frontier advances for
        // worker 0, it has advanced to at least that point for all upstream operators.
        //
        // Alternative solutions would be to "teach" persistence to work with allowing compaction
        // from multiple workers, or to use a non-timely solution for keeping track of outstanding
        // write capabilities.
        let active_operator = self.scope().index() == 0;

        // An activator that allows futures to re-schedule this operator when ready.
        let activator = Arc::new(
            self.scope()
                .sync_activator_for(&op.operator_info().address[..]),
        );

        let mut pending_futures = VecDeque::new();

        // The combination of the input frontier (which is a proxy for how far upstream operators
        // have written and/or sealed) and the allowed compaction frontier (that likely gets
        // updated based on messages from the coordinator).
        let mut effective_input_frontier = Antichain::new();

        // The current compaction frontier.
        let mut compaction_frontier: Antichain<u64> = Antichain::new();

        // A candidate compaction frontier. When we compact, we pick the current effective input
        // frontier as the next candidate. Once the effective input frontier is strictly beyond
        // (that is >, not >=) this candidate, it becomes the compaction frontier.
        //
        // NOTE: We could try and be more clever here, say we could apply the logic that checks if
        // we moved beyond a given frontier only to the input frontier, not the combined/effective
        // frontier, and move through candidates like this. The reasoning behind this is that we
        // don't have to wait until the allowed frontier advances past a given frontier, we only
        // need to be careful not to advance the compaction frontier up to the seal frontier, for
        // which the input frontier is a proxy.
        let mut candidate_compaction_frontier: Antichain<u64> =
            Antichain::from_elem(TimelyTimestamp::minimum());

        op.build_reschedule(move |_capabilities| {
            move |frontiers| {
                let mut data_output = data_output.activate();

                // Pass through all data.
                data_input.for_each(|cap, data| {
                    data.swap(&mut data_buffer);

                    let mut session = data_output.session(&cap);
                    session.give_vec(&mut data_buffer);
                });

                if !active_operator {
                    // We are always complete if we're not the active operator.
                    return false;
                }

                let allowed_compaction_frontier = allowed_compaction_frontier.borrow_mut();
                let input_frontier = frontiers[0].frontier();

                effective_input_frontier.clear();
                effective_input_frontier.extend(input_frontier.iter().cloned());
                effective_input_frontier.extend(allowed_compaction_frontier.iter().cloned());

                if PartialOrder::less_than(
                    &candidate_compaction_frontier,
                    &effective_input_frontier,
                ) {
                    // We can now safely compact to this candidate frontier, because we know the
                    // input frontier combined with the allowed compaction is strictly beyond (that
                    // is >).
                    std::mem::swap(&mut compaction_frontier, &mut candidate_compaction_frontier);

                    // The current combined input frontier becomes the next candidate up to which
                    // we could allow compaction, once it is strictly dominated by the then current
                    // effective input frontier.
                    candidate_compaction_frontier.clear();
                    candidate_compaction_frontier.extend(effective_input_frontier.iter().cloned());

                    tracing::trace!(
                        "In {}, allowing compaction up to {:?}. Effective input frontier: {:?}",
                        &operator_name,
                        compaction_frontier,
                        effective_input_frontier
                    );

                    let fut = write.allow_compaction(compaction_frontier.clone());

                    pending_futures.push_back(CompactionFuture {
                        frontier: compaction_frontier.clone(),
                        fut,
                    });
                }

                // Swing through all pending futures and see if they're ready. Ready futures will
                // invoke the Activator, which will make sure that we arrive here, even when there
                // are no changes in the input frontier or new input.
                let waker = futures_util::task::waker_ref(&activator);
                let mut context = Context::from_waker(&waker);

                while let Some(mut pending_future) = pending_futures.pop_front() {
                    match Pin::new(&mut pending_future.fut).poll(&mut context) {
                        std::task::Poll::Ready(Ok(_)) => {
                            tracing::trace!(
                                "In {}, finished allowing compaction up to {:?}",
                                &operator_name,
                                pending_future.frontier,
                            );
                        }
                        std::task::Poll::Ready(Err(e)) => {
                            // We don't retry or emit an error. Compaction is an optimization that
                            // is not strictly necessary for correctness.
                            tracing::error!("In {}: {}", &operator_name, e);
                        }
                        std::task::Poll::Pending => {
                            // We assume that compaction requests are worked off in order and stop
                            // trying for the first one that is not done. Push the future back to
                            // the front of the queue. We have to do this dance of popping and
                            // pushing because we're modifying the queue while we work on a future.
                            // This prevents us from just getting a reference to the front of the
                            // queue and then popping once we know that a future is done.
                            pending_futures.push_front(pending_future);
                            break;
                        }
                    }
                }

                // When we're done, clear all pending futures to allow speedy shutdown.
                if frontiers[0].frontier().is_empty() {
                    pending_futures.clear();
                }

                !pending_futures.is_empty()
            }
        });

        data_output_stream
    }
}

struct CompactionFuture<F: Future<Output = Result<SeqNo, Error>>> {
    frontier: Antichain<u64>,
    fut: F,
}

/// Extension trait for [`Stream`].
pub trait RetractUnsealed<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Passes through each element of the stream and sends retractions to the given
    /// [`StreamWriteHandle`] for updates that are beyond the given `upper_ts`. In practice,
    /// the `upper_ts` is the lowest timestamp that is sealed across all persisted streams
    /// for a source.
    ///
    /// This does not wait for retractions to be persisted before passing through the data. We do,
    /// however, wait for data to be persisted before allowing the frontier to advance. In other
    /// words, this operator is holding on to capabilities as long as retractions belonging to
    /// their timestamp is not persisted.
    fn retract_unsealed(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        upper_ts: u64,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K, V> RetractUnsealed<G, K, V> for Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Debug,
    V: TimelyData + Codec + Debug,
{
    fn retract_unsealed(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        upper_ts: u64,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
        let (pass_through, to_retract) = self.branch(move |_, (_, t, _)| t >= &upper_ts);

        let (retract_oks, errs) = to_retract
            .map(|(data, time, diff)| (data, time, -diff))
            .persist(&format!("retract_unsealed({})", name), write);

        // Introduce a data-dependency between pass_through, and retract_oks even
        // though in reality they operate on disjoint subsets of data and no true
        // data dependency exists. This way, we ensure that the frontier doesn't
        // advance for downstreams users until after all retractions have been
        // persisted.
        //
        // TODO: we could have done this with fewer operators by concatenating
        // pass_through, to_retract, and retract_oks, as to_retract and retract_oks
        // cancel out. This approach seemed more isolated and safer.
        //
        // TODO: this approach also has the downside that this flat_map needs to
        // go through all of the retractions individually. We could avoid this
        // extra work by having `persist` take an argument that determines whether
        // it should pass through outputs or not.
        let retract_oks = retract_oks.flat_map(|_| None);

        let oks = pass_through.concat(&retract_oks);

        (oks, errs)
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::dataflow::operators::Capture;
    use timely::Config;
    use tokio::runtime::Runtime as AsyncRuntime;

    use crate::error::Error;
    use crate::indexed::{ListenEvent, SnapshotExt};
    use crate::mem::MemRegistry;
    use crate::unreliable::UnreliableHandle;

    use super::*;

    #[test]
    fn persist() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;
        timely::execute_directly(move |worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load("1");
                let mut input = Handle::new();
                let (ok_stream, _) = input.to_stream(scope).persist("test", write);
                let probe = ok_stream.probe();
                (input, probe)
            });
            for i in 1..=5 {
                input.send(((i.to_string(), ()), i, 1));
            }
            input.advance_to(6);
            while probe.less_than(&6) {
                worker.step();
            }
        });

        let expected = vec![
            (("1".to_string(), ()), 1, 1),
            (("2".to_string(), ()), 2, 1),
            (("3".to_string(), ()), 3, 1),
            (("4".to_string(), ()), 4, 1),
            (("5".to_string(), ()), 5, 1),
        ];

        let p = registry.runtime_no_reentrance()?;
        let (_write, read) = p.create_or_load("1");
        assert_eq!(read.snapshot()?.read_to_end()?, expected);

        Ok(())
    }

    #[test]
    fn persist_error_stream() -> Result<(), Error> {
        let mut p = MemRegistry::new().runtime_no_reentrance()?;
        let (write, _read) = p.create_or_load::<(), ()>("error_stream");
        p.stop()?;

        let recv = timely::execute_directly(move |worker| {
            let (mut input, probe, err_stream) = worker.dataflow(|scope| {
                let mut input = Handle::new();
                let (_, err_stream) = input.to_stream(scope).persist("test", write);
                let probe = err_stream.probe();
                (input, probe, err_stream.capture())
            });

            input.send((((), ()), 1, 1));
            input.advance_to(1);

            while probe.less_than(&1) {
                worker.step();
            }

            err_stream
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            "In persist(test), error writing data for time 0: runtime shutdown".to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn seal() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;

        timely::execute_directly(move |worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("1");
                let mut input = Handle::new();
                let ok_stream = input.to_stream(scope).seal("test", write);
                let probe = ok_stream.probe();
                (input, probe)
            });
            input.send((((), ()), 1, 1));
            input.advance_to(42);
            while probe.less_than(&42) {
                worker.step();
            }
        });

        let p = registry.runtime_no_reentrance()?;
        let (_write, read) = p.create_or_load::<(), ()>("1");
        assert_eq!(read.snapshot()?.get_seal(), Antichain::from_elem(42));

        Ok(())
    }

    #[test]
    fn seal_frontier_advance_only_on_success() -> Result<(), Error> {
        ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.runtime_unreliable(unreliable.clone())?;

        timely::execute_directly(move |worker| {
            let (mut input, seal_probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("primary");
                let mut input = Handle::new();
                let stream = input.to_stream(scope);

                let sealed_stream = stream.seal("test", write);

                let seal_probe = sealed_stream.probe();

                (input, seal_probe)
            });

            input.send((((), ()), 0, 1));

            input.advance_to(1);
            while seal_probe.less_than(&1) {
                worker.step();
            }

            unreliable.make_unavailable();

            input.advance_to(2);

            // This is the best we can do. Wait for a bit, and verify that the frontier didn't
            // advance. Of course, we cannot rule out that the frontier might advance on the 11th
            // step, but tests without the fix showed the test to be very unstable on this
            for _i in 0..10 {
                worker.step();
            }
            assert!(seal_probe.less_than(&2));

            // After we make the runtime available again, sealing will work and the frontier will
            // advance.
            unreliable.make_available();
            while seal_probe.less_than(&2) {
                worker.step();
            }
        });

        Ok(())
    }

    /// Test to make sure we handle closing the seal operator correctly and don't
    /// incorrectly process any seal futures after the operator has been closed.
    #[test]
    fn regression_9419_seal_close() -> Result<(), Error> {
        ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.runtime_unreliable(unreliable.clone())?;

        timely::execute_directly(move |worker| {
            let (mut input, mut placeholder, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("primary");
                let mut input = Handle::new();
                let stream = input.to_stream(scope);
                // We need to create a placeholder stream to force the dataflow to stay around
                // even after the actual input has been closed.
                let mut placeholder = Handle::new();
                let placeholder_stream = placeholder.to_stream(scope);

                let sealed_stream = stream.seal("test", write);

                let stream = placeholder_stream.concat(&sealed_stream);
                let probe = stream.probe();

                (input, placeholder, probe)
            });

            // Send data here mostly to avoid having to dictate types to the
            // compiler.
            input.send((((), ()), 0, 1));
            placeholder.send((((), ()), 0, 1));

            placeholder.advance_to(1);
            unreliable.make_unavailable();

            // Advance the frontier while persist is unavailable in order to force
            // the seal operator into a retry loop with this seal operation.
            input.advance_to(1);

            // Allow the operator to submit the seal request.
            worker.step();

            // We close the input, which will make the operator drop all its capabilities. The
            // operator still has a pending seal request, so it will not be shut down.
            input.close();
            // This will make the seal request succeed. If the operator tried to downgrade the (now
            // nonexistent) capabilities, this would fail.
            unreliable.make_available();

            // Once input has been closed, the frontier can safely advance without
            // it.
            placeholder.advance_to(2);
            while probe.less_than(&2) {
                worker.step();
            }
        });

        Ok(())
    }

    #[test]
    fn conditional_seal() -> Result<(), Error> {
        ore::test::init_logging();
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;

        // Setup listens for both collections and record seal events. Afterwards, we will verify
        // that we get the expected seals, in the right order.
        let (_write, primary_read) = p.create_or_load::<(), ()>("primary");
        let (_write, condition_read) = p.create_or_load::<(), ()>("condition");

        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        enum Sealed {
            Primary(u64),
            Condition(u64),
        }

        let (primary_listen_tx, primary_listen_rx) = crossbeam_channel::unbounded();
        primary_read.listen(primary_listen_tx)?;

        let (condition_listen_tx, condition_listen_rx) = crossbeam_channel::unbounded();
        condition_read.listen(condition_listen_tx)?;
        let (listen_tx, listen_rx) = crossbeam_channel::unbounded();

        let async_runtime = AsyncRuntime::new()?;
        let listener_handle = async_runtime.spawn(async move {
            let mut num_channels_closed = 0;
            loop {
                crossbeam_channel::select! {
                    recv(primary_listen_rx) -> msg => {
                        match msg {
                            Ok(ListenEvent::Sealed(ts)) => {
                                let _ = listen_tx.send(Sealed::Primary(ts));
                            }
                            Ok(ListenEvent::Records(_)) => (),
                            Err(crossbeam_channel::RecvError) => {
                                num_channels_closed += 1;
                            }
                        }
                    }
                    recv(condition_listen_rx) -> msg => {
                        match msg {
                            Ok(ListenEvent::Sealed(ts)) => {
                                let _ = listen_tx.send(Sealed::Condition(ts));
                            }
                            Ok(ListenEvent::Records(_)) => (),
                            Err(crossbeam_channel::RecvError) => {
                                num_channels_closed += 1;
                            }
                        }
                    }
                };

                if num_channels_closed == 2 {
                    break;
                }
            }
        });

        let mut p_clone = p.clone();
        timely::execute_directly(move |worker| {
            let (mut primary_input, mut condition_input, seal_probe) = worker.dataflow(|scope| {
                let (primary_write, _read) = p.create_or_load::<(), ()>("primary");
                let (condition_write, _read) = p.create_or_load::<(), ()>("condition");
                let mut primary_input: Handle<u64, ((), u64, isize)> = Handle::new();
                let mut condition_input = Handle::new();
                let primary_stream = primary_input.to_stream(scope);
                let condition_stream = condition_input.to_stream(scope);

                let sealed_stream = primary_stream.conditional_seal(
                    "test",
                    &condition_stream,
                    primary_write,
                    condition_write,
                );

                let seal_probe = sealed_stream.probe();

                (primary_input, condition_input, seal_probe)
            });

            // Only send data on the condition input, not on the primary input. This simulates the
            // case where our primary input never sees any data.
            condition_input.send((((), ()), 0, 1));

            primary_input.advance_to(1);

            condition_input.advance_to(1);
            while seal_probe.less_than(&1) {
                worker.step();
            }

            // Pull primary input to 3 already. We're still expecting a seal at 2 for primary,
            // though, when condition advances to 2.
            primary_input.advance_to(3);

            condition_input.advance_to(2);
            while seal_probe.less_than(&2) {
                worker.step();
            }

            condition_input.advance_to(3);
            while seal_probe.less_than(&3) {
                worker.step();
            }

            // Advance conditional input ahead of the primary.
            condition_input.advance_to(4);

            // Give the worker a chance to process the work. We can't use the existing
            // probe here, because only the conditional input gets sealed. Ideally, we
            // would be able to insert a probe within the conditional_seal operator
            // itself but that's not possible at the moment.
            for _ in 1..10 {
                worker.step();
            }
        });

        // Stop the runtime so the listener task can exit.
        p_clone.stop()?;
        if let Err(e) = block_on(listener_handle) {
            return Err(Error::from(e.to_string()));
        }
        let actual_seals: Vec<_> = listen_rx.try_iter().collect();

        // Assert that:
        //  a) We don't seal primary when condition has not sufficiently advanced.
        //  b) Condition is sealed before primary for the same timestamp OR
        //     primary and condition got swapped when reading from two channels and a
        //     primary seal at t is just before the condition seal at a time t' >= t.
        //  c) We seal up, even when never receiving any data.
        //  d) Seals happen in timestamp order.
        //
        // We cannot assert a specific seal ordering because the order is not deterministic.

        let mut current_condition_seal = 0;
        let mut current_primary_seal = 0;
        let mut condition_seals = vec![];
        let mut primary_seals = vec![];
        let mut seal_pairs = vec![];

        // Chunk up the list of seals into overlapping pairs so we can easily find
        // the next seal.
        for i in 0..actual_seals.len() - 1 {
            seal_pairs.push((actual_seals[i], Some(actual_seals[i + 1])));
        }

        seal_pairs.push((actual_seals[actual_seals.len() - 1], None));

        for (seal, optional_next) in seal_pairs {
            match seal {
                Sealed::Condition(ts) => {
                    assert!(ts >= current_condition_seal);
                    current_condition_seal = ts;
                    condition_seals.push(ts);
                }
                Sealed::Primary(ts) => {
                    assert!(ts >= current_primary_seal);
                    if let Some(Sealed::Condition(next_condition_seal)) = optional_next {
                        assert!(ts <= next_condition_seal);
                    } else {
                        assert!(ts <= current_condition_seal);
                    };
                    current_primary_seal = ts;
                    primary_seals.push(ts);
                }
            }
        }

        // Check that the seal values for each collection are exactly what
        // we expect.
        assert_eq!(condition_seals, vec![1, 2, 3, 4]);
        assert_eq!(primary_seals, vec![1, 2, 3]);

        Ok(())
    }

    #[test]
    fn conditional_seal_frontier_advance_only_on_success() -> Result<(), Error> {
        ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.runtime_unreliable(unreliable.clone())?;

        timely::execute_directly(move |worker| {
            let (mut primary_input, mut condition_input, seal_probe) = worker.dataflow(|scope| {
                let (primary_write, _read) = p.create_or_load::<(), ()>("primary");
                let (condition_write, _read) = p.create_or_load::<(), ()>("condition");
                let mut primary_input: Handle<u64, ((), u64, isize)> = Handle::new();
                let mut condition_input = Handle::new();
                let primary_stream = primary_input.to_stream(scope);
                let condition_stream = condition_input.to_stream(scope);

                let sealed_stream = primary_stream.conditional_seal(
                    "test",
                    &condition_stream,
                    primary_write,
                    condition_write,
                );

                let seal_probe = sealed_stream.probe();

                (primary_input, condition_input, seal_probe)
            });

            condition_input.send((((), ()), 0, 1));

            primary_input.advance_to(1);
            condition_input.advance_to(1);
            while seal_probe.less_than(&1) {
                worker.step();
            }

            unreliable.make_unavailable();

            primary_input.advance_to(2);
            condition_input.advance_to(2);

            // This is the best we can do. Wait for a bit, and verify that the frontier didn't
            // advance. Of course, we cannot rule out that the frontier might advance on the 11th
            // step, but tests without the fix showed the test to be very unstable on this
            for _i in 0..10 {
                worker.step();
            }
            assert!(seal_probe.less_than(&2));

            // After we make the runtime available again, sealing will work and the frontier will
            // advance.
            unreliable.make_available();
            while seal_probe.less_than(&2) {
                worker.step();
            }
        });

        Ok(())
    }

    // Test using multiple workers and ensure that `conditional_seal()` doesn't block the
    // frontier for non-active seal operators.
    //
    // A failure in this test would manifest as indefinite hanging of the test, we never see the
    // frontier advance as we expect to.
    #[test]
    fn conditional_seal_multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let guards = timely::execute(Config::process(3), move |worker| {
            let (mut primary_input, mut condition_input, seal_probe) = worker.dataflow(|scope| {
                let (primary_write, _read) = p.create_or_load::<(), ()>("primary");
                let (condition_write, _read) = p.create_or_load::<(), ()>("condition");
                let mut primary_input: Handle<u64, ((), u64, isize)> = Handle::new();
                let mut condition_input: Handle<u64, ((), u64, isize)> = Handle::new();
                let primary_stream = primary_input.to_stream(scope);
                let condition_stream = condition_input.to_stream(scope);

                let sealed_stream = primary_stream.conditional_seal(
                    "test",
                    &condition_stream,
                    primary_write,
                    condition_write,
                );

                let seal_probe = sealed_stream.probe();

                (primary_input, condition_input, seal_probe)
            });

            primary_input.advance_to(42);
            condition_input.advance_to(42);
            while seal_probe.less_than(&42) {
                worker.step();
            }
        })?;

        let timely_result: Result<Vec<_>, _> = guards.join().into_iter().collect();
        timely_result.expect("timely workers failed");

        Ok(())
    }

    #[test]
    fn retract_unsealed() -> Result<(), Error> {
        ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        timely::execute_directly(move |worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("test");
                let mut input = Handle::new();
                let stream = input.to_stream(scope);

                let (output, _) = stream.retract_unsealed("test_retract_unsealed", write, 5);

                let probe = output.probe();

                (input, probe)
            });

            for i in 0..=6 {
                input.send((((), ()), i, 1));
            }

            // Note that these were all sent at the timely time of 0.
            input.advance_to(1);
            while probe.less_than(&1) {
                worker.step();
            }
        });

        let expected = vec![(((), ()), 5, -1), (((), ()), 6, -1)];

        let p = registry.runtime_no_reentrance()?;
        let (_write, read) = p.create_or_load("test");
        assert_eq!(read.snapshot()?.read_to_end()?, expected);

        Ok(())
    }

    // Tests the case where the input frontier is beyond the allowed frontier that we get from
    // outside.
    #[test]
    fn allow_compaction_input_beyond_allowed() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;

        timely::execute_directly(move |worker| {
            let allowed_compaction = Rc::new(RefCell::new(Antichain::from_elem(3)));

            let (mut input, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("1");
                let mut input = Handle::new();

                let ok_stream =
                    input
                        .to_stream(scope)
                        .allow_compaction("test", write, allowed_compaction);

                let probe = ok_stream.probe();
                (input, probe)
            });

            input.send((((), ()), 1, 1));

            input.advance_to(2);
            while probe.less_than(&2) {
                worker.step();
            }

            input.advance_to(10);
            while probe.less_than(&10) {
                worker.step();
            }
        });

        let p = registry.runtime_no_reentrance()?;
        let description = p.get_description("1")?;

        // The operator will allow compaction up to the last frontier before the effective
        // frontier. We keep `allowed_compaction` at [3], so in our setup the last frontier before
        // that is [2].
        assert_eq!(*description.since(), Antichain::from_elem(2));

        Ok(())
    }

    // Tests the case where the allowed frontier is beyond the input frontier
    #[test]
    fn allow_compaction_allowed_beyond_input() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;

        timely::execute_directly(move |worker| {
            let allowed_compaction = Rc::new(RefCell::new(Antichain::from_elem(10)));

            let (mut input, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("1");
                let mut input = Handle::new();

                let ok_stream =
                    input
                        .to_stream(scope)
                        .allow_compaction("test", write, allowed_compaction);

                let probe = ok_stream.probe();
                (input, probe)
            });

            input.send((((), ()), 1, 1));

            input.advance_to(2);
            while probe.less_than(&2) {
                worker.step();
            }

            input.advance_to(3);
            while probe.less_than(&3) {
                worker.step();
            }

            // The operator will allow compaction up to the last frontier before the effective
            // frontier. We keep `allowed_compaction` at [3], so in our setup the last frontier before
            // that is [2].
            // let p = registry.runtime_no_reentrance()?;
            let description = p.get_description("1").unwrap();
            assert_eq!(*description.since(), Antichain::from_elem(2));
        });

        let p = registry.runtime_no_reentrance()?;
        let description = p.get_description("1")?;

        // Closing the operator advances the input frontier to the empty frontier (aka "infinity"),
        // we are therefore allowed to advanec to [3], which was the last valid frontier before
        // that.
        assert_eq!(*description.since(), Antichain::from_elem(3));

        Ok(())
    }
}
