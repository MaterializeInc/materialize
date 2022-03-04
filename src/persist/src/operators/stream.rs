// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Modular Timely Dataflow operators that can persist and seal updates in streams.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use mz_persist_types::Codec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::{Branch, Concat, Map};
use timely::dataflow::ProbeHandle;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::Data as TimelyData;
use timely::PartialOrder;
use tracing::{error, trace};

use crate::client::MultiWriteHandle;
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
    /// Use this together with [`seal`](Seal::seal) and
    /// [`await_frontier`](AwaitFrontier::await_frontier) if you want to make sure that data only
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
    ) -> (Stream<G, ((K, V), u64, i64)>, Stream<G, (String, u64, i64)>);
}

impl<G, K, V> Persist<G, K, V> for Stream<G, ((K, V), u64, i64)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec,
    V: TimelyData + Codec,
{
    fn persist(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (Stream<G, ((K, V), u64, i64)>, Stream<G, (String, u64, i64)>) {
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
                    // example, by seal() operators and await_frontier().
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
                                    trace!(
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
                                    error!("{}", error);

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
    /// Passes through each element of the stream and calls
    /// [`seal_all`](MultiWriteHandle::seal_all) on the given [`MultiWriteHandle`] when the
    /// combined input frontier advances advances. The combined input frontier is derived by
    /// combining the frontier of the input with the optional [`ProbeHandles`](ProbeHandle).
    ///
    /// This does not wait for the seal to succeed before passing through the data. We do, however,
    /// wait for the seal to be successful before allowing the frontier to advance. In other words,
    /// this operator is holding on to capabilities as long as seals corresponding to their
    /// timestamp are not done.
    fn seal(
        &self,
        name: &str,
        additional_frontiers: Vec<ProbeHandle<u64>>,
        write: MultiWriteHandle,
    ) -> Stream<G, (D, u64, i64)>;
}

impl<G, D> Seal<G, D> for Stream<G, (D, u64, i64)>
where
    G: Scope<Timestamp = u64>,
    D: TimelyData,
{
    fn seal(
        &self,
        name: &str,
        additional_frontiers: Vec<ProbeHandle<u64>>,
        write: MultiWriteHandle,
    ) -> Stream<G, (D, u64, i64)> {
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
        let sync_activator = Arc::new(
            self.scope()
                .sync_activator_for(&seal_op.operator_info().address[..]),
        );

        // A cheaper (non-thread-safe) activator that we use to re-schedule ourselves in case the
        // frontier as reported by the probe handles has not advanced as far as the input frontier.
        let activator = self
            .scope()
            .activator_for(&seal_op.operator_info().address[..]);

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

                for probe_frontier in additional_frontiers.iter() {
                    probe_frontier.with_frontier(|frontier| {
                        new_input_frontier.extend(frontier.iter().cloned())
                    });
                }

                if PartialOrder::less_than(&new_input_frontier.borrow(), &frontiers[0].frontier()) {
                    // Immediately schedule again, to see if the probe frontiers to catch up.
                    activator.activate();
                    // TODO: This is essentially as busy loop. We could use `activate_after()`, or
                    // not use activate at all and rely on the fact that the frontiers usually
                    // advance to the same times because they come from a common source.
                }

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
                        trace!(
                            "In {}, sealing collection up to {}...",
                            &operator_name,
                            frontier_element,
                        );

                        let future = write.seal_all(*frontier_element);

                        pending_futures.push_back(SealFuture {
                            time: *frontier_element,
                            future,
                        });
                    }
                }

                // Swing through all pending futures and see if they're ready. Ready futures will
                // invoke the Activator, which will make sure that we arrive here, even when there
                // are no changes in the input frontier or new input.
                let waker = futures_util::task::waker_ref(&sync_activator);
                let mut context = Context::from_waker(&waker);

                while let Some(mut pending_future) = pending_futures.pop_front() {
                    match Pin::new(&mut pending_future.future).poll(&mut context) {
                        std::task::Poll::Ready(Ok(_)) => {
                            trace!(
                                "In {}, finished sealing collection up to {}",
                                &operator_name,
                                pending_future.time,
                            );

                            // Explicitly downgrade the capability to the new time.
                            cap_set.downgrade(Some(pending_future.time));
                        }
                        std::task::Poll::Ready(Err(e)) => {
                            trace!(
                                "Error sealing {} up to {}: {}",
                                &operator_name,
                                pending_future.time,
                                e
                            );

                            // Only retry this seal if there is no other pending seal at a time >=
                            // this seal's time.
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
                                trace!("Adding seal to queue again: {}", pending_future.time);

                                let future = write.seal_all(pending_future.time);
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
                if frontiers[0].is_empty() {
                    cap_set.downgrade(input_frontier.iter());
                    pending_futures.clear();
                }

                !pending_futures.is_empty()
            }
        });

        data_output_stream
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
    fn await_frontier(&self, name: &str) -> Stream<G, (D, u64, i64)>;
}

impl<G, D> AwaitFrontier<G, D> for Stream<G, (D, u64, i64)>
where
    G: Scope<Timestamp = u64>,
    D: TimelyData,
{
    // Note: This is mostly a copy of the timely delay() operator without the delaying part.
    fn await_frontier(&self, name: &str) -> Stream<G, (D, u64, i64)> {
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
    ) -> (Stream<G, ((K, V), u64, i64)>, Stream<G, (String, u64, i64)>);
}

impl<G, K, V> RetractUnsealed<G, K, V> for Stream<G, ((K, V), u64, i64)>
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
    ) -> (Stream<G, ((K, V), u64, i64)>, Stream<G, (String, u64, i64)>) {
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
    use crate::indexed::ListenEvent;
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
                let write = MultiWriteHandle::new(&write);
                let mut input = Handle::new();
                let ok_stream = input.to_stream(scope).seal("test", vec![], write);
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
        mz_ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.runtime_unreliable(unreliable.clone())?;

        timely::execute_directly(move |worker| {
            let (mut input, seal_probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("primary");
                let write = MultiWriteHandle::new(&write);
                let mut input = Handle::new();
                let stream = input.to_stream(scope);

                let sealed_stream = stream.seal("test", vec![], write);

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
        mz_ore::test::init_logging();
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.runtime_unreliable(unreliable.clone())?;

        timely::execute_directly(move |worker| {
            let (mut input, mut placeholder, probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("primary");
                let write = MultiWriteHandle::new(&write);
                let mut input = Handle::new();
                let stream = input.to_stream(scope);
                // We need to create a placeholder stream to force the dataflow to stay around
                // even after the actual input has been closed.
                let mut placeholder = Handle::new();
                let placeholder_stream = placeholder.to_stream(scope);

                let sealed_stream = stream.seal("test", vec![], write);

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

    // TODO: I could remove all the complicated channel logic, the multiple downgrades and
    // interleaved stepping, because I don't think we can verify ordering.
    //
    // Verify that seal works correctly when sealing multiple streams and when using probe handles
    // to further restrict the input frontier.
    //
    // NOTE: We know by construction of the MultiWriteHandle that we seal atomically, and we cannot
    // realistically assert on the order we get from the channels.
    #[test]
    fn seal_multiple_streams() -> Result<(), Error> {
        mz_ore::test::init_logging();
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
        #[allow(clippy::disallowed_methods)]
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
                let mut primary_input: Handle<u64, ((), u64, i64)> = Handle::new();
                let mut condition_input = Handle::new();
                let primary_stream = primary_input.to_stream(scope);
                let condition_stream = condition_input.to_stream(scope);

                let mut multi_write = MultiWriteHandle::new(&primary_write);
                multi_write
                    .add_stream(&condition_write)
                    .expect("client known from same runtime");

                let sealed_stream =
                    primary_stream.seal("test", vec![condition_stream.probe()], multi_write);

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
            for _ in 0..10 {
                worker.step();
            }

            // Hard shut down the dataflows. We have to do this because execute_directly
            // will otherwise close the primary and condition inputs once this closure
            // completes and then call step_or_park until no more dataflows remain.
            //
            // Unfortunately, this approach is a touch brittle for the conditional
            // seal operator and the desired behavior of this test, as there is no
            // guarantee that each operator will observe both inputs closing at
            // the same time. In particular, the primary seal operator can observe
            // that the primary input closed before the condition input, and
            // inadvertently seal the primary to its frontier (4), even though
            // the primary never advanced to 4.
            //
            // Sidestep this problem by directly closing the dataflow.
            let dataflows = worker.installed_dataflows();
            for dataflow in dataflows {
                worker.drop_dataflow(dataflow);
            }
        });

        // Stop the runtime so the listener task can exit.
        p_clone.stop()?;
        if let Err(e) = block_on(listener_handle) {
            return Err(Error::from(e.to_string()));
        }
        let actual_seals: Vec<_> = listen_rx.try_iter().collect();

        // Assert that:
        //  a) We seal up, even when never receiving any data.
        //  b) Seals happen in timestamp order.
        //  c) If we seal any of the involved streams to `t` we must seal all streams to `t`.
        //
        // We cannot assert a specific seal ordering because the order is not deterministic.

        let mut condition_seals = vec![];
        let mut primary_seals = vec![];

        let mut latest_condition_seal = 0;
        let mut latest_primary_seal = 0;

        for seal in actual_seals.iter() {
            match seal {
                Sealed::Primary(ts) => {
                    primary_seals.push(*ts);
                    assert!(*ts >= latest_primary_seal);
                    latest_primary_seal = *ts;
                    assert!(actual_seals.contains(&Sealed::Condition(*ts)));
                }
                Sealed::Condition(ts) => {
                    condition_seals.push(*ts);
                    assert!(*ts >= latest_condition_seal);
                    latest_condition_seal = *ts;
                    assert!(actual_seals.contains(&Sealed::Primary(*ts)));
                }
            }
        }

        // Check that the seal values for each collection are exactly what
        // we expect.
        assert_eq!(condition_seals, vec![1, 2, 3]);
        assert_eq!(primary_seals, vec![1, 2, 3]);

        Ok(())
    }

    // Test using multiple workers and ensure that `seal()` doesn't block the frontier for
    // non-active seal operators.
    //
    // A failure in this test would manifest as indefinite hanging of the test, we never see the
    // frontier advance as we expect to.
    #[test]
    fn seal_multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let guards = timely::execute(Config::process(3), move |worker| {
            let (mut input, seal_probe) = worker.dataflow(|scope| {
                let (write, _read) = p.create_or_load::<(), ()>("primary");
                let mut input: Handle<u64, ((), u64, i64)> = Handle::new();
                let stream = input.to_stream(scope);

                let multi_write = MultiWriteHandle::new(&write);

                let sealed_stream = stream.seal("test", vec![], multi_write);

                let seal_probe = sealed_stream.probe();

                (input, seal_probe)
            });

            input.advance_to(42);
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
        mz_ore::test::init_logging();
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
}
