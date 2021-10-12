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
use std::fmt::Debug;

use persist_types::Codec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::OkErr;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::{Data as TimelyData, PartialOrder};

use crate::indexed::runtime::StreamWriteHandle;

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
        let operator_name = format!("persist({})", name);
        let mut persist_op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut input = persist_op.new_input(&self, Pipeline);

        let (mut data_output, data_output_stream) = persist_op.new_output();
        let (mut error_output, error_output_stream) = persist_op.new_output();

        let mut buffer = Vec::new();
        let mut write_futures = HashMap::new();
        let mut input_frontier =
            Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());
        let error_output_port = error_output_stream.name().port;

        persist_op.build(move |_capabilities| {
            move |frontiers| {
                let mut data_output = data_output.activate();
                let mut error_output = error_output.activate();

                // Write out everything and forward, keeping the write futures.
                input.for_each(|cap, data| {
                    data.swap(&mut buffer);

                    let write_future = write.write(buffer.iter().as_ref());

                    let mut session = data_output.session(&cap);
                    session.give_vec(&mut buffer);

                    let write_futures = &mut write_futures
                        .entry(cap.retain_for_output(error_output_port))
                        .or_insert_with(|| Vec::new());
                    write_futures.push(write_future);
                });

                // Block on outstanding writes when the input frontier advances.
                // This way, when the downstream frontier advances, we know that all writes that
                // are before it are done.
                let new_input_frontier = frontiers[0].frontier();
                let progress =
                    !PartialOrder::less_equal(&new_input_frontier, &input_frontier.borrow());

                if !progress {
                    return;
                }

                input_frontier.clear();
                input_frontier.extend(new_input_frontier.into_iter().cloned());
                let contained_times: Vec<_> = write_futures
                    .keys()
                    .filter(|time| !input_frontier.less_equal(time.time()))
                    .cloned()
                    .collect();

                // TODO: Even more pipelining: the operator should yield when the futures
                // are not ready and re-schedule itself using an `Activator`. As it is, we
                // have a synchronization barrier once every second (default timestamping
                // interval).
                // TODO: Potentially move the logic for determining when futures are ready
                // and frontier management into a struct/impl.
                for time in contained_times {
                    let write_futures = write_futures.remove(&time).expect("missing futures");

                    log::trace!(
                        "In {} waiting on write futures for time: {}",
                        &operator_name,
                        time.time()
                    );
                    for future in write_futures {
                        if let Err(e) = future.recv() {
                            let mut session = error_output.session(&time);
                            // TODO: make error retractable? Probably not...
                            session.give((e.to_string(), *time.time(), 1));
                        }
                    }
                    log::trace!(
                        "In {} finished write futures for time: {}",
                        &operator_name,
                        time.time()
                    );
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
    fn seal<K, V>(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (Stream<G, (D, u64, isize)>, Stream<G, (String, u64, isize)>)
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
    ) -> (Stream<G, (D, u64, isize)>, Stream<G, (String, u64, isize)>)
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
    fn seal<K, V>(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (Stream<G, (D, u64, isize)>, Stream<G, (String, u64, isize)>)
    where
        K: Codec,
        V: Codec,
    {
        let operator_name = format!("seal({})", name);
        let mut seal_op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut data_input = seal_op.new_input(&self, Pipeline);

        let (mut data_output, data_output_stream) = seal_op.new_output();
        let (mut error_output, error_output_stream) = seal_op.new_output();

        let mut data_buffer = Vec::new();
        let mut input_frontier =
            Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());
        let mut capabilities = Antichain::<Capability<u64>>::new();
        let error_output_port = error_output_stream.name().port;

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

        seal_op.build(move |_capabilities| {
            move |frontiers| {
                let mut data_output = data_output.activate();
                let mut error_output = error_output.activate();

                // Pass through all data.
                data_input.for_each(|cap, data| {
                    data.swap(&mut data_buffer);

                    let mut session = data_output.session(&cap);
                    session.give_vec(&mut data_buffer);

                    // We only need capabilities for reporting errors, which we only need to do
                    // when we're the active operator.
                    if active_seal_operator {
                        capabilities.insert(cap.retain_for_output(error_output_port));
                    }
                });

                if !active_seal_operator {
                    return;
                }

                // Seal if/when the frontier advances.
                let new_input_frontier = frontiers[0].frontier();
                let progress =
                    !PartialOrder::less_equal(&new_input_frontier, &input_frontier.borrow());

                if !progress {
                    return;
                }

                // Only try and seal if we have seen some data. Otherwise, we wouldn't have
                // a capability that allows us to emit errors.
                if let Some(err_cap) = capabilities.get(0) {
                    for frontier_element in new_input_frontier.iter() {
                        // Only seal if this element of the new input frontier truly
                        // represents progress. With Antichain<u64>, this will always be
                        // the case, but antichains of types with a different partial order
                        // can have frontier progress and have some elements that don't
                        // represent progress.
                        if !input_frontier.less_than(frontier_element) {
                            continue;
                        }

                        log::trace!("Sealing {} up to {}", &operator_name, frontier_element);

                        // TODO: Don't block on the seal. Instead, we should yield from the
                        // operator and/or find some other way to wait for the seal to succeed.
                        let result = write.seal(*frontier_element).recv();
                        if let Err(e) = result {
                            log::error!(
                                "Error sealing {} up to {}: {:?}",
                                &operator_name,
                                frontier_element,
                                e
                            );

                            let mut session = error_output.session(err_cap);
                            // TODO: Make error retractable? Probably not...
                            session.give((e.to_string(), *err_cap.time(), 1));
                        }
                    }
                }

                input_frontier.clear();
                input_frontier.extend(new_input_frontier.into_iter().cloned());

                // If we didn't yet receive any data we won't have capabilities yet.
                if !capabilities.is_empty() {
                    // Try and maintain the least amount of capabilities. In our case, where
                    // the timestamp is u64, this means we only ever keep one capability
                    // because u64 has a total order and the input frontier therefore only ever
                    // contains one element.
                    //
                    // This solution is very generic, though, and will work for the case where
                    // we don't use u64 as the timestamp.
                    let mut new_capabilities = Antichain::new();
                    for time in input_frontier.iter() {
                        if let Some(capability) = capabilities
                            .elements()
                            .iter()
                            .find(|c| c.time().less_equal(time))
                        {
                            new_capabilities.insert(capability.delayed(time));
                        } else {
                            panic!("failed to find capability");
                        }
                    }

                    capabilities = new_capabilities;
                }
            }
        });

        (data_output_stream, error_output_stream)
    }

    fn conditional_seal<D2: TimelyData, K, V, K2, V2>(
        &self,
        name: &str,
        condition_input: &Stream<G, (D2, u64, isize)>,
        primary_write: StreamWriteHandle<K, V>,
        condition_write: StreamWriteHandle<K2, V2>,
    ) -> (Stream<G, (D, u64, isize)>, Stream<G, (String, u64, isize)>)
    where
        K: Codec,
        V: Codec,
        K2: TimelyData + Codec,
        V2: TimelyData + Codec,
    {
        let operator_name = format!("conditional_seal({})", name);
        let mut seal_op = OperatorBuilder::new(operator_name.clone(), self.scope());

        let mut primary_data_input = seal_op.new_input(&self, Pipeline);
        let mut condition_data_input = seal_op.new_input(condition_input, Pipeline);

        let (mut data_output, data_output_stream) = seal_op.new_output();

        let mut primary_data_buffer = Vec::new();
        let mut condition_data_buffer = Vec::new();

        let mut input_frontier =
            Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

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

        seal_op.build(move |mut capabilities| {
            let mut cap = if active_seal_operator {
                let cap = capabilities.pop().expect("missing capability");
                Some(cap)
            } else {
                None
            };

            move |frontiers| {
                let mut data_output = data_output.activate();

                // Pass through all data.
                primary_data_input.for_each(|cap, data| {
                    data.swap(&mut primary_data_buffer);

                    let as_result = primary_data_buffer.drain(..).map(|update| Ok(update));

                    let mut session = data_output.session(&cap);
                    session.give_iterator(as_result);
                });

                // Consume condition input data but throw it away. We only use this
                // input to track the frontier (to know how far we're sealed up).
                // TODO: There should be a better way for doing this, maybe?
                condition_data_input.for_each(|_cap, data| {
                    data.swap(&mut condition_data_buffer);
                    condition_data_buffer.drain(..);
                });

                if !active_seal_operator {
                    return;
                }

                let mut new_input_frontier = Antichain::new();
                new_input_frontier.extend(frontiers[0].frontier().into_iter().cloned());
                new_input_frontier.extend(frontiers[1].frontier().into_iter().cloned());

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
                        // First, seal up the condition input.
                        log::trace!(
                            "In {}, sealing condition input up to {}...",
                            &operator_name,
                            frontier_element,
                        );

                        // TODO: Don't block on the seal. Instead, we should yield from the
                        // operator and/or find some other way to wait for the seal to succeed.
                        let result = condition_write.seal(*frontier_element).recv();

                        log::trace!(
                            "In {}, finished sealing condition input up to {}",
                            &operator_name,
                            frontier_element,
                        );

                        if let Err(e) = result {
                            let cap = cap.as_mut().expect("missing capability");
                            let mut session = data_output.session(cap);
                            log::error!(
                                "Error sealing {} (condition) up to {}: {:?}",
                                &operator_name,
                                frontier_element,
                                e
                            );
                            // TODO: make error retractable? Probably not...
                            session.give(Err((e.to_string(), *cap.time(), 1)));

                            // Don't seal the primary collection if sealing the condition
                            // collection failed.
                            continue;
                        }

                        // Then, seal up the primary input.
                        log::trace!(
                            "In {}, sealing primary input up to {}...",
                            &operator_name,
                            frontier_element,
                        );

                        // TODO: Don't block on the seal. Instead, we should yield from the
                        // operator and/or find some other way to wait for the seal to succeed.
                        let result = primary_write.seal(*frontier_element).recv();

                        log::trace!(
                            "In {}, finished sealing primary input up to {}",
                            &operator_name,
                            frontier_element,
                        );

                        if let Err(e) = result {
                            let cap = cap.as_mut().expect("missing capability");
                            let mut session = data_output.session(cap);
                            log::error!(
                                "Error sealing {} (primary) up to {}: {:?}",
                                &operator_name,
                                frontier_element,
                                e
                            );
                            // TODO: make error retractable? Probably not...
                            session.give(Err((e.to_string(), *cap.time(), 1)));
                        }
                    }
                }

                input_frontier.clone_from(&mut new_input_frontier);

                for time in input_frontier.iter() {
                    let cap = cap.as_mut().expect("missing capability");
                    if let Err(e) = cap.try_downgrade(time) {
                        // With Antichain<u64> as frontier, this is guaranteed to succeed. If we
                        // have multi-dimensional capabilities in the future we have to get
                        // slightly more clever about managing our capabilities.
                        panic!(
                            "In {}, error downgrading capability {:?} to {}: {:?}",
                            operator_name, cap, time, e
                        );
                    }
                }

                // An empty input frontier signals that we will never receive any input again. We
                // need to drop our capability to ensure that the empty frontier can propagate
                // downstream.
                if input_frontier.is_empty() && cap.is_some() {
                    cap.take().expect("error dropping capability");
                }
            }
        });

        // We use a single, multiplexed output instead of dealing with the hassles of managing
        // capabilities for a regular output and an error output for the seal operator.
        let (data_output_stream, error_output_stream) = data_output_stream.ok_err(|x| x);

        (data_output_stream, error_output_stream)
    }
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
pub trait RetractFutureUpdates<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Passes through each element of the stream and sends retractions to the given
    /// [`StreamWriteHandle`] for updates that are beyond the given `upper_ts`.
    ///
    /// This does wait on writing each retraction before emitting more data (or advancing the
    /// frontier).
    fn filter_and_retract_future_updates(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        upper_ts: u64,
    ) -> Stream<G, ((K, V), u64, isize)>;
}

impl<G, K, V> RetractFutureUpdates<G, K, V> for Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Debug,
    V: TimelyData + Codec + Debug,
{
    // TODO: This operator is not optimized at all. For example, instead of waiting for each write
    // to succeed, we could do the same thing as the persist() operator and batch a bunch of
    // futures and only wait on them once the frontier advances.
    // For now, that should be ok because this only causes restarts to be slightly slower but
    // doesn't affect the steady-state write path.
    fn filter_and_retract_future_updates(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
        upper_ts: u64,
    ) -> Stream<G, ((K, V), u64, isize)> {
        let operator_name = format!("retract_future_updates({})", name);
        let mut persist_op = OperatorBuilder::new(operator_name, self.scope());

        let mut input = persist_op.new_input(&self, Pipeline);

        let (mut data_output, data_output_stream) = persist_op.new_output();

        let mut buffer = Vec::new();

        persist_op.build(move |_capabilities| {
            move |_frontiers| {
                let mut data_output = data_output.activate();

                // Write out everything and forward, keeping the write futures.
                input.for_each(|cap, data| {
                    data.swap(&mut buffer);

                    let mut session = data_output.session(&cap);
                    for update in buffer.drain(..) {
                        if update.1 >= upper_ts {
                            log::trace!(
                                "Update {:?} is beyond upper_ts {}, retracting...",
                                update,
                                upper_ts
                            );
                            let (data, ts, diff) = update;
                            let anti_update = (data, ts, -diff);
                            write
                                .write(&[anti_update])
                                .recv()
                                .expect("error persisting retraction");
                            continue;
                        }
                        session.give(update);
                    }
                });
            }
        });

        data_output_stream
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::dataflow::operators::Capture;

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
                let (write, _read) = p.create_or_load("1").unwrap();
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
        let (_write, read) = p.create_or_load("1")?;
        assert_eq!(read.snapshot()?.read_to_end()?, expected);

        Ok(())
    }

    #[test]
    fn persist_error_stream() -> Result<(), Error> {
        let mut unreliable = UnreliableHandle::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;

        let (write, _read) = p.create_or_load::<(), ()>("error_stream").unwrap();
        unreliable.make_unavailable();

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
            "failed to append to unsealed: unavailable: blob set".to_string(),
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
                let (write, _read) = p.create_or_load::<(), ()>("1").unwrap();
                let mut input = Handle::new();
                let (ok_stream, _) = input.to_stream(scope).seal("test", write);
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
        let (_write, read) = p.create_or_load::<(), ()>("1")?;
        assert_eq!(read.snapshot()?.get_seal(), Antichain::from_elem(42));

        Ok(())
    }

    #[test]
    fn seal_error_stream() -> Result<(), Error> {
        let mut unreliable = UnreliableHandle::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;

        let (write, _read) = p.create_or_load::<(), ()>("error_stream").unwrap();
        unreliable.make_unavailable();

        let recv = timely::execute_directly(move |worker| {
            let (mut input, probe, err_stream) = worker.dataflow(|scope| {
                let mut input = Handle::new();
                let (_, err_stream) = input.to_stream(scope).seal("test", write);
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
            "failed to commit metadata after appending to unsealed: unavailable: blob set"
                .to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn conditional_seal() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let p = registry.runtime_no_reentrance()?;

        // Setup listens for both collections and record seal events. Afterwards, we will verify
        // that we get the expected seals, in the right order.
        let (_write, primary_read) = p.create_or_load::<(), ()>("primary").unwrap();
        let (_write, condition_read) = p.create_or_load::<(), ()>("condition").unwrap();

        #[derive(Debug, PartialEq, Eq)]
        enum Sealed {
            Primary(u64),
            Condition(u64),
        }

        let (listen_tx, listen_rx) = mpsc::channel();

        {
            let listen_tx = listen_tx.clone();
            let listen_fn = Box::new(move |e| {
                match e {
                    ListenEvent::Sealed(t) => listen_tx.send(Sealed::Primary(t)).unwrap(),
                    _ => panic!("unexpected data"),
                };
                ()
            });

            primary_read.listen(listen_fn)?;
        };
        {
            let listen_fn = Box::new(move |e| {
                match e {
                    ListenEvent::Sealed(t) => listen_tx.send(Sealed::Condition(t)).unwrap(),
                    _ => panic!("unexpected data"),
                };
                ()
            });

            condition_read.listen(listen_fn)?;
        };

        timely::execute_directly(move |worker| {
            let (mut primary_input, mut condition_input, primary_probe, condition_probe) = worker
                .dataflow(|scope| {
                    let (primary_write, _read) = p.create_or_load::<(), ()>("primary").unwrap();
                    let (condition_write, _read) = p.create_or_load::<(), ()>("condition").unwrap();
                    let mut primary_input: Handle<u64, ((), u64, isize)> = Handle::new();
                    let mut condition_input = Handle::new();
                    let primary_stream = primary_input.to_stream(scope);
                    let condition_stream = condition_input.to_stream(scope);
                    let (_, _) = primary_stream.conditional_seal(
                        "test",
                        &condition_stream,
                        primary_write,
                        condition_write,
                    );

                    let primary_probe = primary_stream.probe();
                    let condition_probe = condition_stream.probe();

                    (
                        primary_input,
                        condition_input,
                        primary_probe,
                        condition_probe,
                    )
                });

            // Only send data on the condition input, not on the primary input. This simulates the
            // case where our primary input never sees any data.
            condition_input.send((((), ()), 0, 1));

            primary_input.advance_to(1);
            condition_input.advance_to(1);
            while primary_probe.less_than(&1) {
                worker.step();
            }

            // Pull primary input to 3 already. We're still expecting a seal at 2 for primary,
            // though, when condition advances to 2.
            primary_input.advance_to(3);
            while primary_probe.less_than(&3) {
                worker.step();
            }

            condition_input.advance_to(2);
            while condition_probe.less_than(&2) {
                worker.step();
            }

            condition_input.advance_to(3);
            while condition_probe.less_than(&3) {
                worker.step();
            }
        });

        let actual_seals: Vec<_> = listen_rx.try_iter().collect();

        // Assert that:
        //  a) We don't seal primary when condition has not sufficiently advanced.
        //  b) Condition is sealed before primary for the same timestamp.
        //  c) We seal up, even when never receiving any data.
        assert_eq!(
            vec![
                Sealed::Condition(1),
                Sealed::Primary(1),
                Sealed::Condition(2),
                Sealed::Primary(2),
                Sealed::Condition(3),
                Sealed::Primary(3)
            ],
            actual_seals
        );

        Ok(())
    }

    #[test]
    fn conditional_seal_error_stream() -> Result<(), Error> {
        let mut unreliable = UnreliableHandle::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;

        let (primary_write, _read) = p.create_or_load::<(), ()>("primary").unwrap();
        let (condition_write, _read) = p.create_or_load::<(), ()>("condition").unwrap();
        unreliable.make_unavailable();

        let recv = timely::execute_directly(move |worker| {
            let (mut primary_input, mut condition_input, probe, err_stream) =
                worker.dataflow(|scope| {
                    let mut primary_input = Handle::new();
                    let mut condition_input = Handle::new();
                    let primary_stream = primary_input.to_stream(scope);
                    let condition_stream = condition_input.to_stream(scope);

                    let (_, err_stream) = primary_stream.conditional_seal(
                        "test",
                        &condition_stream,
                        primary_write,
                        condition_write,
                    );

                    let probe = err_stream.probe();
                    (primary_input, condition_input, probe, err_stream.capture())
                });

            primary_input.send((((), ()), 0, 1));
            condition_input.send((((), ()), 0, 1));

            primary_input.advance_to(1);
            condition_input.advance_to(1);

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
            "failed to commit metadata after appending to unsealed: unavailable: blob set"
                .to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }
}
