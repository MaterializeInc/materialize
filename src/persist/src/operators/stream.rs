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

use persist_types::Codec;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
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
pub trait Seal<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Passes through each element of the stream and seals the given collection (the `write`
    /// handle) when the input frontier advances.
    ///
    /// This does not wait for the seal to succeed before passing through the data. We do, however,
    /// wait for the seal to be successful before allowing the frontier to advance. In other words,
    /// this operator is holding on to capabilities as long as seals corresponding to their
    /// timestamp are not done.
    fn seal(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K, V> Seal<G, K, V> for Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec,
    V: TimelyData + Codec,
{
    fn seal(
        &self,
        name: &str,
        write: StreamWriteHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
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
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::dataflow::operators::Capture;

    use crate::error::Error;
    use crate::indexed::SnapshotExt;
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
                let (write, _read) = p.create_or_load("1").unwrap();
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
}
