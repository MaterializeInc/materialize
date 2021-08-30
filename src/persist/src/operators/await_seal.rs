// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that passes through its input and blocks the
//! frontier from advancing before the seal frontier on the given monitored
//! persistent collections advances.

// TODO: This seems a bit clunky. It might be easier to just use timely frontier tracking to do the
// same thing. That is, let the "persist" operators only downgrade their frontier once they sealed,
// but still sent data along before that.

// TODO: This operator is "pessimistic", in that it holds data back until the seal frontier
// advances. Another alternative that would work is "optimistic concurrency control", where the
// operator sends data along but only blocks the frontier from advancing. This should work ok if
// downstream operators only act on data when it is not beyond a frontier anymore, i.e. when the
// frontier acts as a signal for completion. Which it usually does, in timely.

use std::sync::mpsc::{self, TryRecvError};
use std::time::Duration;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, Stream};

use crate::error::Error;
use crate::indexed::runtime::StreamReadHandle;
use crate::indexed::ListenEvent;
use crate::Codec;

/// A Timely Dataflow operator that holds data in the stream until it is not beyond the seal
/// frontier of the given monitored persistent collections. In other words: it holds the data until
/// it is passed by the seal frontier.
pub trait AwaitSeal<G: Scope<Timestamp = u64>, D> {
    /// A Timely Dataflow operator that holds data in the stream until it is not beyond the seal
    /// frontier of the given monitored persistent collections. In other words: it holds the data
    /// until it is passed by the seal frontier.
    // TODO: if we really want this, we should add a trait that allows to just listen on seals. We
    // don't want/need all the K1, V1, K2 types here.
    fn await_seal_binary<K1, V1, K2, V2>(
        &self,
        read_left: StreamReadHandle<K1, V1>,
        read_right: StreamReadHandle<K2, V2>,
    ) -> Result<Stream<G, (D, u64, isize)>, Error>
    where
        K1: Codec + Send,
        K2: Codec + Send,
        V1: Codec + Send,
        V2: Codec + Send;
}

impl<G, D> AwaitSeal<G, D> for Stream<G, (D, u64, isize)>
where
    G: Scope<Timestamp = u64>,
    D: timely::Data,
{
    fn await_seal_binary<K1, V1, K2, V2>(
        &self,
        read_left: StreamReadHandle<K1, V1>,
        read_right: StreamReadHandle<K2, V2>,
    ) -> Result<Stream<G, (D, u64, isize)>, Error>
    where
        K1: Codec + Send,
        K2: Codec + Send,
        V1: Codec + Send,
        V2: Codec + Send,
    {
        let (listen_left_tx, listen_left_rx) = mpsc::channel();
        let listen_left_fn = Box::new(move |e| {
            let _ = listen_left_tx.send(e);
        });

        read_left.listen(listen_left_fn)?;

        let (listen_right_tx, listen_right_rx) = mpsc::channel();
        let listen_right_fn = Box::new(move |e| {
            let _ = listen_right_tx.send(e);
        });

        read_right.listen(listen_right_fn)?;

        let mut await_op =
            OperatorBuilder::new("await_seal_binary".to_string(), self.scope().clone());
        let activator = self
            .scope()
            .activator_for(&await_op.operator_info().address[..]);

        let mut input = await_op.new_input(&self, Pipeline);
        let (mut output, output_stream) = await_op.new_output();

        let mut buffer = Vec::new();
        let mut current_combined_frontier = u64::MIN;
        let mut current_left_frontier = u64::MIN;
        let mut current_right_frontier = u64::MIN;

        let mut stash = std::collections::BTreeMap::new();

        await_op.build(move |mut capabilities| {
            let mut capability = capabilities.remove(0);

            move |_frontiers| {
                // stash all incoming data
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    let entry = stash
                        .entry(time.time().clone())
                        .or_insert_with(|| (time.retain(), Vec::new()));
                    entry.1.extend(buffer.drain(..));
                });

                // listen on collections and downgrade capability when needed
                // TODO: error handling
                match listen_left_rx.try_recv() {
                    Ok(e) => match e {
                        ListenEvent::Records(mut _records) => {
                            // ignoring
                            activator.activate()
                        }
                        ListenEvent::Sealed(ts) => {
                            if ts > current_left_frontier {
                                current_left_frontier = ts;
                            }
                            activator.activate()
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        // ignore
                        activator.activate_after(Duration::from_millis(100));
                    }
                    Err(TryRecvError::Disconnected) => {
                        // ingore
                    }
                }
                match listen_right_rx.try_recv() {
                    Ok(e) => match e {
                        ListenEvent::Records(mut _records) => {
                            // ignoring
                        }
                        ListenEvent::Sealed(ts) => {
                            if ts > current_right_frontier {
                                current_right_frontier = ts;
                            }
                        }
                    },
                    Err(TryRecvError::Empty) => {
                        // ignore
                    }
                    Err(TryRecvError::Disconnected) => {
                        // ingore
                    }
                }
                let new_combined_frontier =
                    std::cmp::min(current_left_frontier, current_right_frontier);

                if new_combined_frontier > current_combined_frontier {
                    capability.downgrade(&new_combined_frontier);
                    current_combined_frontier = new_combined_frontier;

                    // data can only become available if the frontier advanced
                    let mut removed_times = Vec::new();
                    let mut output = output.activate();
                    for (time, (cap, data)) in stash.iter_mut() {
                        if *time < current_combined_frontier {
                            let mut session = output.session(&cap);
                            removed_times.push(time.clone());
                            session.give_vec(data);
                        } else {
                            // BTreeMaps are sorted, so no further times will be
                            // available
                            break;
                        }
                    }
                    for time in removed_times {
                        stash.remove(&time);
                    }
                }
            }
        });

        Ok(output_stream)
    }
}

#[cfg(test)]
mod tests {
    // TODO(aljoscha): add tests
}
