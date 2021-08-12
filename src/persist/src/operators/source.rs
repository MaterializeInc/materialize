// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that mirrors a persisted stream.

use std::sync::mpsc::{self, TryRecvError};
use std::time::Duration;

use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::Data as TimelyData;

use crate::indexed::runtime::StreamReadHandle;
use crate::indexed::ListenEvent;
use crate::operators::flatten_decoded_update;
use crate::{operators, Codec};

/// A Timely Dataflow operator that mirrors a persisted stream.
pub trait PersistedSource<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Emits a snapshot of the persisted stream taken as of this call and
    /// listens for any new data added to the persisted stream after that.
    fn persisted_source(
        &mut self,
        read: &StreamReadHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K, V> PersistedSource<G, K, V> for G
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Send,
    V: TimelyData + Codec + Send,
{
    fn persisted_source(
        &mut self,
        read: &StreamReadHandle<K, V>,
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
        let (listen_tx, listen_rx) = mpsc::channel();
        let listen_fn = Box::new(move |e| {
            // TODO: If send fails, it means the operator is no longer running.
            // We should probably allow the listen to deregister itself.
            let _ = listen_tx.send(e);
        });
        let err_new_register = match read.listen(listen_fn) {
            Ok(_) => operator::empty(self),
            Err(err) => vec![(err.to_string(), 0, 1)].to_stream(self),
        };

        // TODO: Plumb the name of the stream down through the handles and use
        // it for the operator.
        let (ok_new, err_new_decode) =
            operator::source(self, "PersistedSource", |capability, info| {
                let worker_index = self.index();
                let activator = self.activator_for(&info.address[..]);
                let mut cap = Some(capability);
                move |output| {
                    let mut done = false;
                    if let Some(cap) = cap.as_mut() {
                        let mut session = output.session(cap);
                        match listen_rx.try_recv() {
                            Ok(e) => match e {
                                ListenEvent::Records(mut records) => {
                                    // TODO: This currently works by only emitting the persisted data on worker
                                    // 0 because that was the simplest thing to do initially. Instead, we should
                                    // shard up the responsibility between all the workers.
                                    if worker_index == 0 {
                                        session.give_vec(&mut records);
                                    }
                                    activator.activate();
                                }
                                ListenEvent::Sealed(ts) => {
                                    cap.downgrade(&ts);
                                    activator.activate();
                                }
                            },
                            Err(TryRecvError::Empty) => {
                                // TODO: Hook the activator up to the callback instead of
                                // TryRecvError::Empty.
                                activator.activate_after(Duration::from_millis(100));
                            }
                            Err(TryRecvError::Disconnected) => {
                                done = true;
                            }
                        }
                    }
                    if done {
                        cap = None;
                    }
                }
            })
            .ok_err(|u| flatten_decoded_update(u));
        let err_new_decode = err_new_decode.flat_map(std::convert::identity);

        // Replay the previously persisted data, if any.
        let (ok_previous, err_previous) = operators::replay(self, &read);

        let ok_all = ok_previous.concat(&ok_new);
        let err_all = err_previous
            .concat(&err_new_register)
            .concat(&err_new_decode);
        (ok_all, err_all)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::Capture;
    use timely::Config;

    use crate::error::Error;
    use crate::mem::MemRegistry;
    use crate::unreliable::UnreliableHandle;

    use super::*;

    #[test]
    fn persisted_source() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.open("1", "lock 1")?;

        let recv = timely::execute_directly(move |worker| {
            let recv = worker.dataflow(|scope| {
                let (_, read) = p.create_or_load::<String, ()>("1").unwrap();
                let (ok_stream, _) = scope.persisted_source(&read);
                ok_stream.capture()
            });

            let (write, _) = p.create_or_load("1").unwrap();
            for i in 1..=5 {
                write
                    .write(&[((i.to_string(), ()), i, 1)])
                    .recv()
                    .expect("write was successful");
            }
            write.seal(6).recv().expect("seal was successful");
            recv
        });

        let mut actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|((k, _), _, _)| k))
            .collect::<Vec<_>>();
        actual.sort();
        let expected = (1usize..=5usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    // TODO: At the moment, there's a race between registering the listener and
    // getting the snapshot. Fix it by get a seqno back from listener
    // registration and use it as the upper bound of the snapshot.
    #[test]
    #[ignore]
    fn multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.open("multiple_workers", "lock 1")?;

        // Write some data using 3 workers.
        timely::execute(Config::process(3), move |worker| {
            worker.dataflow::<u64, _, _>(|scope| {
                let (write, _) = p.create_or_load("1").unwrap();
                // Write one thing from each worker.
                write
                    .write(&[((format!("worker-{}", scope.index()), ()), 1, 1)])
                    .recv()
                    .expect("write was successful")
            });
        })?;

        // Execute a second dataflow with a different number of workers (2).
        // This is mainly testing that we only get one copy of the original
        // persisted data in the stream (as opposed to one per worker).
        let p = registry.open("multiple_workers", "lock 2")?;
        let (tx, rx) = mpsc::channel();
        let capture_tx = Arc::new(Mutex::new(tx));
        timely::execute(Config::process(2), move |worker| {
            worker.dataflow(|scope| {
                let (write, read) = p.create_or_load("1").unwrap();
                let (ok_stream, _) = scope.persisted_source(&read);

                // Write one thing from each worker again. This time at timestamp 2.
                write
                    .write(&[((format!("worker-{}", scope.index()), ()), 2, 1)])
                    .recv()
                    .expect("write was successful");

                // Now seal time 3 so we can probe the output.
                if scope.index() == 0 {
                    write.seal(3).recv().expect("seal was successful");
                }

                // Send the data to be captured by a channel so that we can replay
                // its contents outside of the dataflow and verify they are correct
                let tx = capture_tx.lock().expect("lock is not poisoned").clone();
                ok_stream.capture_into(tx)
            });
            // TODO: This hangs because the PersistedSource operator is still
            // hanging on to its capability. We need to wait until we've
            // captured through ts=3 and then unregister the listener/close the
            // persister to unblock things.
        })?;

        let mut actual = rx
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();
        actual.sort();

        let expected = vec![
            (("worker-0".to_owned(), ()), 1, 1),
            (("worker-0".to_owned(), ()), 2, 1),
            (("worker-1".to_owned(), ()), 1, 1),
            (("worker-1".to_owned(), ()), 2, 1),
            (("worker-2".to_owned(), ()), 1, 1),
        ];
        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn error_stream() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.open_unreliable("1", "error_stream", unreliable.clone())?;
        let (_, read) = p.create_or_load::<(), ()>("1").unwrap();
        unreliable.make_unavailable();

        let recv = timely::execute_directly(move |worker| {
            let recv = worker.dataflow(|scope| {
                let (_, err_stream) = scope.persisted_source(&read);
                err_stream.capture()
            });

            unreliable.make_available();
            // TODO: think through the error handling more. Ideally, we could make
            // this test work without this call to seal.
            let (write, _) = p.create_or_load::<(), ()>("1").unwrap();
            write.seal(1).recv().expect("seal was successful");
            recv
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            "replaying persisted data: unavailable: blob get".to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }
}
