// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that mirrors a persisted stream.

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::Duration;

use persist_types::Codec;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Map, OkErr, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::Data as TimelyData;

use crate::indexed::runtime::StreamReadHandle;
use crate::indexed::{ListenEvent, ListenFn};
use crate::operators::replay::Replay;

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
        let listen_fn = ListenFn(Box::new(move |e| {
            // TODO: If send fails, it means the operator is no longer running.
            // We should probably allow the listen to deregister itself.
            let _ = listen_tx.send(e);
        }));

        let snapshot = read.listen(listen_fn);

        // TODO: Plumb the name of the stream down through the handles and use
        // it for the operator.
        let (ok_all, err_all) = match snapshot {
            Err(err) => {
                (
                    operator::empty(self),
                    // TODO: Figure out how to make these retractable.
                    vec![(format!("replaying persisted data: {}", err), 0, 1)].to_stream(self),
                )
            }
            Ok(snapshot) => {
                let snapshot_seal = snapshot.get_seal();

                // listen to new data
                let (ok_new, err_new) = listen_source(self, snapshot_seal, listen_rx);
                let err_new_decode = err_new.flat_map(std::convert::identity);

                // Replay the previously persisted data, if any.
                let (ok_previous, err_previous) = self.replay(snapshot);

                let ok_all = ok_previous.concat(&ok_new);
                let err_all = err_previous.concat(&err_new_decode);
                (ok_all, err_all)
            }
        };

        (ok_all, err_all)
    }
}

/// Creates a source that listens on the given `listen_rx`.
fn listen_source<S, K, V>(
    scope: &S,
    lower_filter: Antichain<u64>,
    listen_rx: Receiver<ListenEvent<Vec<u8>, Vec<u8>>>,
) -> (
    Stream<S, ((K, V), u64, isize)>,
    Stream<S, Vec<(String, u64, isize)>>,
)
where
    S: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Send,
    V: TimelyData + Codec + Send,
{
    let source_stream = operator::source(scope, "PersistedSource", |mut capability, info| {
        let worker_index = scope.index();
        let activator = scope.activator_for(&info.address[..]);

        // For Antichain<u64> there can only ever be one element, because
        // u64 has a total order. If we ever change this to an Antichain<T>
        // where we only have T: PartialOrder, this would change. If we have
        // that, though, I don't think we can downgrade this capability because
        // downgrading can only happen when `old_ts` <= `new_ts`, which is not
        // the case when an Antichain has multiple elements.
        for element in lower_filter.iter() {
            capability.downgrade(element);
        }

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
                                for record in records.drain(..) {
                                    let ((k, v), ts, diff) = record;
                                    let k = K::decode(&k);
                                    let v = V::decode(&v);
                                    session.give(((k, v), ts, diff));
                                }
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
    });

    source_stream.ok_err(|u| flatten_decoded_update(u))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};
    use timely::dataflow::ProbeHandle;
    use timely::Config;

    use crate::error::Error;
    use crate::mem::MemRegistry;
    use crate::unreliable::UnreliableHandle;

    use super::*;

    #[test]
    fn persisted_source() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let (oks, errs) = timely::execute_directly(move |worker| {
            let (oks, errs) = worker.dataflow(|scope| {
                let (_, read) = p.create_or_load::<String, ()>("1").unwrap();
                let (ok_stream, err_stream) = scope.persisted_source(&read);
                (ok_stream.capture(), err_stream.capture())
            });

            let (write, _) = p.create_or_load("1").unwrap();
            for i in 1..=5 {
                write
                    .write(&[((i.to_string(), ()), i, 1)])
                    .recv()
                    .expect("write was successful");
            }
            write.seal(6).recv().expect("seal was successful");
            (oks, errs)
        });

        assert_eq!(
            errs.extract()
                .into_iter()
                .flat_map(|(_time, data)| data.into_iter().map(|(err, _ts, _diff)| err))
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );

        let mut actual = oks
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|((k, _), _, _)| k))
            .collect::<Vec<_>>();
        actual.sort();

        let expected = (1usize..=5usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    /// Verifies that `persisted_source()` correctly downgrades to the upper frontier
    /// of already existing/sealed data when it is re-created based on an existing collection.
    ///
    /// There is no easy way to assert on a probe. Instead, we step the worker until the probe is
    /// no longer `less_than` the expected timestamp. This test would hang if the source were not
    /// working as expected.
    #[test]
    fn eager_capability_downgrade() -> Result<(), Error> {
        let mut registry = MemRegistry::new();

        let seal_ts = {
            let p = registry.runtime_no_reentrance()?;

            let (write, _) = p.create_or_load("1").unwrap();
            for i in 1..=5 {
                write
                    .write(&[((i.to_string(), ()), i, 1)])
                    .recv()
                    .expect("write was successful");
            }
            write.seal(6).recv().expect("seal was successful");
            6
        };

        let registry = Arc::new(Mutex::new(registry));

        let result = timely::execute_directly(move |worker| {
            let mut registry = registry.lock().expect("poisoned lock");
            let p = registry.runtime_no_reentrance().expect("missing registry");
            let mut probe = ProbeHandle::new();

            worker.dataflow(|scope| {
                let (_, read) = p.create_or_load::<String, ()>("1").unwrap();
                let (oks, _rrs) = scope.persisted_source(&read);

                oks.probe_with(&mut probe);
            });

            while probe.less_than(&seal_ts) {
                worker.step();
            }

            true
        });

        assert!(result);

        Ok(())
    }

    // TODO: At the moment, this test hangs indefinitely. Currently unclear why.
    #[test]
    #[ignore]
    fn multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

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
        let p = registry.runtime_no_reentrance()?;
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
        let mut unreliable = UnreliableHandle::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;
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

        let expected = vec![
        (
            "replaying persisted data: failed to commit metadata after appending to unsealed: unavailable: blob set".to_string(),
            0,
            1,
        ),
        ];
        assert_eq!(actual, expected);

        Ok(())
    }
}

fn flatten_decoded_update<K, V>(
    update: ((Result<K, String>, Result<V, String>), u64, isize),
) -> Result<((K, V), u64, isize), Vec<(String, u64, isize)>> {
    let ((k, v), ts, diff) = update;
    match (k, v) {
        (Ok(k), Ok(v)) => Ok(((k, v), ts, diff)),
        (Err(k_err), Ok(_)) => Err(vec![(k_err, ts, diff)]),
        (Ok(_), Err(v_err)) => Err(vec![(v_err, ts, diff)]),
        (Err(k_err), Err(v_err)) => Err(vec![(k_err, ts, diff), (v_err, ts, diff)]),
    }
}
