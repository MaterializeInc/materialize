// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that mirrors a persisted stream.

use std::time::Duration;

use mz_persist_types::Codec;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::Concat;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::Data as TimelyData;

use crate::client::StreamReadHandle;
use crate::indexed::ListenEvent;
use crate::operators::replay::Replay;
use crate::operators::DEFAULT_OUTPUTS_PER_YIELD;

/// A Timely Dataflow operator that mirrors a persisted stream.
pub trait PersistedSource<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Emits a snapshot of the persisted stream taken as of this call and
    /// listens for any new data added to the persisted stream after that.
    fn persisted_source(
        &mut self,
        read: StreamReadHandle<K, V>,
        as_of_frontier: &Antichain<u64>,
    ) -> Stream<G, (Result<(K, V), String>, u64, i64)> {
        self.persisted_source_yield(read, as_of_frontier, DEFAULT_OUTPUTS_PER_YIELD)
    }

    /// Emits a snapshot of the persisted stream taken as of this call and
    /// listens for any new data added to the persisted stream after that,
    /// yielding periodically.
    ///
    /// This yields after `outputs_per_yield` outputs to allow downstream
    /// operators to reduce down the data and limit max memory usage.
    fn persisted_source_yield(
        &mut self,
        read: StreamReadHandle<K, V>,
        as_of_frontier: &Antichain<u64>,
        outputs_per_yield: usize,
    ) -> Stream<G, (Result<(K, V), String>, u64, i64)>;
}

impl<G, K, V> PersistedSource<G, K, V> for G
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Send,
    V: TimelyData + Codec + Send,
{
    fn persisted_source_yield(
        &mut self,
        read: StreamReadHandle<K, V>,
        as_of_frontier: &Antichain<u64>,
        outputs_per_yield: usize,
    ) -> Stream<G, (Result<(K, V), String>, u64, i64)> {
        let (listen_tx, listen_rx) = crossbeam_channel::unbounded();
        let snapshot = read.listen(listen_tx);

        let snapshot_seal = snapshot
            .as_ref()
            .map_or(None, |snapshot| Some(snapshot.get_seal()));

        // TODO: Plumb the name of the stream down through the handles and use
        // it for the operator.

        // listen to new data
        let new = listen_source(self, snapshot_seal, listen_rx, outputs_per_yield);

        // Replay the previously persisted data, if any.
        let previous = self.replay_yield(snapshot, as_of_frontier, outputs_per_yield);

        previous.concat(&new)
    }
}

/// Creates a source that listens on the given `listen_rx`.
fn listen_source<G, K, V>(
    scope: &G,
    // This uses an `Option<Antichain<_>>` and not an empty `Antichain` to signal that there is no
    // initial frontier because an empty frontier would signal that we are at "the end of time",
    // meaning that there will be no more events in the future.
    initial_frontier: Option<Antichain<u64>>,
    listen_rx: crossbeam_channel::Receiver<ListenEvent>,
    _outputs_per_yield: usize,
) -> Stream<G, (Result<(K, V), String>, u64, i64)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec + Send,
    V: TimelyData + Codec + Send,
{
    operator::source(scope, "PersistedSource", |mut capability, info| {
        let worker_index = scope.index();
        let activator = scope.activator_for(&info.address[..]);

        // For Antichain<u64> there can only ever be one element, because
        // u64 has a total order. If we ever change this to an Antichain<T>
        // where we only have T: PartialOrder, this would change. If we have
        // that, though, I don't think we can downgrade this capability because
        // downgrading can only happen when `old_ts` <= `new_ts`, which is not
        // the case when an Antichain has multiple elements.
        if let Some(initial_frontier) = initial_frontier {
            for element in initial_frontier.iter() {
                capability.downgrade(element);
            }
        }

        // TODO: This currently works by only emitting the persisted data on worker
        // 0 because that was the simplest thing to do initially. Instead, we should
        // shard up the responsibility between all the workers.
        let (mut receiver_cap, mut done) = match worker_index {
            0 => (Some((listen_rx, capability)), false),
            _ => (None, true),
        };
        move |output| {
            if let Some((listen_rx, cap)) = receiver_cap.as_mut() {
                let mut session = output.session(cap);

                match listen_rx.try_recv() {
                    Ok(e) => match e {
                        ListenEvent::Records(mut records) => {
                            for record in records.drain(..) {
                                let ((k, v), ts, diff) = record;
                                let k = K::decode(&k);
                                let v = V::decode(&v);
                                let result = match ((k, v), ts, diff) {
                                    ((Ok(k), Ok(v)), ts, diff) => (Ok((k, v)), ts, diff),
                                    ((Err(err), _), ts, diff) => (Err(err), ts, diff),
                                    ((_, Err(err)), ts, diff) => (Err(err), ts, diff),
                                };

                                session.give(result);
                            }
                            activator.activate();
                            // TODO: Instead of yielding after every message
                            // from the channel, only yield after we've given at
                            // least `outputs_per_yield` records (or if we get
                            // an Empty or Disconnected from try_recv).
                        }
                        ListenEvent::Sealed(ts) => {
                            cap.downgrade(&ts);
                            activator.activate();
                        }
                    },
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        activator.activate_after(Duration::from_millis(100));
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        done = true;
                    }
                }
            }
            if done {
                receiver_cap = None;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, OkErr, Probe};
    use timely::dataflow::ProbeHandle;
    use timely::Config;

    use crate::error::Error;
    use crate::mem::MemRegistry;
    use crate::operators::split_ok_err;
    use crate::unreliable::UnreliableHandleOld;

    use super::*;

    #[test]
    fn persisted_source() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let (oks, errs) = timely::execute_directly(move |worker| {
            let (oks, errs) = worker.dataflow(|scope| {
                let (_write, read) = p.create_or_load::<String, ()>("1");
                let (ok_stream, err_stream) = scope
                    .persisted_source(read, &Antichain::from_elem(0))
                    .ok_err(split_ok_err);
                (ok_stream.capture(), err_stream.capture())
            });

            let (write, _) = p.create_or_load("1");
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

            let (write, _) = p.create_or_load("1");
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
                let (_write, read) = p.create_or_load::<String, ()>("1");
                let (oks, _rrs) = scope
                    .persisted_source(read, &Antichain::from_elem(0))
                    .ok_err(split_ok_err);

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

    #[test]
    #[ignore]
    fn multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        // Write some data using 3 workers.
        timely::execute(Config::process(3), move |worker| {
            worker.dataflow::<u64, _, _>(|scope| {
                let (write, _) = p.create_or_load("1");
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
                let (write, read) = p.create_or_load("1");
                let (ok_stream, _) = scope
                    .persisted_source(read, &Antichain::from_elem(0))
                    .ok_err(split_ok_err);

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
        let mut p = MemRegistry::new().runtime_no_reentrance()?;
        let (_write, read) = p.create_or_load::<(), ()>("1");
        p.stop()?;

        let recv = timely::execute_directly(move |worker| {
            let recv = worker.dataflow(|scope| {
                let (_, err_stream) = scope
                    .persisted_source(read, &Antichain::from_elem(0))
                    .ok_err(split_ok_err);
                err_stream.capture()
            });
            recv
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            "replaying persisted data: runtime shutdown".to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn initial_error_handling() -> Result<(), Error> {
        let mut unreliable = UnreliableHandleOld::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;
        unreliable.make_unavailable();
        let (_, read) = p.create_or_load::<(), ()>("test_name");

        let recv = timely::execute_directly(move |worker| {
            let recv = worker.dataflow(|scope| {
                let stream = scope.persisted_source(read, &Antichain::from_elem(0));
                stream.capture()
            });

            recv
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            Err("replaying persisted data: unavailable: blob set".to_string()),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }

    // Previously, creating a persistent source would create differently shaped operator graphs,
    // depending on whether an internal call to persistence was successful or not. Timely dataflow
    // doesn't like that, which would manifest in hanging worker threads when trying to shut down.
    //
    // This test concurrently shuts down the persist runtime (which will make the internal
    // `listen()` call fail) and creates a bunch of timely workers. Some workers are expected to
    // call `listen` before the runtime is shut down and some after. We "help" the test a bit by
    // inserting `thread::sleep()` calls in strategic places, this way it is almost guaranteed to
    // fail without the fix.
    #[test]
    fn regression_8687_deterministic_operator_construction() -> Result<(), Error> {
        let p = MemRegistry::new().runtime_no_reentrance()?;
        let (write, _read) = p.create_or_load::<String, ()>("1");

        // Write some data.
        let data = vec![(("ciao".into(), ()), 1, 1), (("bello".into(), ()), 1, 1)];
        write.write(&data).recv().expect("write failed");
        write.seal(1).recv().expect("seal failed");

        // Concurrently shut down the runtime and create multiple timely workers/threads from a
        // second thread that create a persistent source.
        let mut cloned_p = p.clone();
        let runtime_shutdown = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1));
            cloned_p.stop().expect("shutdown failed");
        });

        let timely = std::thread::spawn(move || {
            let guards = timely::execute(Config::process(3), move |worker| {
                let mut probe = ProbeHandle::new();
                if worker.index() == 0 {
                    std::thread::sleep(Duration::from_millis(2));
                }
                worker.dataflow(|scope| {
                    let (_write, read) = p.create_or_load::<String, ()>("1");
                    let data = scope.persisted_source(read, &Antichain::from_elem(0));
                    data.probe_with(&mut probe);
                });

                while probe.less_than(&1) {
                    worker.step();
                }
            })?;

            let result: Result<Vec<_>, _> = guards.join().into_iter().collect();

            result
        });

        // Assert that we can shut down cleanly. Without the fix, this test would not fail but
        // would hang indefinitely while trying to shut down the timely workers.
        runtime_shutdown
            .join()
            .expect("joining runtime thread failed");

        timely
            .join()
            .expect("joining timely launcher thread failed")
            .expect("timely workers failed");

        Ok(())
    }
}
