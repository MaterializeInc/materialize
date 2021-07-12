// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that synchronously persists stream input.

use std::convert::identity;
use std::sync::mpsc;

use timely::dataflow::channels::pushers::buffer::AutoflushSession;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::{ActivateCapability, Concat, ToStream, UnorderedInput};
use timely::dataflow::{Scope, Stream};
use timely::scheduling::ActivateOnDrop;
use timely::Data;

use crate::error::Error;
use crate::indexed::runtime::{CmdResponse, StreamReadHandle, StreamWriteHandle};
use crate::indexed::Snapshot;
use crate::Token;

/// A persistent equivalent of [UnorderedInput].
pub trait PersistentUnorderedInput<G: Scope<Timestamp = u64>, K: Data> {
    /// A persistent equivalent of [UnorderedInput::new_unordered_input].
    fn new_persistent_unordered_input(
        &mut self,
        token: Token<StreamWriteHandle<K, ()>, StreamReadHandle<K, ()>>,
    ) -> (
        (
            PersistentUnorderedHandle<K>,
            ActivateCapability<G::Timestamp>,
        ),
        Stream<G, (K, u64, isize)>,
    );
}

impl<G, K> PersistentUnorderedInput<G, K> for G
where
    G: Scope<Timestamp = u64>,
    K: Data,
{
    fn new_persistent_unordered_input(
        &mut self,
        token: Token<StreamWriteHandle<K, ()>, StreamReadHandle<K, ()>>,
    ) -> (
        (
            PersistentUnorderedHandle<K>,
            ActivateCapability<G::Timestamp>,
        ),
        Stream<G, (K, u64, isize)>,
    ) {
        let ((handle, cap), stream) = self.new_unordered_input();
        let (write, meta) = token.into_inner();

        // Replay the previously persisted data, if any.
        //
        // TODO: This currently works by only emitting the persisted data on
        // worker 0 because that was the simplest thing to do initially.
        // Instead, we should shard up the responsibility between all the
        // workers.
        let previously_persisted = if self.index() == 0 {
            // TODO: Do this with a timely operator that reads the snapshot.
            let mut snap = meta.snapshot().expect("TODO");
            let mut buf = Vec::new();
            while snap.read(&mut buf) {}
            buf.into_iter()
                .map(|((key, _), ts, diff)| (key, ts, diff))
                .to_stream(self)
        } else {
            operator::empty(self)
        };

        let handle = PersistentUnorderedHandle {
            write: Box::new(write),
            handle,
        };
        ((handle, cap), previously_persisted.concat(&stream))
    }
}

/// A persistent equivalent of [UnorderedHandle].
pub struct PersistentUnorderedHandle<K: Data> {
    write: Box<StreamWriteHandle<K, ()>>,
    handle: UnorderedHandle<u64, (K, u64, isize)>,
}

impl<K: Data> PersistentUnorderedHandle<K> {
    /// A persistent equivalent of [UnorderedHandle::session].
    pub fn session<'b>(
        &'b mut self,
        cap: ActivateCapability<u64>,
    ) -> PersistentUnorderedSession<'b, K> {
        PersistentUnorderedSession {
            write: &mut self.write,
            session: self.handle.session(cap),
        }
    }
}

/// A persistent equivalent of [UnorderedHandle::session]'s return type.
pub struct PersistentUnorderedSession<'b, K: timely::Data> {
    write: &'b mut Box<StreamWriteHandle<K, ()>>,
    session: ActivateOnDrop<
        AutoflushSession<
            'b,
            u64,
            (K, u64, isize),
            Counter<u64, (K, u64, isize), Tee<u64, (K, u64, isize)>>,
        >,
    >,
}

impl<'b, K: timely::Data> PersistentUnorderedSession<'b, K> {
    /// Transmits a single record after synchronously persisting it.
    pub fn give(&mut self, data: (K, u64, isize), res: CmdResponse<()>) {
        let (tx, rx) = mpsc::channel();
        self.write
            .write(&[((data.0.clone(), ()), data.1, data.2)], tx.into());
        // TODO: The signature of this method is now async, but this bit is
        // still blocking. The lifetime issues here are really tricky.
        let r = rx
            .recv()
            .map_err(|_| Error::RuntimeShutdown)
            .and_then(identity);
        if let Ok(_) = r {
            self.session.give(data);
        }
        res.send(r);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc, Mutex};

    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::Capture;
    use timely::Config;

    use crate::error::Error;
    use crate::mem::MemRegistry;

    use super::*;

    #[test]
    fn new_persistent_unordered_input() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.open("1", "new_persistent_unordered_input_1")?;

        timely::execute_directly(move |worker| {
            let (mut handle, cap) = worker.dataflow(|scope| {
                let persister = p.create_or_load("1").unwrap();
                let (input, _stream) = scope.new_persistent_unordered_input(persister);
                input
            });
            let mut session = handle.session(cap);
            let (tx, rx) = mpsc::channel();
            for i in 1..=5 {
                session.give((i.to_string(), i, 1), tx.clone().into());
            }
            for _ in 1..=5 {
                rx.recv()
                    .expect("runtime has not stopped")
                    .expect("write was successful")
            }
        });

        // Execute a second dataflow and reuse the previous in-memory state.
        // This exists to simulate what would happen after a restart in a Persister
        // that was actually backed by persistent storage
        let p = registry.open("1", "new_persistent_unordered_input_2")?;
        let recv = timely::execute_directly(move |worker| {
            let ((mut handle, cap), recv) = worker.dataflow(|scope| {
                let persister = p.create_or_load("1").unwrap();
                let (input, stream) = scope.new_persistent_unordered_input(persister);
                // Send the data to be captured by a channel so that we can replay
                // its contents outside of the dataflow and verify they are correct
                let recv = stream.capture();
                (input, recv)
            });
            let mut session = handle.session(cap);
            let (tx, rx) = mpsc::channel();
            for i in 6..=9 {
                session.give((i.to_string(), i, 1), tx.clone().into());
            }
            for _ in 6..=9 {
                rx.recv()
                    .expect("runtime has not stopped")
                    .expect("write was successful")
            }
            recv
        });

        let mut actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|x| x.0))
            .collect::<Vec<_>>();
        actual.sort();
        let expected = (1usize..=9usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn multiple_workers() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.open("1", "multiple_workers 1")?;

        // Write some data using 3 workers.
        timely::execute(Config::process(3), move |worker| {
            worker.dataflow(|scope| {
                let persister = p.create_or_load("multiple_workers").unwrap();
                let ((mut handle, cap), _stream) = scope.new_persistent_unordered_input(persister);
                // Write one thing from each worker.
                let (tx, rx) = mpsc::channel();
                handle
                    .session(cap)
                    .give((format!("worker-{}", scope.index()), 1, 1), tx.into());
                rx.recv()
                    .expect("runtime has not stopped")
                    .expect("write was successful")
            });
        })?;

        // Execute a second dataflow with a different number of workers (2).
        // This is mainly testing that we only get one copy of the original
        // persisted data in the stream (as opposed to one per worker).
        let p = registry.open("1", "multiple_workers 2")?;
        let (tx, rx) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        timely::execute(Config::process(2), move |worker| {
            worker.dataflow(|scope| {
                let persister = p.create_or_load("multiple_workers").unwrap();
                let (_, stream) = scope.new_persistent_unordered_input(persister);
                // Send the data to be captured by a channel so that we can replay
                // its contents outside of the dataflow and verify they are correct
                let tx = tx.lock().expect("lock is not poisoned").clone();
                stream.capture_into(tx);
            });
        })?;

        let mut actual = rx
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();
        actual.sort();

        let expected = vec![
            ("worker-0".to_owned(), 1, 1),
            ("worker-1".to_owned(), 1, 1),
            ("worker-2".to_owned(), 1, 1),
        ];
        assert_eq!(expected, actual);

        Ok(())
    }
}
