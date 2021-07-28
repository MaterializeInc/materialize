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
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::{ActivateCapability, Concat, Map, UnorderedInput};
use timely::dataflow::{Scope, Stream};
use timely::scheduling::ActivateOnDrop;
use timely::Data;

use crate::error::Error;
use crate::indexed::runtime::{CmdResponse, StreamReadHandle, StreamWriteHandle};
use crate::storage::SeqNo;
use crate::{operators, EncodeDecode};

/// A persistent equivalent of [UnorderedInput].
pub trait PersistentUnorderedInput<G: Scope<Timestamp = u64>, K: Data> {
    /// A persistent equivalent of [UnorderedInput::new_unordered_input].
    fn new_persistent_unordered_input(
        &mut self,
        token: (StreamWriteHandle<K, ()>, StreamReadHandle<K, ()>),
    ) -> (
        (
            PersistentUnorderedHandle<K>,
            ActivateCapability<G::Timestamp>,
        ),
        Stream<G, (K, u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K> PersistentUnorderedInput<G, K> for G
where
    G: Scope<Timestamp = u64>,
    K: Data + EncodeDecode,
{
    fn new_persistent_unordered_input(
        &mut self,
        token: (StreamWriteHandle<K, ()>, StreamReadHandle<K, ()>),
    ) -> (
        (
            PersistentUnorderedHandle<K>,
            ActivateCapability<G::Timestamp>,
        ),
        Stream<G, (K, u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
        let ((handle, cap), ok_new) = self.new_unordered_input();
        let (write, read) = token;

        // Replay the previously persisted data, if any.
        let (ok_previous, err_previous) = operators::replay(self, &read);
        let ok_previous = ok_previous.map(|((k, _), ts, diff)| (k, ts, diff));

        let handle = PersistentUnorderedHandle {
            write: Box::new(write),
            handle,
        };
        let ok_all = ok_previous.concat(&ok_new);
        // In contrast to other operators, PersistentUnorderedInput returns
        // errors persisting new data via the handle, so the only errors we can
        // have are from trying to read the previously persisted data.
        let err_all = err_previous;
        ((handle, cap), ok_all, err_all)
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

impl<'b, K: timely::Data + EncodeDecode> PersistentUnorderedSession<'b, K> {
    /// Transmits a single record after synchronously persisting it.
    pub fn give(&mut self, data: (K, u64, isize), res: CmdResponse<SeqNo>) {
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
    use crate::unreliable::UnreliableHandle;

    use super::*;

    #[test]
    fn new_persistent_unordered_input() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.open("1", "new_persistent_unordered_input_1")?;

        timely::execute_directly(move |worker| {
            let (mut handle, cap) = worker.dataflow(|scope| {
                let token = p.create_or_load("1").unwrap();
                let (input, _, _) = scope.new_persistent_unordered_input(token);
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
                    .expect("write was successful");
            }
        });

        // Execute a second dataflow and reuse the previous in-memory state.
        // This exists to simulate what would happen after a restart in a Persister
        // that was actually backed by persistent storage
        let p = registry.open("1", "new_persistent_unordered_input_2")?;
        let recv = timely::execute_directly(move |worker| {
            let ((mut handle, cap), recv) = worker.dataflow(|scope| {
                let token = p.create_or_load("1").unwrap();
                let (input, ok_stream, _) = scope.new_persistent_unordered_input(token);
                // Send the data to be captured by a channel so that we can replay
                // its contents outside of the dataflow and verify they are correct
                let recv = ok_stream.capture();
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
                    .expect("write was successful");
            }
            recv
        });

        let mut actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|(k, _, _)| k))
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
                let token = p.create_or_load("multiple_workers").unwrap();
                let ((mut handle, cap), _, _) = scope.new_persistent_unordered_input(token);
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
                let token = p.create_or_load("multiple_workers").unwrap();
                let (_, ok_stream, _) = scope.new_persistent_unordered_input(token);
                // Send the data to be captured by a channel so that we can replay
                // its contents outside of the dataflow and verify they are correct
                let tx = tx.lock().expect("lock is not poisoned").clone();
                ok_stream.capture_into(tx);
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

    #[test]
    fn error_stream() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let mut unreliable = UnreliableHandle::default();
        let p = registry.open_unreliable("1", "error_stream", unreliable.clone())?;
        unreliable.make_unavailable();

        let recv = timely::execute_directly(move |worker| {
            worker.dataflow(|scope| {
                let token = p.create_or_load::<(), ()>("error_stream").unwrap();
                let (_, _, err_stream) = scope.new_persistent_unordered_input(token);
                err_stream.capture()
            })
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            "replaying persisted data: unavailable: buffer snapshot".to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }
}
