// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime for concurrent, asynchronous use of [Indexed].

use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

use log;

use crate::error::Error;
use crate::indexed::encoding::Id;
use crate::indexed::{Indexed, IndexedSnapshot};
use crate::storage::{Blob, Buffer};
use crate::Data;

enum Cmd<K, V> {
    Register(String, CmdResponse<Id>),
    Write(Id, Vec<((K, V), u64, isize)>, CmdResponse<()>),
    Seal(Id, u64, CmdResponse<()>),
    Snapshot(Id, CmdResponse<IndexedSnapshot<K, V>>),
    Stop(CmdResponse<()>),
}

/// Starts the runtime in a [std::thread].
///
/// This returns a clone-able client handle. The runtime is stopped when any
/// client calls [RuntimeClient::stop] or when all clients have been dropped.
///
/// TODO: At the moment, this runs IO and heavy cpu work in a single thread.
/// Move this work out into whatever async runtime the user likes, via something
/// like https://docs.rs/rdkafka/0.26.0/rdkafka/util/trait.AsyncRuntime.html
pub fn start<K, V, U, L>(indexed: Indexed<K, V, U, L>) -> RuntimeClient<K, V>
where
    K: Data + Send + Sync + 'static,
    V: Data + Send + Sync + 'static,
    U: Buffer + Send + 'static,
    L: Blob + Send + 'static,
{
    // TODO: Is an unbounded channel the right thing to do here?
    let (tx, rx) = crossbeam_channel::unbounded();
    let handle = thread::spawn(move || {
        // TODO: Set up the tokio or other async runtime context here.
        let mut l = RuntimeImpl { indexed, rx };
        while l.work() {}
    });
    let handle = Mutex::new(Some(handle));
    let core = RuntimeCore { handle, tx };
    RuntimeClient {
        core: Arc::new(core),
    }
}

/// A receiver for the `Result<T, Error>` response of an asynchronous command.
pub enum CmdResponse<T> {
    /// No response is desired.
    Ignore,
    /// A single channel send per command call.
    ///
    /// It is supported for multiple command results to be sent to the same
    /// channel by cloning the sender. This even works if they are different
    /// types of command, but the expected usage is to issue N writes and then
    /// wait on them all by receiving N times.
    ///
    /// NB: The given Sender may be dropped without having received a message if
    /// the runtime is shut down.
    StdChannel(mpsc::Sender<Result<T, Error>>),
    /// A callback executed once.
    ///
    /// NB: This isn't guaranteed to get called because of a race with runtime
    /// shutdown.
    Callback(Box<dyn FnOnce(Result<T, Error>) + Send + 'static>),
    // TODO: Add a std::future::Future variant to better play with async? I
    // think the Future impl would be pretty straightforward.
}

impl<T> From<mpsc::Sender<Result<T, Error>>> for CmdResponse<T> {
    fn from(c: mpsc::Sender<Result<T, Error>>) -> Self {
        CmdResponse::StdChannel(c)
    }
}

impl<T> From<Box<dyn FnOnce(Result<T, Error>) + Send + 'static>> for CmdResponse<T> {
    fn from(c: Box<dyn FnOnce(Result<T, Error>) + Send + 'static>) -> Self {
        CmdResponse::Callback(c)
    }
}

impl<T> CmdResponse<T> {
    fn send(self, t: Result<T, Error>) {
        match self {
            CmdResponse::Ignore => {}
            CmdResponse::Callback(c) => c(t),
            CmdResponse::StdChannel(c) => {
                if let Err(mpsc::SendError(_)) = c.send(t) {
                    // The SendError docs indicate that this only happens if the
                    // receiver was dropped. If the client that sent this cmd
                    // has hung up (imagine an INSERT spawned a Cmd::Write, but
                    // the pgwire connection has been severed while it synced to
                    // disk), then no one is listening and that's okay.
                }
                // Defensively drop c, just to make it obvious.
                drop(c)
            }
        }
    }
}

struct RuntimeCore<K, V> {
    handle: Mutex<Option<JoinHandle<()>>>,
    tx: crossbeam_channel::Sender<Cmd<K, V>>,
}

impl<K, V> RuntimeCore<K, V> {
    fn send(&self, cmd: Cmd<K, V>) {
        if let Err(crossbeam_channel::SendError(cmd)) = self.tx.send(cmd) {
            // According to the docs, a SendError can only happen if the
            // receiver has hung up, which in this case only happens if the
            // thread has exited. The thread only exits if we send it a
            // Cmd::Stop (or it panics).
            match cmd {
                Cmd::Stop(res) => {
                    // Already stopped: no-op.
                    res.send(Ok(()))
                }
                Cmd::Register(_, res) => {
                    res.send(Err(Error::from("register cmd sent to stopped runtime")))
                }
                Cmd::Write(_, _, res) => {
                    res.send(Err(Error::from("write cmd sent to stopped runtime")))
                }
                Cmd::Seal(_, _, res) => {
                    res.send(Err(Error::from("seal cmd sent to stopped runtime")))
                }
                Cmd::Snapshot(_, res) => {
                    res.send(Err(Error::from("snapshot cmd sent to stopped runtime")))
                }
            }
        }
    }

    fn stop(&self) -> Result<(), Error> {
        if let Some(handle) = self.handle.lock()?.take() {
            let (tx, rx) = mpsc::channel();
            self.send(Cmd::Stop(tx.into()));
            rx.recv().map_err(|_| Error::RuntimeShutdown)??;
            if let Err(_) = handle.join() {
                // If the thread panic'd, then by definition it has been
                // stopped, so we can return an Ok. This is surprising, though,
                // so log a message. Unfortunately, there isn't really a way to
                // put the panic message in this log.
                log::error!("persist runtime thread panic'd");
            }
        }
        Ok(())
    }
}

impl<K, V> Drop for RuntimeCore<K, V> {
    fn drop(&mut self) {
        if let Err(err) = self.stop() {
            log::error!("dropping persist runtime: {}", err);
        }
    }
}

/// A clone-able handle to the persistence runtime.
///
/// The runtime is stopped when any client calls [RuntimeClient::stop] or when
/// all clients have been dropped, whichever comes first.
///
/// NB: The methods below are executed concurrently. For a some call X to be
/// guaranteed as "previous" to another call Y, X's response must have been
/// received before Y's call started.
pub struct RuntimeClient<K, V> {
    core: Arc<RuntimeCore<K, V>>,
}

impl<K, V> Clone for RuntimeClient<K, V> {
    fn clone(&self) -> Self {
        RuntimeClient {
            core: self.core.clone(),
        }
    }
}

impl<K: Clone, V: Clone> RuntimeClient<K, V> {
    /// Synchronously registers a new stream for writes and reads.
    ///
    /// This method is idempotent.
    pub fn register(&self, id: &str) -> Result<Id, Error> {
        let (tx, rx) = mpsc::channel();
        self.core.send(Cmd::Register(id.to_owned(), tx.into()));
        rx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    /// Asynchronously persists `(Key, Value, Time, Diff)` updates for the
    /// stream with the given id.
    ///
    /// The id must have previously been registered.
    pub fn write(&self, id: Id, updates: &[((K, V), u64, isize)], res: CmdResponse<()>) {
        self.core.send(Cmd::Write(id, updates.to_vec(), res))
    }

    /// Asynchronously advances the "sealed" frontier for the stream with the
    /// given id, which restricts what times can later be sealed and/or written
    /// for that id.
    ///
    /// The id must have previously been registered.
    pub fn seal(&self, id: Id, ts: u64, res: CmdResponse<()>) {
        self.core.send(Cmd::Seal(id, ts, res))
    }

    /// Asynchronously returns a [crate::persister::Snapshot] for the stream
    /// with the given id.
    ///
    /// This snapshot is guaranteed to include any previous writes.
    ///
    /// The id must have previously been registered.
    pub fn snapshot(&self, id: Id, res: CmdResponse<IndexedSnapshot<K, V>>) {
        self.core.send(Cmd::Snapshot(id, res))
    }

    /// Synchronously stops the runtime, causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn stop(&mut self) -> Result<(), Error> {
        self.core.stop()
    }
}

struct RuntimeImpl<K, V, U: Buffer, L: Blob> {
    indexed: Indexed<K, V, U, L>,
    rx: crossbeam_channel::Receiver<Cmd<K, V>>,
}

impl<K: Data, V: Data, U: Buffer, L: Blob> RuntimeImpl<K, V, U, L> {
    /// Synchronously waits for the next command, executes it, and responds.
    ///
    /// Returns false to indicate a graceful shutdown, true otherwise.
    fn work(&mut self) -> bool {
        let cmd = match self.rx.recv() {
            Ok(cmd) => cmd,
            Err(crossbeam_channel::RecvError) => {
                // All Runtime handles hung up. Drop should have shut things down
                // nicely, so this is unexpected.
                return false;
            }
        };
        match cmd {
            Cmd::Stop(res) => {
                res.send(self.indexed.close());
                return false;
            }
            Cmd::Register(id, res) => {
                let r = self.indexed.register(&id);
                res.send(Ok(r));
            }
            Cmd::Write(id, updates, res) => {
                let r = self.indexed.write_sync(id, &updates);
                // TODO: Move this to a Cmd::Tick or something.
                let r = r.and_then(|_| self.indexed.step());
                res.send(r);
            }
            Cmd::Seal(id, ts, res) => {
                let r = self.indexed.seal(id, ts);
                res.send(r);
            }
            Cmd::Snapshot(id, res) => {
                let r = self.indexed.snapshot(id);
                res.send(r);
            }
        }
        return true;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use crate::mem::{MemBlob, MemBuffer, MemRegistry};
    use crate::persister::SnapshotExt;

    use super::*;

    fn block_on<T, F: FnOnce(CmdResponse<T>)>(f: F) -> Result<T, Error> {
        let (tx, rx) = mpsc::channel();
        f(tx.into());
        rx.recv().expect("block_on receiver is not dropped")
    }

    #[test]
    fn runtime() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let indexed = Indexed::new(MemBuffer::new("runtime")?, MemBlob::new("runtime")?)?;
        let mut runtime = start(indexed);

        let id = runtime.register("0")?;
        block_on(|res| runtime.write(id, &data, res))?;
        block_on(|res| runtime.seal(id, 3, res))?;
        let snap = block_on(|res| runtime.snapshot(id, res))?;
        assert_eq!(snap.read_to_end(), data);

        // Commands sent after stop return an error, but calling stop again is
        // fine.
        runtime.stop()?;
        assert!(runtime.register("0").is_err());
        runtime.stop()?;

        Ok(())
    }

    #[test]
    fn concurrent() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let indexed = Indexed::new(MemBuffer::new("concurrent")?, MemBlob::new("concurrent")?)?;
        let client1 = start(indexed);
        let id = client1.register("0")?;

        // Everything is still running after client1 is dropped.
        let mut client2 = client1.clone();
        drop(client1);
        block_on(|res| client2.write(id, &data, res))?;
        let snap = block_on(|res| client2.snapshot(id, res))?;
        assert_eq!(snap.read_to_end(), data);
        client2.stop()?;

        Ok(())
    }

    #[test]
    fn restart() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        let mut registry = MemRegistry::new();

        // Shutdown happens if we explicitly call stop, unlocking the buffer and
        // blob and allowing them to be reused in the next Indexed.
        let mut persister = registry.open("path", "restart-1")?;
        let (mut write, _) = persister.create_or_load("0")?.into_inner();
        write.write_sync(&data[0..1])?;
        assert_eq!(persister.stop(), Ok(()));

        // Shutdown happens if all handles are dropped, even if we don't call
        // stop.
        let mut persister = registry.open("path", "restart-2")?;
        let (mut write, _) = persister.create_or_load("0")?.into_inner();
        write.write_sync(&data[1..2])?;
        drop(write);
        drop(persister);

        // We can read back what we previously wrote.
        {
            let mut persister = registry.open("path", "restart-1")?;
            let (_, meta) = persister.create_or_load("0")?.into_inner();
            let snap = meta.snapshot()?;
            let mut actual = snap.read_to_end();
            actual.sort();
            assert_eq!(actual, data);
        }

        Ok(())
    }
}
