// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::error::Error;
use crate::pfuture::{PFuture, PFutureHandle};

/// A mechanism for externally waiting on dataflow progress.
#[derive(Clone, Debug)]
pub struct DataflowProgress {
    // When the rx side closes, it sets this bool to true before draining and
    // closing the channel. This is to handle the possibility of a race where a
    // new waiter is added after the channel drain but before its closed. For
    // us, this means if the bool is true, we immediately fill the returned
    // future with an error instead of attempting to write to the channel.
    closed: Arc<AtomicBool>,
    tx: crossbeam_channel::Sender<(u64, PFutureHandle<()>)>,
}

impl DataflowProgress {
    /// Returns a client for waiting on dataflow progress and the paired handle
    /// for transmitting it.
    pub fn new() -> (DataflowProgressHandle, DataflowProgress) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let closed = Arc::new(AtomicBool::new(false));
        (
            DataflowProgressHandle {
                core: Arc::new(Mutex::new(DataflowProgressCore {
                    rx,
                    closed: closed.clone(),
                    waiters: BTreeMap::new(),
                    frontier: Antichain::from_elem(0),
                })),
            },
            DataflowProgress { tx, closed },
        )
    }

    /// Returns a Future that is filled once the given timestamp is no longer in
    /// advance of the paired dataflow probe's frontier (i.e. returns/unblocks
    /// when `!dataflow.less_than(ts)`).
    pub fn wait(&self, ts: u64) -> PFuture<()> {
        let (tx, rx) = PFuture::new();
        if self.closed.load(Ordering::SeqCst) {
            tx.fill(Err(Error::RuntimeShutdown));
            return rx;
        }
        if let Err(err) = self.tx.send((ts, tx)) {
            let (_, tx) = err.into_inner();
            tx.fill(Err(Error::RuntimeShutdown));
        }
        rx
    }
}

/// The dataflow handle for a [DataflowProgress].
#[derive(Clone, Debug)]
pub struct DataflowProgressHandle {
    core: Arc<Mutex<DataflowProgressCore>>,
}

impl DataflowProgressHandle {
    /// Advance the internal frontier and unblock any relevant waiters.
    ///
    /// This is intended to be called in the dataflow step loop:
    ///
    /// ```
    /// let (progress_tx, progress_rx) = DataflowProgress::new();
    /// ...
    /// timely::execute(
    ///     timely::Config::process(workers),
    ///     move |worker| {
    ///         let dataflow_read = dataflow_read.clone();
    ///         let mut probe = ProbeHandle::new();
    ///         worker.dataflow(|scope| {
    ///             let out = ...;
    ///             out.probe_with(&mut probe);
    ///         })
    ///         while worker.step_or_park(None) {
    ///             probe.with_frontier(|frontier| {
    ///                 progress_tx.maybe_progress(frontier);
    ///             })
    ///         }
    ///         progress_tx.close();
    ///     }
    /// )
    /// ```
    pub fn maybe_progress(&self, frontier: AntichainRef<u64>) {
        if let Ok(mut core) = self.core.lock() {
            core.maybe_progress(frontier)
        }
    }

    /// Close down this [DataflowProgress] and fill any remaining waiters with an
    /// error.
    pub fn close(&self) {
        if let Ok(mut core) = self.core.lock() {
            core.close()
        }
    }
}

#[derive(Debug)]
struct DataflowProgressCore {
    // See the field documentation in DataflowProgress for the protocol on using
    // these.
    closed: Arc<AtomicBool>,
    rx: crossbeam_channel::Receiver<(u64, PFutureHandle<()>)>,

    waiters: BTreeMap<u64, Vec<PFutureHandle<()>>>,
    frontier: Antichain<u64>,
}

impl DataflowProgressCore {
    fn maybe_progress(&mut self, frontier: AntichainRef<u64>) {
        self.drain_rx();
        if PartialOrder::less_than(&self.frontier.borrow(), &frontier) {
            self.frontier.clone_from(&frontier.to_owned());
            let mut removed = Vec::new();
            for (wait_ts, waiters) in self.waiters.iter_mut() {
                if !frontier.less_than(wait_ts) {
                    removed.push(*wait_ts);
                    waiters.drain(..).for_each(|w| {
                        w.fill(Ok(()));
                    })
                } else {
                    // Since the timestamps are totally ordered, the
                    // BTreeMap iteration lets us stop here.
                    break;
                }
            }
            for ts in removed {
                self.waiters.remove(&ts);
            }
        }
    }

    fn drain_rx(&mut self) {
        for (wait_ts, waiter) in self.rx.try_iter() {
            if !self.frontier.less_than(&wait_ts) {
                waiter.fill(Ok(()));
            } else {
                self.waiters.entry(wait_ts).or_default().push(waiter);
            }
        }
    }

    fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        for (_wait_ts, waiter) in self.rx.try_iter() {
            waiter.fill(Err(Error::RuntimeShutdown));
        }
        for (_wait_ts, waiters) in self.waiters.iter_mut() {
            for waiter in waiters.drain(..) {
                waiter.fill(Err(Error::RuntimeShutdown));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_util::task::noop_waker_ref;

    use super::*;

    #[test]
    fn dataflow_progress() {
        let mut context = Context::from_waker(noop_waker_ref());
        let (tx, rx) = DataflowProgress::new();
        let mut w1 = Box::new(rx.wait(1));
        let mut w2 = Box::new(rx.wait(2));
        let mut w3 = Box::new(rx.wait(3));

        // No progress yet.
        assert_eq!(Pin::new(&mut w1).poll(&mut context), Poll::Pending);

        // Advance frontier to 2. This should unblock < and == but not >.
        tx.maybe_progress(Antichain::from_elem(2).borrow());
        assert_eq!(Pin::new(&mut w1).poll(&mut context), Poll::Ready(Ok(())));
        assert_eq!(Pin::new(&mut w2).poll(&mut context), Poll::Ready(Ok(())));
        assert_eq!(Pin::new(&mut w3).poll(&mut context), Poll::Pending);

        // A new wait on something already passed is available after the next
        // maybe_progress call, even if there wasn't progress.
        let mut w2 = Box::new(rx.wait(2));
        assert_eq!(Pin::new(&mut w2).poll(&mut context), Poll::Pending);
        tx.maybe_progress(Antichain::from_elem(2).borrow());
        assert_eq!(Pin::new(&mut w2).poll(&mut context), Poll::Ready(Ok(())));

        // Shutting down the dataflow unblocks the remaining waiter with an Err.
        tx.close();
        assert_eq!(
            Pin::new(&mut w3).poll(&mut context),
            Poll::Ready(Err(Error::RuntimeShutdown))
        );

        // With the dataflow shut down, newly registered waiters immediately get
        // an Err (sadly, they get an Err even if they would have immediately
        // returned an Ok before the shutdown, which is a bit odd, but the
        // current single user of this doesn't care so fixing it is not worth
        // the added complexity).
        let mut w2 = Box::new(rx.wait(2));
        let mut w3 = Box::new(rx.wait(3));
        assert_eq!(
            Pin::new(&mut w2).poll(&mut context),
            Poll::Ready(Err(Error::RuntimeShutdown))
        );
        assert_eq!(
            Pin::new(&mut w3).poll(&mut context),
            Poll::Ready(Err(Error::RuntimeShutdown))
        );
    }
}
