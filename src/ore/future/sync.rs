// Copyright 2016 Alex Crichton
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Portions of this file are derived from the Forward stream combinator in the
// futures project. The original source code was retrieved on September 4, 2019
// from:
//
//     https://github.com/rust-lang-nursery/futures-rs/blob/632838c87c7e338bce2cddf11e60ffdf9e2a9da6/src/stream/forward.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Extensions for future-aware synchronization from the [`futures::sync`]
//! module.

pub mod mpsc {
    //! Extensions for future-aware MPSC channels from the
    //! [`futures::sync::mpsc`] module.

    use futures::sink::SinkMapErr;
    use futures::stream::Fuse;
    use futures::sync::mpsc::{unbounded, SendError, UnboundedReceiver, UnboundedSender};
    use futures::{try_ready, Async, AsyncSink, Future, Poll, Sink, Stream};
    use std::thread::{self, Thread};
    use tokio::executor::{Executor, SpawnError};

    /// Extension methods for [`futures::sync::mpsc::UnboundedReceiver`].
    pub trait ReceiverExt {
        /// Request that the current thread be unparked whenever a message is
        /// sent over the channel.
        ///
        /// Though the returned receiver implements [`Send`], sending it to
        /// another thread will likely result in incorrect behavior, as the
        /// thread that called `request_unparks` will forever be the thread that
        /// is unparked, even if another thread holds the receiver.
        ///
        /// TODO(benesch): `impl !Send` once anti-traits land.
        fn request_unparks(self, executor: impl Executor) -> Result<Self, SpawnError>
        where
            Self: Sized;
    }

    impl<T> ReceiverExt for UnboundedReceiver<T>
    where
        T: Send + 'static,
    {
        fn request_unparks(self, mut executor: impl Executor) -> Result<Self, SpawnError> {
            let (tx, rx) = unbounded();
            executor.spawn(Box::new(UnparkingForward::new(tx, self, thread::current())))?;
            Ok(rx)
        }
    }

    // UnparkingForward adapts async channels for use with synchronous code.
    // Whenever a message is received on the contained `tx`, it is forwarded
    // to `rx`, and then the contained `thread` is unparked.
    #[derive(Debug)]
    #[must_use = "futures do nothing unless polled"]
    struct UnparkingForward<T> {
        tx: SinkMapErr<UnboundedSender<T>, fn(SendError<T>) -> ()>,
        rx: Fuse<UnboundedReceiver<T>>,
        buffered: Option<T>,
        thread: Thread,
    }

    impl<T> UnparkingForward<T> {
        fn new(
            tx: UnboundedSender<T>,
            rx: UnboundedReceiver<T>,
            thread: Thread,
        ) -> UnparkingForward<T> {
            UnparkingForward {
                // Send errors will cause this future to resolve, which will in
                // turn cause the upstream sender to receive a send error.
                tx: tx.sink_map_err(crate::future::discard),
                rx: rx.fuse(),
                buffered: None,
                thread,
            }
        }

        fn try_start_send(&mut self, item: T) -> Poll<(), ()> {
            debug_assert!(self.buffered.is_none());
            if let AsyncSink::NotReady(item) = self.tx.start_send(item)? {
                self.buffered = Some(item);
                return Ok(Async::NotReady);
            }
            self.thread.unpark();
            Ok(Async::Ready(()))
        }
    }

    impl<T> Future for UnparkingForward<T> {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<(), ()> {
            // If we've got an item buffered already, we need to write it to the
            // sink before we can do anything else
            if let Some(item) = self.buffered.take() {
                try_ready!(self.try_start_send(item))
            }

            loop {
                match self.rx.poll()? {
                    Async::Ready(Some(item)) => try_ready!(self.try_start_send(item)),
                    Async::Ready(None) => {
                        try_ready!(self.tx.close());
                        return Ok(Async::Ready(()));
                    }
                    Async::NotReady => {
                        try_ready!(self.tx.poll_complete());
                        return Ok(Async::NotReady);
                    }
                }
            }
        }
    }
}
