// Copyright 2016 Alex Crichton
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the Forward stream combinator in the
// futures project. The original source code was retrieved on September 4, 2019
// from:
//
//     https://github.com/rust-lang-nursery/futures-rs/blob/632838c87c7e338bce2cddf11e60ffdf9e2a9da6/src/stream/forward.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Extensions for future-aware channels from the [`futures::channel`]
//! module.

pub mod mpsc {
    //! Extensions for future-aware MPSC channels from the
    //! [`futures::channel::mpsc`] module.

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::thread::{self, Thread};

    use futures::channel::mpsc::{unbounded, SendError, UnboundedReceiver, UnboundedSender};
    use futures::stream::Fuse;
    use futures::{ready, FutureExt, Sink, Stream, StreamExt};

    /// Extension methods for [`futures::channel::mpsc::UnboundedReceiver`].
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
        fn request_unparks(self, executor: &tokio::runtime::Handle) -> Self
        where
            Self: Sized;
    }

    impl<T> ReceiverExt for UnboundedReceiver<T>
    where
        T: Unpin + Send + 'static,
    {
        fn request_unparks(self, executor: &tokio::runtime::Handle) -> Self {
            let (tx, rx) = unbounded();
            let fut = UnparkingForward::new(tx, self, thread::current()).map(|_| ());
            let _ = executor.spawn(fut);
            rx
        }
    }

    // UnparkingForward adapts async channels for use with synchronous code.
    // Whenever a message is received on the contained `tx`, it is forwarded
    // to `rx`, and then the contained `thread` is unparked.
    #[derive(Debug)]
    struct UnparkingForward<T> {
        tx: UnboundedSender<T>,
        rx: Fuse<UnboundedReceiver<T>>,
        buffered: Option<T>,
        thread: Thread,
    }

    impl<T> UnparkingForward<T>
    where
        T: Unpin,
    {
        fn new(
            tx: UnboundedSender<T>,
            rx: UnboundedReceiver<T>,
            thread: Thread,
        ) -> UnparkingForward<T> {
            UnparkingForward {
                tx,
                rx: rx.fuse(),
                buffered: None,
                thread,
            }
        }

        fn tx_pin(&mut self) -> Pin<&mut UnboundedSender<T>> {
            Pin::new(&mut self.tx)
        }

        fn rx_pin(&mut self) -> Pin<&mut Fuse<UnboundedReceiver<T>>> {
            Pin::new(&mut self.rx)
        }

        fn try_start_send(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            item: T,
        ) -> Poll<Result<(), SendError>> {
            debug_assert!(self.buffered.is_none());
            if self.tx_pin().poll_ready(cx).is_ready() {
                self.tx_pin().start_send(item)?;
                self.thread.unpark();
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        }
    }

    impl<T> Future for UnparkingForward<T>
    where
        T: Unpin,
    {
        type Output = Result<(), SendError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            // If we've got an item buffered already, we need to write it to the
            // sink before we can do anything else
            if let Some(item) = self.buffered.take() {
                ready!(self.as_mut().try_start_send(cx, item))?;
            }

            loop {
                match self.rx_pin().poll_next(cx) {
                    Poll::Ready(Some(item)) => ready!(self.as_mut().try_start_send(cx, item))?,
                    Poll::Ready(None) => {
                        let _ = ready!(self.tx_pin().poll_close(cx));
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => {
                        let _ = ready!(self.tx_pin().poll_flush(cx));
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}
