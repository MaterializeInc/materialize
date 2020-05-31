// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Multiple-producer, single-consumer (MPSC) channels.
//!
//! MPSC channels can be dynamically allocated from a [`Switchboard`]:
//!
//! ```
//! use comm::Switchboard;
//! use futures::sink::SinkExt;
//! use futures::stream::StreamExt;
//! use tokio::net::UnixStream;
//!
//! let (switchboard, mut runtime) = Switchboard::local()?;
//! let (tx, mut rx) = switchboard.mpsc();
//! runtime.spawn(async move {
//!     // Do work.
//!     let answer = 42;
//!     tx.connect().await?.send(answer).await?;
//!     Ok::<_, comm::Error>(())
//! });
//! assert_eq!(runtime.block_on(rx.next()).transpose()?, Some(42));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Unconnected senders are quite flexible. They implement
//! [`Serialize`][serde::Serialize] and [`Deserialize`][serde::Deserialize] so
//! that they can be sent over the network to other processes, or [`Clone`]d and
//! shared with other threads in the same process.
//!
//! Receivers and connected senders are less flexible, but still implement
//! [`Send`] and so can be freely sent between threads.

use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io;
use tokio::net::{TcpStream, UnixStream};
use uuid::Uuid;

use ore::future::{OreFutureExt, OreStreamExt};

use crate::error::Error;
use crate::protocol::{self, Addr, SendSink};
use crate::switchboard::Switchboard;

/// The transmission end of an MPSC channel.
///
/// Unlike [`broadcast::Sender`][crate::broadcast::Sender], this sender must be
/// connected via [`connect`][Sender::connect] before it can be used. The
/// unconnected sender is more flexible, however, and implements [`Serialize`]
/// and [`Deserialize`] so that it can be sent to another process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sender<D> {
    addr: Addr,
    uuid: Uuid,
    _data: std::marker::PhantomData<D>,
}

impl<D> Sender<D> {
    pub(crate) fn new(addr: impl Into<Addr>, uuid: Uuid) -> Sender<D> {
        Sender {
            addr: addr.into(),
            uuid,
            _data: PhantomData,
        }
    }

    /// Connects this `Sender` to the receiver so that transmissions can begin.
    ///
    /// The returned future resolves to a sink. Messages pushed into the sink
    /// will be delivered to the receiving end of this channel, potentially on
    /// another process. See the [`futures::Sink`] documentation for details
    /// about the API for sending messages to a sink.
    pub fn connect(&self) -> impl Future<Output = Result<SendSink<D>, io::Error>>
    where
        D: Serialize + for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        match &self.addr {
            Addr::Tcp(addr) => {
                protocol::connect_channel::<TcpStream, _>(addr.clone(), self.uuid).left()
            }
            Addr::Unix(addr) => {
                protocol::connect_channel::<UnixStream, _>(addr.clone(), self.uuid).right()
            }
        }
    }
}

/// The receiving end of an MPSC channel.
///
/// See the [`futures::Stream`] documentation for details about the API for
/// receiving messages from a stream.
pub struct Receiver<D>(
    Box<dyn Stream<Item = Result<D, Error>> + Unpin + Send>,
    Option<Box<dyn FnOnce() + Send>>,
);

impl<D> Receiver<D> {
    pub(crate) fn new<C>(
        conn_rx: impl Stream<Item = protocol::Framed<C>> + Send + Unpin + 'static,
        switchboard: Switchboard<C>,
        on_drop: Option<Box<dyn FnOnce() + Send>>,
    ) -> Receiver<D>
    where
        C: protocol::Connection,
        D: Serialize + for<'de> Deserialize<'de> + Send + Unpin + 'static,
    {
        Receiver(
            Box::new(
                conn_rx
                    .map(move |conn| protocol::decoder(conn, switchboard.clone()))
                    .select_flatten(),
            ),
            on_drop,
        )
    }
}

impl<D> Stream for Receiver<D> {
    type Item = Result<D, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

impl<D> fmt::Debug for Receiver<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("mpsc receiver")
    }
}

impl<D> Drop for Receiver<D> {
    fn drop(&mut self) {
        if let Some(f) = self.1.take() {
            f();
        }
    }
}
