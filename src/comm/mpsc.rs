// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Multiple-producer, single-consumer (MPSC) channels.
//!
//! MPSC channels can be dynamically allocated from a [`Switchboard`]:
//!
//! ```
//! use comm::Switchboard;
//! use futures::{Future, Sink, Stream};
//! use tokio::net::UnixStream;
//!
//! let (switchboard, _runtime) = Switchboard::local()?;
//! let (tx, rx) = switchboard.mpsc();
//! std::thread::spawn(move || -> Result<(), bincode::Error> {
//!     // Do work.
//!     let answer = 42;
//!     tx.connect::<UnixStream>().wait()?.send(answer).wait()?;
//!     Ok(())
//! });
//! assert_eq!(rx.wait().next().transpose()?, Some(42));
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

use futures::{Future, Poll, Sink, Stream};
use ore::future::StreamExt;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;

use tokio::io;
use uuid::Uuid;

use crate::protocol;

/// The transmission end of an MPSC channel.
///
/// Unlike [`broadcast::Sender`][crate::broadcast::Sender], this sender must be
/// connected via [`connect`][Sender::connect] before it can be used. The
/// unconnected sender is more flexible, however, and implements [`Serialize`]
/// and [`Deserialize`] so that it can be sent to another process.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Sender<D> {
    addr: String,
    uuid: Uuid,
    _data: std::marker::PhantomData<D>,
}

impl<D> Sender<D> {
    pub(crate) fn new(addr: String, uuid: Uuid) -> Sender<D> {
        Sender {
            addr,
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
    pub fn connect<C>(
        &self,
    ) -> impl Future<Item = impl Sink<SinkItem = D, SinkError = bincode::Error>, Error = io::Error>
    where
        C: protocol::Connection,
        D: Serialize + Send,
        for<'de> D: Deserialize<'de>,
    {
        let uuid = self.uuid;
        C::connect(&self.addr)
            .and_then(move |conn| protocol::send_handshake(conn, uuid))
            .map(protocol::encoder)
    }
}

/// The receiving end of an MPSC channel.
///
/// See the [`futures::Stream`] documentation for details about the API for
/// receiving messages from a stream.
pub struct Receiver<D>(Box<dyn Stream<Item = D, Error = bincode::Error> + Send>);

impl<D> Receiver<D> {
    pub(crate) fn new<C>(conn_rx: impl Stream<Item = C, Error = ()> + Send + 'static) -> Receiver<D>
    where
        C: protocol::Connection,
        D: Serialize + Send + 'static,
        for<'de> D: Deserialize<'de>,
    {
        Receiver(Box::new(
            conn_rx
                .map_err(|_| -> bincode::Error { unreachable!() })
                .map(protocol::decoder)
                .select_flatten(),
        ))
    }
}

impl<D> Stream for Receiver<D> {
    type Item = D;
    type Error = bincode::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

impl<D> fmt::Debug for Receiver<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("mpsc receiver")
    }
}
