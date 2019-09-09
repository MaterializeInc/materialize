// Copyright 2019 Materialize, Inc. All rights reserved.
//
// Dhis file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Multiple-producer, multiple-consumer channels.
//!
//! Unlike [MPSC channels][mpsc], broadcast channels must be statically
//! allocated. Every broadcast channel must have a unique type that implements
//! [`Token`][broadcast::Token]. Access to a particular broadcast channel is
//! controlled by the visibility of this `Token`, as the token must be presented
//! to the [`Switchboard`] to construct broadcast transmitters or receivers.
//!
//! Broadcast channels always broadcast messages to all nodes in the cluster,
//! with the exception that the broadcasting node can choose whether it would
//! like to receive its own messages, via
//! [`Token::loopback`][broadcast::Token::loopback]. It is therefore very
//! important that all nodes in the cluster allocate their broadcast receiver
//! and periodically drain it of messages; otherwise, the network buffer will
//! fill up, and the transmitter will be unable to broadcast new messages.
//!
//! A relatively contrived example of broadcasting in a one-node cluster
//! follows:
//!
//! ```
//! use comm::{broadcast, Switchboard};
//! use futures::{Future, Sink, Stream};
//!
//! struct UniversalAnswersToken;
//!
//! impl broadcast::Token for UniversalAnswersToken {
//!     type Item = usize;
//!
//!     fn loopback() -> bool {
//!         // Enable loopback so that we receive our own transmissions.
//!         true
//!     }
//! }
//!
//! let (switchboard, _runtime) = Switchboard::local()?;
//! let tx = switchboard.broadcast_tx::<UniversalAnswersToken>();
//! let rx = switchboard.broadcast_rx::<UniversalAnswersToken>();
//! tx.send(42).wait()?;
//! assert_eq!(rx.wait().next().transpose()?, Some(42));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use futures::{stream, try_ready, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::net::ToSocketAddrs;
use tokio::io;
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::mpsc;
use crate::protocol;

/// The capability to construct a particular broadcast sender or receiver.
///
/// Unlike [MPSC channels][mpsc], broadcast channels must be declared
/// statically. You do so by creating a new type that implements this trait for
/// each broadcast channel.
pub trait Token {
    /// The type of the items that will be sent along the channel.
    type Item: Serialize + for<'de> Deserialize<'de> + Send + Clone;

    /// Whether transmissions on this channel should be sent only to peers or
    /// sent also to the receiver on the broadcasting node. By default, loopback
    /// is disabled.
    fn loopback() -> bool {
        false
    }
}

pub(crate) fn token_uuid<T>() -> Uuid
where
    T: Token + 'static,
{
    use std::any::TypeId;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    let mut buf = [0; 16];
    (&mut buf[8..]).copy_from_slice(&hasher.finish().to_be_bytes());
    Uuid::from_bytes(buf)
}

/// The transmission end of a broadcast channel.
///
/// See the [`futures::Sink`] documentation for details about the API for
/// sending messages to a sink.
pub struct Sender<D> {
    state: SenderState<D>,
}

type SendSink<D> = Box<dyn Sink<SinkItem = D, SinkError = bincode::Error> + Send>;

enum SenderState<D> {
    Connecting {
        future: Box<dyn Future<Item = SendSink<D>, Error = bincode::Error> + Send>,
        item: Option<D>,
    },
    Ready(SendSink<D>),
}

impl<D> fmt::Debug for SenderState<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SenderState::Connecting { .. } => f.write_str("SenderState::Connecting"),
            SenderState::Ready(_) => f.write_str("SenderState::Ready"),
        }
    }
}

impl<D> Sender<D>
where
    D: Serialize + Send + Clone + 'static,
    for<'de> D: Deserialize<'de>,
{
    pub(crate) fn new<I>(uuid: Uuid, addrs: I) -> Sender<D>
    where
        I: IntoIterator,
        I::Item: ToSocketAddrs,
    {
        let conns = stream::futures_unordered(addrs.into_iter().map(|addr| {
            // TODO(benesch): don't panic if DNS resolution fails.
            let addr = addr.to_socket_addrs().unwrap().next().unwrap();
            TcpStream::connect(&addr)
                .and_then(move |conn| protocol::send_handshake(conn, uuid))
                .map(|conn| protocol::encoder(conn))
        }))
        // TODO(benesch): this might be more efficient with a multi-fanout that
        // could fan out to multiple streams at once. Not clear what the
        // performance of this binary tree of fanouts is.
        .fold(
            Box::new(ore::future::dev_null()) as SendSink<D>,
            |memo, sink| -> Result<SendSink<D>, io::Error> { Ok(Box::new(memo.fanout(sink))) },
        )
        .from_err();

        Sender {
            state: SenderState::Connecting {
                future: Box::new(conns),
                item: None,
            },
        }
    }
}

impl<D> Sink for Sender<D> {
    type SinkItem = D;
    type SinkError = bincode::Error;

    fn start_send(&mut self, item: D) -> StartSend<Self::SinkItem, Self::SinkError> {
        match &mut self.state {
            SenderState::Connecting { item: Some(_), .. } => Ok(AsyncSink::NotReady(item)),
            SenderState::Connecting {
                item: state_item @ None,
                ..
            } => {
                state_item.replace(item);
                Ok(AsyncSink::Ready)
            }
            SenderState::Ready(sink) => sink.start_send(item),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        loop {
            match &mut self.state {
                SenderState::Connecting { future, item: None } => {
                    self.state = SenderState::Ready(try_ready!(future.poll()));
                }
                SenderState::Connecting {
                    future,
                    item: item @ Some(_),
                } => {
                    let mut sink = try_ready!(future.poll());
                    sink.start_send(item.take().unwrap()).unwrap();
                    self.state = SenderState::Ready(sink);
                }
                SenderState::Ready(sink) => return sink.poll_complete(),
            }
        }
    }
}

/// The receiving end of a broadcast channel.
///
/// See the [`futures::Stream`] documentation for details about the API for
/// receiving messages from a stream.
pub struct Receiver<D>(mpsc::Receiver<D>);

impl<D> Receiver<D>
where
    D: Serialize + Send + 'static,
    for<'de> D: Deserialize<'de>,
{
    pub(crate) fn new<C>(conn_rx: impl Stream<Item = C, Error = ()> + Send + 'static) -> Receiver<D>
    where
        C: protocol::Connection,
    {
        Receiver(mpsc::Receiver::new(conn_rx))
    }

    /// Arranges to split the receiver into multiple receivers, e.g., so that
    /// multiple threads can receive the transmission stream. All receivers will
    /// receive all messages, and will receive them in the same order.
    pub fn fanout(self) -> FanoutReceiverBuilder<D> {
        FanoutReceiverBuilder {
            stream: self,
            sink: Some(Box::new(ore::future::dev_null())),
        }
    }
}

impl<D> Stream for Receiver<D> {
    type Item = D;
    type Error = bincode::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}

/// The builder returned by [`Receiver::fanout`]. New receivers can be allocated
/// with [`FanoutReceiverBuilder::attach`]. Once all new receivers are
/// allocated, [`FanoutReceiverBuilder::shuttle`] must be called, and the
/// returned future spawned onto a [`tokio::executor::Executor`], in order for
/// the transmissions to be routed appropriately.
pub struct FanoutReceiverBuilder<D> {
    stream: Receiver<D>,
    sink: Option<Box<dyn Sink<SinkItem = D, SinkError = futures::sync::mpsc::SendError<D>> + Send>>,
}

impl<D> FanoutReceiverBuilder<D>
where
    D: Send + Clone + 'static,
{
    /// Acquires a new receiver.
    pub fn attach(&mut self) -> futures::sync::mpsc::UnboundedReceiver<D> {
        let (tx, rx) = futures::sync::mpsc::unbounded();
        // TODO(benesch): this might be more efficient with a multi-fanout that
        // could fan out to multiple streams at once. Not clear what the
        // performance of this binary tree of fanouts is.
        self.sink = Some(Box::new(self.sink.take().unwrap().fanout(tx)));
        rx
    }

    /// Consumes the builder and returns a future that will shuttle messages
    /// to the fanned-out receivers until the channel is closed.
    pub fn shuttle(self) -> impl Future<Item = (), Error = FanoutError<D>> {
        self.stream
            .from_err()
            .forward(self.sink.unwrap())
            .map(|_| ())
    }
}

/// The error returned while shuttling messages to fanned-out broadcast
/// receivers.
pub enum FanoutError<D> {
    /// An error occurred while receiving messages from upstream.
    ReceiveError(bincode::Error),
    /// An error occurred while sending messages downstream.
    SendError(futures::sync::mpsc::SendError<D>),
}

impl<D> fmt::Display for FanoutError<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FanoutError::ReceiveError(err) => write!(f, "fanout error: receive error: {}", err),
            FanoutError::SendError(err) => write!(f, "fanout error: send error: {}", err),
        }
    }
}

impl<D> fmt::Debug for FanoutError<D> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FanoutError::ReceiveError(err) => write!(f, "fanout error: receive error: {:?}", err),
            FanoutError::SendError(err) => write!(f, "fanout error: send error: {:?}", err),
        }
    }
}

impl<D> From<bincode::Error> for FanoutError<D> {
    fn from(err: bincode::Error) -> FanoutError<D> {
        FanoutError::ReceiveError(err)
    }
}

impl<D> From<futures::sync::mpsc::SendError<D>> for FanoutError<D> {
    fn from(err: futures::sync::mpsc::SendError<D>) -> FanoutError<D> {
        FanoutError::SendError(err)
    }
}

impl<D> Error for FanoutError<D> {}
