// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! The guts of the underlying network communication protocol.

use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use ore::netio::{SniffedStream, SniffingStream};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_serde_bincode::{ReadBincode, WriteBincode};
use uuid::Uuid;

/// A magic number that is sent along at the beginning of each network
/// connection. The intent is to make it easy to sniff out `comm` traffic when
/// multiple protocols are multiplexed on the same port.
pub const PROTOCOL_MAGIC: [u8; 8] = [0x5f, 0x65, 0x44, 0x90, 0xaf, 0x4b, 0x3c, 0xfc];

/// Reports whether the connection handshake is `comm` traffic by sniffing out
/// whether the first bytes of `buf` match [`PROTOCOL_MAGIC`].
///
/// See [`crate::Switchboard::handle_connection`] for a usage example.
pub fn match_handshake(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    buf[..8] == PROTOCOL_MAGIC
}

/// A trait for objects that can serve as the underlying transport layer for
/// `comm` traffic.
///
/// Only [`TcpStream`] and [`SniffedStream`] support is provided at the moment,
/// but support for any owned, thread-safe type which implements [`AsyncRead`]
/// and [`AsyncWrite`] can be added trivially, i.e., by implementing this trait.
pub trait Connection: AsyncRead + AsyncWrite + Send + Sized + 'static {
    /// Connects to the specified `addr`.
    fn connect(addr: &SocketAddr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send>;
}

impl Connection for TcpStream {
    fn connect(addr: &SocketAddr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send> {
        Box::new(TcpStream::connect(addr).map(|conn| {
            conn.set_nodelay(true).expect("set_nodelay call failed");
            conn
        }))
    }
}

impl<C> Connection for SniffedStream<C>
where
    C: Connection,
{
    fn connect(addr: &SocketAddr) -> Box<dyn Future<Item = Self, Error = io::Error> + Send> {
        Box::new(C::connect(addr).map(|conn| SniffingStream::new(conn).into_sniffed()))
    }
}

pub(crate) fn resolve_addr(addr: &str) -> Result<SocketAddr, io::Error> {
    match addr.to_socket_addrs() {
        // TODO(benesch): we should try connecting to all resolved
        // addresses, not just the first.
        Ok(mut addrs) => match addrs.next() {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "dns resolution failed",
            )),
            Some(addr) => Ok(addr),
        },
        Err(err) => Err(err),
    }
}

pub(crate) fn send_handshake<C>(conn: C, uuid: Uuid) -> SendHandshakeFuture<C>
where
    C: Connection,
{
    let mut buf = [0; 24];
    (&mut buf[..8]).copy_from_slice(&PROTOCOL_MAGIC);
    (&mut buf[8..]).copy_from_slice(uuid.as_bytes());
    SendHandshakeFuture {
        inner: io::write_all(conn, buf),
    }
}

pub(crate) struct SendHandshakeFuture<C> {
    inner: io::WriteAll<C, [u8; 24]>,
}

impl<C> Future for SendHandshakeFuture<C>
where
    C: Connection,
{
    type Item = C;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (stream, _buf) = try_ready!(self.inner.poll());
        Ok(Async::Ready(stream))
    }
}

pub(crate) fn recv_handshake<C>(conn: C) -> RecvHandshakeFuture<C>
where
    C: Connection,
{
    RecvHandshakeFuture {
        inner: io::read_exact(conn, [0; 24]),
    }
}

pub(crate) struct RecvHandshakeFuture<C>
where
    C: Connection,
{
    inner: io::ReadExact<C, [u8; 24]>,
}

impl<C> Future for RecvHandshakeFuture<C>
where
    C: Connection,
{
    type Item = (C, Uuid);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (stream, buf) = try_ready!(self.inner.poll());
        let uuid_bytes = &buf[8..];
        debug_assert_eq!(uuid_bytes.len(), 16);
        // Parsing a UUID only fails if the slice is not exactly 16 bytes, so
        // it's safe to unwrap here.
        let uuid = Uuid::from_slice(uuid_bytes).unwrap();
        Ok(Async::Ready((stream, uuid)))
    }
}

/// Constructs a [`Sink`] which encodes incoming `D`s using [bincode] and sends
/// them over the connection `conn` with a length prefix. Its dual is
/// [`decoder`].
///
/// [bincode]: https://crates.io/crates/bincode
pub(crate) fn encoder<C, D>(conn: C) -> impl Sink<SinkItem = D, SinkError = bincode::Error>
where
    C: Connection,
    D: Serialize + Send,
    for<'de> D: Deserialize<'de>,
{
    WriteBincode::new(FramedWrite::new(conn, LengthDelimitedCodec::new()).sink_from_err())
}

/// Constructs a [`Stream`] which decodes bincoded, length-prefixed `D`s from
/// the connection `conn`. Its dual is [`encoder`].
pub(crate) fn decoder<C, D>(conn: C) -> impl Stream<Item = D, Error = bincode::Error>
where
    C: Connection,
    D: Serialize + Send,
    for<'de> D: Deserialize<'de>,
{
    ReadBincode::new(FramedRead::new(conn, LengthDelimitedCodec::new()).from_err())
}
