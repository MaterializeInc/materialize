// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use futures::{try_ready, Async, Future, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use uuid::Uuid;

/// A magic number that is sent along at the beginning of each TCP connection.
/// The intent is to make it easy to sniff out `comm` traffic when multiple
/// protocols are multiplexed on the same port.
pub const PROTOCOL_MAGIC: [u8; 8] = [0x5f, 0x65, 0x44, 0x90, 0xaf, 0x4b, 0x3c, 0xfc];

/// Reports whether the connection handshake is `comm` traffic.
pub fn match_handshake(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    return &buf[..8] == PROTOCOL_MAGIC;
}

pub(crate) fn send_handshake(conn: TcpStream, uuid: Uuid) -> SendHandshakeFuture {
    conn.set_nodelay(true).expect("set_nodelay call failed");
    let mut buf = [0; 24];
    (&mut buf[..8]).copy_from_slice(&PROTOCOL_MAGIC);
    (&mut buf[8..]).copy_from_slice(uuid.as_bytes());
    SendHandshakeFuture {
        inner: io::write_all(conn, buf),
    }
}

pub(crate) struct SendHandshakeFuture {
    inner: io::WriteAll<TcpStream, [u8; 24]>,
}

impl Future for SendHandshakeFuture {
    type Item = TcpStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (stream, _buf) = try_ready!(self.inner.poll());
        Ok(Async::Ready(stream))
    }
}

pub(crate) fn recv_handshake<C>(conn: C) -> RecvHandshakeFuture<C>
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    RecvHandshakeFuture {
        inner: io::read_exact(conn, [0; 24]),
    }
}

pub(crate) struct RecvHandshakeFuture<C>
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    inner: io::ReadExact<C, [u8; 24]>,
}

impl<C> Future for RecvHandshakeFuture<C>
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    type Item = (C, Uuid);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (stream, buf) = try_ready!(self.inner.poll());
        // Parsing a UUID only fails if the slice is not exactly 16 bytes, so
        // it's safe to unwrap.
        let uuid = Uuid::from_slice(&buf[8..]).unwrap();
        Ok(Async::Ready((stream, uuid)))
    }
}
