// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use smallvec::{smallvec, SmallVec};
use tokio::io::{AsyncRead, AsyncWrite};

const INLINE_BUF_LEN: usize = 8;

/// A stream that can peek at its initial bytes without consuming them.
///
/// `SniffingStream`s have rather high overhead while sniffing, as they must
/// double-buffer every read to support rewinding to the beginning of the
/// stream.
///
/// `SniffingStream`s are designed to enable multiplexing multiple protocols on
/// the same [`TcpStream`](std::net::TcpStream). Determining which protocol the
/// client expects requires reading several bytes from the stream, but the
/// eventual protocol handler will be very confused if it receives a stream
/// whose read pointer has been advanced by several bytes. A `SniffingStream`
/// can easily be rewound to the beginning, via [`into_sniffed`], before it is
/// presented to the protocol handler.
///
/// [`into_sniffed`]: SniffingStream::into_sniffed
///
/// # Examples
///
/// ```
/// # fn handle_http<S>(stream: S) -> std::io::Result<()> { Ok(()) }
/// # fn handle_foo<S>(stream: S) -> std::io::Result<()> { Ok(()) }
/// # use tokio::io::{AsyncRead, AsyncReadExt};
/// # use ore::netio::SniffingStream;
///
/// async fn handle_connection<S>(stream: S) -> std::io::Result<()>
/// where
///     S: AsyncRead + Unpin
/// {
///     let mut ss = SniffingStream::new(stream);
///     let mut buf = [0; 4];
///     ss.read_exact(&mut buf).await?;
///     if &buf == b"GET " {
///         handle_http(ss.into_sniffed())
///     } else {
///         handle_foo(ss.into_sniffed())
///     }
/// }
/// ```
pub struct SniffingStream<A> {
    inner: A,
    buf: SmallVec<[u8; INLINE_BUF_LEN]>,
    off: usize,
    len: usize,
}

impl<A> fmt::Debug for SniffingStream<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SniffingStream")
            .field("buf_size", &self.buf.len())
            .field("off", &self.off)
            .field("len", &self.len)
            .finish()
    }
}

impl<A> SniffingStream<A>
where
    A: Unpin,
{
    /// Creates a new `SniffingStream` that wraps an underlying stream.
    pub fn new(inner: A) -> SniffingStream<A> {
        SniffingStream {
            inner,
            buf: smallvec![0; INLINE_BUF_LEN],
            off: 0,
            len: 0,
        }
    }

    /// Produces a `SniffedStream` that will yield bytes from the beginning of
    /// the underlying stream, including the bytes that were previously yielded
    /// by the `SniffingStream`.
    ///
    /// The `SniffingStream` is consumed by this method.
    pub fn into_sniffed(mut self) -> SniffedStream<A> {
        self.buf.truncate(self.len);
        SniffedStream {
            inner: self.inner,
            buf: self.buf,
            off: 0,
        }
    }

    fn inner_pin(&mut self) -> Pin<&mut A> {
        Pin::new(&mut self.inner)
    }
}

impl<A> AsyncRead for SniffingStream<A>
where
    A: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        let me = &mut *self;
        if me.off == me.len {
            if me.len == me.buf.len() {
                me.buf.resize(me.buf.len() << 1, 0);
            }
            me.len += ready!(Pin::new(&mut me.inner).poll_read(cx, &mut me.buf[me.len..]))?;
        }
        let ncopied = (&me.buf[me.off..me.len]).read(buf)?;
        me.off += ncopied;
        Poll::Ready(Ok(ncopied))
    }
}

impl<A> AsyncWrite for SniffingStream<A>
where
    A: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.inner_pin().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.inner_pin().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.inner_pin().poll_shutdown(cx)
    }
}

/// A [`SniffingStream`] that has relinquished its capability to sniff bytes.
pub struct SniffedStream<A> {
    inner: A,
    buf: SmallVec<[u8; INLINE_BUF_LEN]>,
    off: usize,
}

impl<A> SniffedStream<A>
where
    A: Unpin,
{
    /// Returns a reference to the underlying stream.
    pub fn get_ref(&self) -> &A {
        &self.inner
    }

    /// Consumes the `SniffedStream`, returning the underlying stream. Be very
    /// careful with this function! The underlying stream pointer will have been
    /// advanced past any bytes sniffed from the [`SniffingStream`] that created
    /// this [`SniffedStream`].
    pub fn into_inner(self) -> A {
        self.inner
    }

    fn inner_pin(&mut self) -> Pin<&mut A> {
        Pin::new(&mut self.inner)
    }
}

impl<A> fmt::Debug for SniffedStream<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SniffedStream")
            .field("buf_size", &self.buf.len())
            .field("off", &self.off)
            .finish()
    }
}

impl<A> AsyncRead for SniffedStream<A>
where
    A: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.off == self.buf.len() {
            return self.inner_pin().poll_read(cx, buf);
        }
        let ncopied = (&self.buf[self.off..]).read(buf)?;
        self.off += ncopied;
        Poll::Ready(Ok(ncopied))
    }
}

impl<A> AsyncWrite for SniffedStream<A>
where
    A: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.inner_pin().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.inner_pin().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.inner_pin().poll_shutdown(cx)
    }
}
