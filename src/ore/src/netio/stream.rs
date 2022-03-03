// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::ready;
use smallvec::{smallvec, SmallVec};
use tokio::io::{self, AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};

use crate::netio::AsyncReady;

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
/// # use mz_ore::netio::SniffingStream;
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
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let me = &mut *self;
        if me.off == me.len {
            if me.len == me.buf.len() {
                me.buf.resize(me.buf.len() << 1, 0);
            }
            let mut buf = ReadBuf::new(&mut me.buf[me.len..]);
            ready!(Pin::new(&mut me.inner).poll_read(cx, &mut buf))?;
            me.len += buf.filled().len();
        }
        let n = cmp::min(me.len - me.off, buf.remaining());
        buf.put_slice(&me.buf[me.off..me.off + n]);
        me.off += n;
        Poll::Ready(Ok(()))
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

#[async_trait]
impl<A> AsyncReady for SniffingStream<A>
where
    A: AsyncReady + Send + Sync,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.ready(interest).await
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

    /// Returns a reference to the bytes that were sniffed at the beginning of
    /// the stream.
    pub fn sniff_buffer(&self) -> &[u8] {
        &self.buf
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
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        if self.off == self.buf.len() {
            return self.inner_pin().poll_read(cx, buf);
        }
        let n = cmp::min(self.buf.len() - self.off, buf.remaining());
        buf.put_slice(&self.buf[self.off..self.off + n]);
        self.off += n;
        Poll::Ready(Ok(()))
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

#[async_trait]
impl<A> AsyncReady for SniffedStream<A>
where
    A: AsyncReady + Send + Sync,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.ready(interest).await
    }
}
