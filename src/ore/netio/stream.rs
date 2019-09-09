// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt;
use std::io;

use futures::Poll;
use smallvec::{smallvec, SmallVec};
use tokio::io::{AsyncRead, AsyncWrite, Read, Write};

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
/// # fn handle_http<S: Read + Write>(stream: S) -> std::io::Result<()> { Ok(()) }
/// # fn handle_foo<S: Read + Write>(stream: S) -> std::io::Result<()> { Ok(()) }
/// # use std::io::{Read, Write};
/// # use ore::netio::SniffingStream;
///
/// fn handle_connection<S: Read + Write>(stream: S) -> std::io::Result<()> {
///     let mut ss = SniffingStream::new(stream);
///     let mut buf = [0; 4];
///     ss.read_exact(&mut buf)?;
///     if &buf == b"GET " {
///         handle_http(ss.into_sniffed())
///     } else {
///         handle_foo(ss.into_sniffed())
///     }
/// }
/// ```
pub struct SniffingStream<S> {
    inner: S,
    buf: SmallVec<[u8; INLINE_BUF_LEN]>,
    off: usize,
    len: usize,
}

impl<S> fmt::Debug for SniffingStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SniffingStream")
            .field("buf_size", &self.buf.len())
            .field("off", &self.off)
            .field("len", &self.len)
            .finish()
    }
}

impl<S> SniffingStream<S> {
    /// Creates a new `SniffingStream` that wraps an underlying stream.
    pub fn new(inner: S) -> SniffingStream<S> {
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
    pub fn into_sniffed(mut self) -> SniffedStream<S> {
        self.buf.truncate(self.len);
        SniffedStream {
            inner: self.inner,
            buf: self.buf,
            off: 0,
        }
    }
}

impl<R: Read> Read for SniffingStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.off == self.len {
            if self.len == self.buf.len() {
                self.buf.resize(self.buf.len() << 1, 0);
            }
            self.len += self.inner.read(&mut self.buf[self.len..])?;
        }
        let ncopied = (&self.buf[self.off..self.len]).read(buf)?;
        self.off += ncopied;
        Ok(ncopied)
    }
}

impl<W: Write> Write for SniffingStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<A: AsyncRead> AsyncRead for SniffingStream<A> {}

impl<A: AsyncWrite> AsyncWrite for SniffingStream<A> {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.inner.shutdown()
    }
}

/// A [`SniffingStream`] that has relinquished its capability to sniff bytes.
pub struct SniffedStream<S> {
    inner: S,
    buf: SmallVec<[u8; INLINE_BUF_LEN]>,
    off: usize,
}

impl<S> SniffedStream<S> {
    /// Consumes the `SniffedStream`, returning the underlying stream. Be very
    /// careful with this function! The underlying stream pointer will have been
    /// advanced past any bytes sniffed from the [`SniffingStream`] that created
    /// this [`SniffedStream`].
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> fmt::Debug for SniffedStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SniffedStream")
            .field("buf_size", &self.buf.len())
            .field("off", &self.off)
            .finish()
    }
}

impl<R: Read> Read for SniffedStream<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.off == self.buf.len() {
            return self.inner.read(buf);
        }
        let ncopied = (&self.buf[self.off..]).read(buf)?;
        self.off += ncopied;
        Ok(ncopied)
    }
}

impl<W: Write> Write for SniffedStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<A: AsyncRead> AsyncRead for SniffedStream<A> {}
impl<A: AsyncWrite> AsyncWrite for SniffedStream<A> {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.inner.shutdown()
    }
}
