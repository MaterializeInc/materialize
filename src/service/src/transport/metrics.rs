// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics support for the Cluster Transport Protocol.

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A trait for types that observe connection metric events.
pub trait Metrics<Out, In>: Clone + Send + 'static {
    /// Callback reporting numbers of bytes sent.
    fn bytes_sent(&mut self, len: usize);
    /// Callback reporting numbers of bytes received.
    fn bytes_received(&mut self, len: usize);
    /// Callback reporting messages sent.
    fn message_sent(&mut self, msg: &Out);
    /// Callback reporting messages received.
    fn message_received(&mut self, msg: &In);
}

/// No-op [`Metrics`] implementation that ignores all events.
#[derive(Clone)]
pub struct NoopMetrics;

impl<Out, In> Metrics<Out, In> for NoopMetrics {
    fn bytes_sent(&mut self, _len: usize) {}
    fn bytes_received(&mut self, _len: usize) {}
    fn message_sent(&mut self, _msg: &Out) {}
    fn message_received(&mut self, _msg: &In) {}
}

/// [`AsyncRead`] wrapper that transparently logs `bytes_received` metrics.
#[pin_project::pin_project]
pub(super) struct Reader<R, M, Out, In> {
    #[pin]
    reader: R,
    metrics: M,
    _phantom: PhantomData<(Out, In)>,
}

impl<R, M, Out, In> Reader<R, M, Out, In> {
    pub fn new(reader: R, metrics: M) -> Self {
        Self {
            reader,
            metrics,
            _phantom: PhantomData,
        }
    }
}

impl<R, M, Out, In> AsyncRead for Reader<R, M, Out, In>
where
    R: AsyncRead,
    M: Metrics<Out, In>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let initial_len = buf.filled().len();

        let this = self.project();
        let poll = this.reader.poll_read(cx, buf);

        let len = buf.filled().len() - initial_len;
        if len > 0 {
            this.metrics.bytes_received(len);
        }

        poll
    }
}

/// [`AsyncWrite`] wrapper that transparently logs `bytes_sent` metrics.
#[pin_project::pin_project]
pub(super) struct Writer<W, M, Out, In> {
    #[pin]
    writer: W,
    metrics: M,
    _phantom: PhantomData<(Out, In)>,
}

impl<W, M, Out, In> Writer<W, M, Out, In> {
    pub fn new(writer: W, metrics: M) -> Self {
        Self {
            writer,
            metrics,
            _phantom: PhantomData,
        }
    }
}

impl<W, M, Out, In> AsyncWrite for Writer<W, M, Out, In>
where
    W: AsyncWrite,
    M: Metrics<Out, In>,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        let poll = this.writer.poll_write(cx, buf);

        if let Poll::Ready(Ok(len)) = &poll
            && *len > 0
        {
            this.metrics.bytes_sent(*len);
        }

        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_shutdown(cx)
    }
}
