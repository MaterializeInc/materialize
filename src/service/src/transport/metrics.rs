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
use std::time::UNIX_EPOCH;

use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropGauge, MetricsRegistry, UIntGaugeVec};
use prometheus::core::AtomicU64;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::error;

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

/// Metrics for a cluster (CTP) server.
pub struct ClusterServerMetrics {
    last_command_received: UIntGaugeVec,
}

impl ClusterServerMetrics {
    /// Registers the cluster server metrics into a `registry`.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            last_command_received: registry.register(metric!(
                name: "mz_cluster_server_last_command_received",
                help: "The time (in seconds since the Unix epoch) at which the server last \
                       received data from the controller, including CTP keepalives. Used to \
                       detect controller connections that are no longer reachable.",
                var_labels: ["server_name"],
            )),
        }
    }

    /// Returns the metrics for the server with the given name.
    pub fn for_server(&self, name: &'static str) -> PerClusterServerMetrics {
        PerClusterServerMetrics {
            last_command_received: self
                .last_command_received
                .get_delete_on_drop_metric(vec![name]),
        }
    }
}

/// Metrics for a single named cluster (CTP) server.
#[derive(Clone, Debug)]
pub struct PerClusterServerMetrics {
    last_command_received: DeleteOnDropGauge<AtomicU64, Vec<&'static str>>,
}

impl<Out, In> Metrics<Out, In> for PerClusterServerMetrics {
    fn bytes_sent(&mut self, _len: usize) {}

    fn bytes_received(&mut self, _len: usize) {
        // We bump the "last command received" timestamp on any inbound bytes, not just on fully
        // decoded command messages (`message_received`). CTP exchanges keepalives on otherwise
        // idle connections, and those show up here but never as messages (empty keepalive frames
        // are dropped before decoding). Counting them as activity makes this metric a
        // connection-liveness signal: it stays fresh as long as the controller connection is
        // healthy and only goes stale once the connection is actually broken. Bumping it only on
        // command messages instead produces false positives on healthy-but-idle clusters that
        // don't receive a command for a while.
        match UNIX_EPOCH.elapsed() {
            Ok(ts) => self.last_command_received.set(ts.as_secs()),
            Err(e) => error!("failed to get system time: {e}"),
        }
    }

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
