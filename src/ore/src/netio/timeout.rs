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

//! `Async{Read,Write}` wrappers that enforce a configurable timeout on each I/O operation.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

/// An [`AsyncRead`] wrapper that enforces a timeout on each read.
#[derive(Debug)]
pub struct TimedReader<R> {
    reader: R,
    timeout: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<R> TimedReader<R> {
    /// Wrap a reader with a timeout.
    pub fn new(reader: R, timeout: Duration) -> Self {
        Self {
            reader,
            timeout,
            sleep: None,
        }
    }

    /// Poll the sleep future, creating it if necessary.
    fn poll_sleep(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let timeout = self.timeout;
        let sleep = self.sleep.get_or_insert_with(|| {
            let sleep = tokio::time::sleep(timeout);
            Box::pin(sleep)
        });

        sleep.as_mut().poll(cx)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TimedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let poll = if self.poll_sleep(cx).is_ready() {
            Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
        } else if let Poll::Ready(result) = Pin::new(&mut self.reader).poll_read(cx, buf) {
            Poll::Ready(result)
        } else {
            Poll::Pending
        };

        if poll.is_ready() {
            self.sleep = None;
        }

        poll
    }
}

/// An [`AsyncWrite`] wrapper that enforces a timeout on each write.
#[derive(Debug)]
pub struct TimedWriter<W> {
    writer: W,
    timeout: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<W> TimedWriter<W> {
    /// Wrap a writer with a timeout.
    pub fn new(writer: W, timeout: Duration) -> Self {
        Self {
            writer,
            timeout,
            sleep: None,
        }
    }

    /// Poll the sleep future, creating it if necessary.
    fn poll_sleep(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let timeout = self.timeout;
        let sleep = self.sleep.get_or_insert_with(|| {
            let sleep = tokio::time::sleep(timeout);
            Box::pin(sleep)
        });

        sleep.as_mut().poll(cx)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TimedWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let poll = if self.poll_sleep(cx).is_ready() {
            Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
        } else if let Poll::Ready(result) = Pin::new(&mut self.writer).poll_write(cx, buf) {
            Poll::Ready(result)
        } else {
            Poll::Pending
        };

        if poll.is_ready() {
            self.sleep = None;
        }

        poll
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let poll = if self.poll_sleep(cx).is_ready() {
            Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
        } else if let Poll::Ready(result) = Pin::new(&mut self.writer).poll_flush(cx) {
            Poll::Ready(result)
        } else {
            Poll::Pending
        };

        if poll.is_ready() {
            self.sleep = None;
        }

        poll
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let poll = if self.poll_sleep(cx).is_ready() {
            Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
        } else if let Poll::Ready(result) = Pin::new(&mut self.writer).poll_shutdown(cx) {
            Poll::Ready(result)
        } else {
            Poll::Pending
        };

        if poll.is_ready() {
            self.sleep = None;
        }

        poll
    }
}
