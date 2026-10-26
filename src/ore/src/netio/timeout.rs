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
use tokio::time::{Instant, Sleep};

#[derive(Debug)]
struct IoTimeout {
    duration: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl IoTimeout {
    fn new(duration: Duration) -> Self {
        Self {
            duration,
            sleep: None,
        }
    }

    fn poll<T>(
        &mut self,
        cx: &mut Context<'_>,
        poll_io: impl FnOnce(&mut Context<'_>) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        // Record the deadline before polling I/O, but only register a timer if
        // I/O blocks. Keep Tokio's zero-duration and overflowing-duration behavior.
        let deadline = if self.sleep.is_none() {
            let deadline = Instant::now().checked_add(self.duration);
            if self.duration.is_zero() || deadline.is_none() {
                self.sleep = Some(Box::pin(tokio::time::sleep(self.duration)));
            }
            deadline
        } else {
            None
        };

        // An expired active timer wins even if I/O has become ready.
        if let Some(sleep) = &mut self.sleep {
            if sleep.as_mut().poll(cx).is_ready() {
                self.sleep = None;
                return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
            }
        }

        let poll = poll_io(cx);
        if poll.is_ready() {
            self.sleep = None;
            return poll;
        }

        if self.sleep.is_none() {
            let mut sleep = Box::pin(tokio::time::sleep_until(
                deadline.expect("new I/O deadline"),
            ));
            if sleep.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
            }
            self.sleep = Some(sleep);
        }
        Poll::Pending
    }
}

/// An [`AsyncRead`] wrapper that enforces a timeout on each read.
#[derive(Debug)]
pub struct TimedReader<R> {
    reader: R,
    timeout: IoTimeout,
}

impl<R> TimedReader<R> {
    /// Wrap a reader with a timeout.
    pub fn new(reader: R, timeout: Duration) -> Self {
        Self {
            reader,
            timeout: IoTimeout::new(timeout),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TimedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.timeout
            .poll(cx, |cx| Pin::new(&mut this.reader).poll_read(cx, buf))
    }
}

/// An [`AsyncWrite`] wrapper that enforces a timeout on each write.
#[derive(Debug)]
pub struct TimedWriter<W> {
    writer: W,
    timeout: IoTimeout,
}

impl<W> TimedWriter<W> {
    /// Wrap a writer with a timeout.
    pub fn new(writer: W, timeout: Duration) -> Self {
        Self {
            writer,
            timeout: IoTimeout::new(timeout),
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TimedWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        this.timeout
            .poll(cx, |cx| Pin::new(&mut this.writer).poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        this.timeout
            .poll(cx, |cx| Pin::new(&mut this.writer).poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        this.timeout
            .poll(cx, |cx| Pin::new(&mut this.writer).poll_shutdown(cx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;

    #[crate::test]
    fn ready_io_does_not_create_a_timer() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut reader = TimedReader::new(tokio::io::empty(), Duration::from_secs(1));
        let mut bytes = [0; 8];
        let mut buf = ReadBuf::new(&mut bytes);
        assert!(
            Pin::new(&mut reader)
                .poll_read(&mut cx, &mut buf)
                .is_ready()
        );
        assert_eq!(buf.filled().len(), 0);
        assert!(reader.timeout.sleep.is_none());
        let mut writer = TimedWriter::new(tokio::io::sink(), Duration::from_secs(1));
        assert!(matches!(
            Pin::new(&mut writer).poll_write(&mut cx, b"hello"),
            Poll::Ready(Ok(5))
        ));
        assert!(writer.timeout.sleep.is_none());
        assert!(matches!(
            Pin::new(&mut writer).poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(writer.timeout.sleep.is_none());
        assert!(matches!(
            Pin::new(&mut writer).poll_shutdown(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(writer.timeout.sleep.is_none());
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn pending_repoll_keeps_deadline_and_timeout_wins_over_ready_io() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut timeout = IoTimeout::new(Duration::from_millis(10));
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        let deadline = timeout.sleep.as_ref().unwrap().deadline();
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        assert_eq!(timeout.sleep.as_ref().unwrap().deadline(), deadline);
        tokio::time::advance(Duration::from_millis(6)).await;
        let result = timeout.poll::<()>(&mut cx, |_| panic!("expired timeout must win"));
        assert!(matches!(result, Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::TimedOut));
        assert!(timeout.sleep.is_none());
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn completion_and_error_clear_timer_and_next_operation_gets_new_deadline() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut timeout = IoTimeout::new(Duration::from_secs(1));
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        let first_deadline = timeout.sleep.as_ref().unwrap().deadline();
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(
            timeout.poll(&mut cx, |_| Poll::Ready(Ok(42))),
            Poll::Ready(Ok(42))
        ));
        assert!(timeout.sleep.is_none());
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        assert!(timeout.sleep.as_ref().unwrap().deadline() > first_deadline);
        let error = timeout.poll::<()>(&mut cx, |_| {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        });
        assert!(matches!(error, Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::BrokenPipe));
        assert!(timeout.sleep.is_none());
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        drop(timeout);
        tokio::time::advance(Duration::from_secs(2)).await;
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn deadline_starts_before_first_io_poll() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut timeout = IoTimeout::new(Duration::from_secs(1));
        let start = Instant::now();
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        assert_eq!(
            timeout.sleep.as_ref().unwrap().deadline(),
            start + Duration::from_secs(1)
        );
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn zero_timeout_matches_tokio_sleep_priority() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut reference = Box::pin(tokio::time::sleep(Duration::ZERO));
        let expected_timeout = reference.as_mut().poll(&mut cx).is_ready();
        let mut timeout = IoTimeout::new(Duration::ZERO);
        let mut polled_io = false;
        let actual = timeout.poll(&mut cx, |_| {
            polled_io = true;
            Poll::Ready(Ok(()))
        });
        assert_eq!(!polled_io, expected_timeout);
        assert_eq!(
            matches!(actual, Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::TimedOut),
            expected_timeout
        );
        assert!(timeout.sleep.is_none());
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn overflowing_duration_preserves_tokio_far_future() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut timeout = IoTimeout::new(Duration::MAX);
        assert!(timeout.poll::<()>(&mut cx, |_| Poll::Pending).is_pending());
        assert!(matches!(
            timeout.poll(&mut cx, |_| Poll::Ready(Ok(()))),
            Poll::Ready(Ok(()))
        ));
        assert!(timeout.sleep.is_none());
    }

    struct ScriptedIo {
        ready: bool,
        polls: usize,
    }

    impl AsyncRead for ScriptedIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.polls += 1;
            if self.ready {
                buf.put_slice(b"x");
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
    }

    impl AsyncWrite for ScriptedIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.polls += 1;
            if self.ready {
                Poll::Ready(Ok(buf.len()))
            } else {
                Poll::Pending
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.polls += 1;
            if self.ready {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn pending_read_completes_before_deadline_and_clears_timer() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut reader = TimedReader::new(
            ScriptedIo {
                ready: false,
                polls: 0,
            },
            Duration::from_millis(10),
        );
        let mut bytes = [0; 8];
        let mut buf = ReadBuf::new(&mut bytes);
        assert!(
            Pin::new(&mut reader)
                .poll_read(&mut cx, &mut buf)
                .is_pending()
        );
        tokio::time::advance(Duration::from_millis(5)).await;
        reader.reader.ready = true;
        assert!(matches!(
            Pin::new(&mut reader).poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(buf.filled(), b"x");
        assert!(reader.timeout.sleep.is_none());
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn timeout_wakes_pending_io_without_an_io_wakeup() {
        use tokio::io::AsyncReadExt;

        let mut reader = TimedReader::new(
            ScriptedIo {
                ready: false,
                polls: 0,
            },
            Duration::from_millis(10),
        );
        let start = Instant::now();
        let mut bytes = [0; 8];
        let error = reader.read(&mut bytes).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(Instant::now() >= start + Duration::from_millis(10));
        assert!(reader.timeout.sleep.is_none());
    }

    #[crate::test]
    fn immediate_error_does_not_create_a_timer() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut timeout = IoTimeout::new(Duration::from_secs(1));
        let result = timeout.poll::<()>(&mut cx, |_| {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        });
        assert!(matches!(result, Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::BrokenPipe));
        assert!(timeout.sleep.is_none());
    }

    #[crate::test(tokio::test(start_paused = true))]
    async fn writer_transitions_preserve_pending_deadline_and_reset_on_completion() {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut writer = TimedWriter::new(
            ScriptedIo {
                ready: false,
                polls: 0,
            },
            Duration::from_millis(10),
        );
        assert!(
            Pin::new(&mut writer)
                .poll_write(&mut cx, b"hello")
                .is_pending()
        );
        let deadline = writer.timeout.sleep.as_ref().unwrap().deadline();
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(Pin::new(&mut writer).poll_flush(&mut cx).is_pending());
        assert_eq!(writer.timeout.sleep.as_ref().unwrap().deadline(), deadline);
        writer.writer.ready = true;
        assert!(matches!(
            Pin::new(&mut writer).poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(writer.timeout.sleep.is_none());
        writer.writer.ready = false;
        assert!(Pin::new(&mut writer).poll_shutdown(&mut cx).is_pending());
        assert!(writer.timeout.sleep.as_ref().unwrap().deadline() > deadline);
        tokio::time::advance(Duration::from_millis(11)).await;
        writer.writer.ready = true;
        let polls = writer.writer.polls;
        let result = Pin::new(&mut writer).poll_shutdown(&mut cx);
        assert!(matches!(result, Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::TimedOut));
        assert_eq!(writer.writer.polls, polls);
        assert!(writer.timeout.sleep.is_none());
    }
}
