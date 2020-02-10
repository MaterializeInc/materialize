// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Communication utilities.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io;
use tokio::time::{Delay, Duration, Instant, Timeout};

use crate::protocol;

/// A future that resolves once a connection can be established or a timeout
/// elapses.
///
/// Under the hood, the future repeatedly opens new TCP connections, with
/// exponential backoff between each attempt, until a TCP connection succeeds.
pub struct TryConnectFuture<C>
where
    C: protocol::Connection,
{
    addr: C::Addr,
    deadline: Instant,
    backoff: Duration,
    state: TryConnectFutureState<C>,
}

impl<C> TryConnectFuture<C>
where
    C: protocol::Connection,
{
    /// Constructs a new `TryConnectFuture` that will attempt to connect to
    /// `addr` until the connection succeeds or the timeout elapses.
    pub fn new(addr: C::Addr, timeout: impl Into<Option<Duration>>) -> TryConnectFuture<C> {
        let deadline = match timeout.into() {
            // If no deadline, set one of approximately forever. Don't increase
            // this value beyond one year, though, or you'll run into
            // https://github.com/tokio-rs/tokio/issues/1953.
            None => Instant::now() + Duration::from_secs(60 * 60 * 24 * 365),
            Some(timeout) => Instant::now() + timeout,
        };
        let state = TryConnectFutureState::connect(addr.clone(), deadline);
        TryConnectFuture {
            addr,
            deadline,
            backoff: Duration::from_millis(100),
            state,
        }
    }
}

impl<C> Future for TryConnectFuture<C>
where
    C: protocol::Connection,
{
    type Output = Result<C, io::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                TryConnectFutureState::Connecting(future) => match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(Ok(conn))) => {
                        return Poll::Ready(Ok(conn));
                    }
                    Poll::Ready(Ok(Err(_))) => {
                        if self.backoff < Duration::from_secs(1) {
                            self.backoff *= 2;
                        }
                        self.state = TryConnectFutureState::sleep(self.backoff, self.deadline);
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "connection attempt timed out",
                        )));
                    }
                },
                TryConnectFutureState::Sleeping(future) => match Pin::new(future).poll(cx) {
                    Poll::Ready(()) => {
                        self.state =
                            TryConnectFutureState::connect(self.addr.clone(), self.deadline)
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

enum TryConnectFutureState<C> {
    Connecting(Timeout<Pin<Box<dyn Future<Output = Result<C, io::Error>> + Send>>>),
    Sleeping(Delay),
}

impl<C> TryConnectFutureState<C>
where
    C: protocol::Connection,
{
    fn connect(addr: C::Addr, deadline: Instant) -> TryConnectFutureState<C> {
        let future = tokio::time::timeout_at(deadline, C::connect(addr));
        TryConnectFutureState::Connecting(future)
    }

    fn sleep(duration: Duration, deadline: Instant) -> TryConnectFutureState<C> {
        let mut when = Instant::now() + duration;
        if when > deadline {
            when = deadline;
        }
        TryConnectFutureState::Sleeping(tokio::time::delay_until(when))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_try_connect_success() -> Result<(), io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let _ = TryConnectFuture::<TcpStream>::new(addr, None).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_try_connect_fail() -> Result<(), io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);
        // Hope that no one else will be listening on this port now.
        match TryConnectFuture::<TcpStream>::new(addr, Duration::from_millis(800)).await {
            Ok(_) => panic!("try connect future unexpectedly succeeded"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::TimedOut),
        }
        Ok(())
    }
}
