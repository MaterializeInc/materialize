// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Communication utilities.

use futures::{Async, Future, Poll};
use std::cmp;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io;
use tokio::timer::{Delay, Timeout};

use crate::protocol;

/// A future that resolves once a connection to [`SocketAddr`] can be
/// established or a timeout elapses.
///
/// Under the hood, the future repeatedly opens new TCP connections, with
/// exponential backoff between each attempt, until a TCP connection succeeds.
pub struct TryConnectFuture<C> {
    addr: SocketAddr,
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
    pub fn new(addr: SocketAddr, timeout: impl Into<Option<Duration>>) -> TryConnectFuture<C> {
        let deadline = match timeout.into() {
            None => Instant::now() + Duration::from_secs(60 * 60 * 24 * 365 * 100), // approximately forever
            Some(timeout) => Instant::now() + timeout,
        };
        TryConnectFuture {
            addr,
            deadline,
            backoff: Duration::from_millis(100),
            state: TryConnectFutureState::connect(&addr, deadline),
        }
    }
}

impl<C> Future for TryConnectFuture<C>
where
    C: protocol::Connection,
{
    type Item = C;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match &mut self.state {
                TryConnectFutureState::Connecting(future) => match future.poll() {
                    Ok(Async::Ready(conn)) => return Ok(Async::Ready(conn)),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => {
                        if err.is_timer() {
                            panic!("tokio timer returned an error: {}", err);
                        } else if err.is_elapsed() {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "connection attempt timed out",
                            ));
                        }
                        if self.backoff < Duration::from_secs(1) {
                            self.backoff *= 2;
                        }
                        self.state = TryConnectFutureState::sleep(self.backoff, self.deadline);
                    }
                },
                TryConnectFutureState::Sleeping(future) => match future.poll() {
                    Ok(Async::Ready(_)) => {
                        self.state = TryConnectFutureState::connect(&self.addr, self.deadline)
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(err) => panic!("tokio timer returned an error: {}", err),
                },
            }
        }
    }
}

enum TryConnectFutureState<C> {
    Connecting(Timeout<Box<dyn Future<Item = C, Error = io::Error> + Send>>),
    Sleeping(Delay),
}

impl<C> TryConnectFutureState<C>
where
    C: protocol::Connection,
{
    fn connect(addr: &SocketAddr, deadline: Instant) -> TryConnectFutureState<C> {
        let future = Timeout::new_at(C::connect(addr), deadline);
        TryConnectFutureState::Connecting(future)
    }

    fn sleep(duration: Duration, deadline: Instant) -> TryConnectFutureState<C> {
        let when = cmp::min(Instant::now() + duration, deadline);
        TryConnectFutureState::Sleeping(Delay::new(when))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::net::{IpAddr, Ipv4Addr, TcpListener};
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::runtime::Runtime;

    #[test]
    fn test_try_connect_success() -> Result<(), io::Error> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = TcpListener::bind(&addr)?;
        let addr = listener.local_addr()?;
        let mut runtime = Runtime::new()?;
        runtime.block_on(TryConnectFuture::<TcpStream>::new(addr, None))?;
        Ok(())
    }

    #[test]
    fn test_try_connect_fail() -> Result<(), io::Error> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = TcpListener::bind(&addr)?;
        let addr = listener.local_addr()?;
        drop(listener);
        // Hope that no one else will be listening on this port now.
        let mut runtime = Runtime::new()?;
        match runtime.block_on(TryConnectFuture::<TcpStream>::new(
            addr,
            Duration::from_millis(800),
        )) {
            Ok(_) => panic!("try connect future unexpectedly succeeded"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::TimedOut),
        }
        Ok(())
    }
}
