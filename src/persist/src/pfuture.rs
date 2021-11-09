// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Public concrete implementation of [std::future::Future].

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::ready;
use tokio::sync::oneshot;

use crate::error::Error;

/// The result of an asynchronous computation.
///
/// Unlike [std::future::Future], the computation will complete even if this is
/// dropped.
pub struct PFuture<T>(oneshot::Receiver<Result<T, Error>>);

impl<T> fmt::Debug for PFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PFuture").finish_non_exhaustive()
    }
}

impl<T> PFuture<T> {
    /// Create a new instance of [PFuture], and the corresponding
    /// [PFutureHandle] to fill it.
    pub fn new() -> (PFutureHandle<T>, PFuture<T>) {
        let (tx, rx) = oneshot::channel();
        (PFutureHandle(tx), PFuture(rx))
    }

    /// Blocks and synchronously receives the result.
    //
    // TODO: Make this cfg(test) or behind a feature gate.
    pub fn recv(self) -> Result<T, Error> {
        futures_executor::block_on(self)
    }
}

impl<T> std::future::Future for PFuture<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = match ready!(Pin::new(&mut self.0).poll(cx)) {
            Ok(x) => x,
            // The sender will only hang up if the runtime is no longer running.
            Err(oneshot::error::RecvError { .. }) => Err(Error::RuntimeShutdown),
        };
        Poll::Ready(res)
    }
}

/// A handle for filling the result of an asynchronous computation.
pub struct PFutureHandle<T>(oneshot::Sender<Result<T, Error>>);

impl<T> PFutureHandle<T> {
    pub(crate) fn fill(self, res: Result<T, Error>) {
        // Don't care if the receiver hung up.
        let _ = self.0.send(res);
    }
}

impl<T> fmt::Debug for PFutureHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PFutureHandle").finish_non_exhaustive()
    }
}
