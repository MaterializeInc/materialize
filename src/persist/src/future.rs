// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Public concrete implementation of [std::future::Future].

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::ready;
use tokio::sync::oneshot;

use crate::error::Error;

/// The result of an asynchronous computation.
///
/// Unlike [std::future::Future], the computation will complete even if this is
/// dropped.
#[derive(Debug)]
pub struct Future<T>(oneshot::Receiver<Result<T, Error>>);

impl<T> Future<T> {
    /// Create a new instance of [Future], and the corresponding [FutureHandle]
    /// to fill it.
    pub fn new() -> (FutureHandle<T>, Future<T>) {
        let (tx, rx) = oneshot::channel();
        (FutureHandle(tx), Future(rx))
    }

    /// Blocks and synchronously receives the result.
    //
    // TODO: Make this cfg(test) or behind a feature gate.
    pub fn recv(self) -> Result<T, Error> {
        futures_executor::block_on(self)
    }
}

impl<T> std::future::Future for Future<T> {
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
#[derive(Debug)]
pub struct FutureHandle<T>(oneshot::Sender<Result<T, Error>>);

impl<T> FutureHandle<T> {
    pub(crate) fn fill(self, res: Result<T, Error>) {
        // Don't care if the receiver hung up.
        let _ = self.0.send(res);
    }
}
