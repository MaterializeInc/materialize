// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Public concrete implementation of [std::future::Future].

use futures_util::FutureExt;
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

    fn flatten_err(res: Result<Result<T, Error>, oneshot::error::RecvError>) -> Result<T, Error> {
        match res {
            Ok(x) => x,
            // The sender will only hang up if the runtime is no longer running.
            Err(oneshot::error::RecvError { .. }) => Err(Error::RuntimeShutdown),
        }
    }

    /// Blocks and synchronously receives the result.
    //
    // TODO: Make this cfg(test) or behind a feature gate.
    pub fn recv(self) -> Result<T, Error> {
        futures_executor::block_on(self.into_future())
    }

    /// Convert this into a [std::future::Future].
    ///
    /// The computation represented by self will complete regardless of whether
    /// the returned Future is polled to completion, but anything derived from
    /// the returned [std::future::Future] (map, etc) is subject to the normal
    /// rules.
    //
    // TODO: Is it possible for this to impl [std::future::Future] directly?
    pub fn into_future(self) -> impl std::future::Future<Output = Result<T, Error>> {
        self.0.map(Self::flatten_err)
    }
}

/// A handle for filling the result of an asynchronous computation.
pub struct FutureHandle<T>(oneshot::Sender<Result<T, Error>>);

impl<T> FutureHandle<T> {
    pub(crate) fn fill(self, res: Result<T, Error>) {
        // Don't care if the receiver hung up.
        let _ = self.0.send(res);
    }
}
