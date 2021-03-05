// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

use async_trait::async_trait;
use tokio::io::{self, Interest, Ready};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

/// Asynchronous IO readiness.
///
/// Like [`tokio::io::AsyncRead`] or [`tokio::io::AsyncWrite`], but for
/// readiness events.
#[async_trait]
pub trait AsyncReady {
    /// Checks for IO readiness.
    ///
    /// See [`TcpStream::ready`] for details.
    async fn ready(&self, interest: Interest) -> io::Result<Ready>;
}

#[async_trait]
impl AsyncReady for TcpStream {
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.ready(interest).await
    }
}

#[async_trait]
impl<S> AsyncReady for SslStream<S>
where
    S: AsyncReady + Sync,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.get_ref().ready(interest).await
    }
}
