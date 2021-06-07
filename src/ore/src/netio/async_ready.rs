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
