// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use derivative::Derivative;
use mz_ore::netio::AsyncReady;
use mz_server_core::TlsMode;
use tokio::io::{self, AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};
use tokio_openssl::SslStream;
use tokio_postgres::error::SqlState;

use crate::ErrorResponse;

pub const CONN_UUID_KEY: &str = "mz_connection_uuid";
pub const MZ_FORWARDED_FOR_KEY: &str = "mz_forwarded_for";

#[derive(Debug)]
pub enum Conn<A> {
    Unencrypted(A),
    Ssl(SslStream<A>),
}

impl<A> Conn<A> {
    pub fn inner_mut(&mut self) -> &mut A {
        match self {
            Conn::Unencrypted(inner) => inner,
            Conn::Ssl(inner) => inner.get_mut(),
        }
    }

    /// Returns an error if tls_mode is incompatible with this connection's stream type.
    pub fn ensure_tls_compatibility(
        &self,
        tls_mode: &Option<TlsMode>,
    ) -> Result<(), ErrorResponse> {
        // Validate that the connection is compatible with the TLS mode.
        //
        // The match here explicitly spells out all cases to be resilient to
        // future changes to TlsMode.
        match (tls_mode, self) {
            (None, Conn::Unencrypted(_)) => (),
            (None, Conn::Ssl(_)) => unreachable!(),
            (Some(TlsMode::Allow), Conn::Unencrypted(_)) => (),
            (Some(TlsMode::Allow), Conn::Ssl(_)) => (),
            (Some(TlsMode::Require), Conn::Ssl(_)) => (),
            (Some(TlsMode::Require), Conn::Unencrypted(_)) => {
                return Err(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "TLS encryption is required",
                ));
            }
        }

        Ok(())
    }
}

impl<A> AsyncRead for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_read(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<A> AsyncWrite for Conn<A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_write(cx, buf),
            Conn::Ssl(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_flush(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Unencrypted(inner) => Pin::new(inner).poll_shutdown(cx),
            Conn::Ssl(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}

#[async_trait]
impl<A> AsyncReady for Conn<A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Sync + Unpin,
{
    async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        match self {
            Conn::Unencrypted(inner) => inner.ready(interest).await,
            Conn::Ssl(inner) => inner.ready(interest).await,
        }
    }
}

/// Metadata about a user that is required to allocate a [`ConnectionHandle`].
#[derive(Debug, Clone, Copy)]
pub struct UserMetadata {
    pub is_admin: bool,
    pub should_limit_connections: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionCounter {
    inner: Arc<Mutex<ConnectionCounterInner>>,
}

impl ConnectionCounter {
    /// Returns a [`ConnectionHandle`] which must be kept alive for the entire duration of the
    /// external connection.
    ///
    /// Dropping the [`ConnectionHandle`] decrements the connection count.
    pub fn allocate_connection(
        &self,
        metadata: impl Into<UserMetadata>,
    ) -> Result<Option<ConnectionHandle>, ConnectionError> {
        let mut inner = self.inner.lock().expect("environmentd panicked");
        let metadata = metadata.into();

        if !metadata.should_limit_connections {
            return Ok(None);
        }

        if (metadata.is_admin && inner.reserved_remaining() > 0)
            || inner.non_reserved_remaining() > 0
        {
            inner.inc_connection_count();
            Ok(Some(self.create_handle()))
        } else {
            Err(ConnectionError::TooManyConnections {
                current: inner.current,
                limit: inner.limit,
            })
        }
    }

    /// Updates the maximum number of connections we allow.
    pub fn update_limit(&self, new_limit: u64) {
        let mut inner = self.inner.lock().expect("environmentd panicked");
        inner.limit = new_limit;
    }

    /// Updates the number of connections we reserve for superusers.
    pub fn update_superuser_reserved(&self, new_reserve: u64) {
        let mut inner = self.inner.lock().expect("environmentd panicked");
        inner.superuser_reserved = new_reserve;
    }

    fn create_handle(&self) -> ConnectionHandle {
        let inner = Arc::clone(&self.inner);
        let decrement_fn = Box::new(move || {
            let mut inner = inner.lock().expect("environmentd panicked");
            inner.dec_connection_count();
        });

        ConnectionHandle {
            decrement_fn: Some(decrement_fn),
        }
    }
}

impl Default for ConnectionCounter {
    fn default() -> Self {
        let inner = ConnectionCounterInner::new(10, 3);
        ConnectionCounter {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionCounterInner {
    /// Current number of connections.
    current: u64,
    /// Total number of connections allowed.
    limit: u64,
    /// Number of connections in `limit` we'll reserve for superusers.
    superuser_reserved: u64,
}

impl ConnectionCounterInner {
    fn new(limit: u64, superuser_reserved: u64) -> Self {
        assert!(superuser_reserved < limit);
        ConnectionCounterInner {
            current: 0,
            limit,
            superuser_reserved,
        }
    }

    fn inc_connection_count(&mut self) {
        self.current += 1;
    }

    fn dec_connection_count(&mut self) {
        self.current -= 1;
    }

    /// The number of connections still available to superusers.
    fn reserved_remaining(&self) -> u64 {
        // Use a saturating sub in case the limit is reduced below the number
        // of current connections.
        self.limit.saturating_sub(self.current)
    }

    /// The number of connections available to non-superusers.
    fn non_reserved_remaining(&self) -> u64 {
        // This ensures that at least a few connections remain for superusers.
        let limit = self.limit.saturating_sub(self.superuser_reserved);
        // Use a saturating sub in case the limit is reduced below the number
        // of current connections.
        limit.saturating_sub(self.current)
    }
}

/// Handle to an open connection, allows us to maintain a count of all connections.
///
/// When Drop-ed decrements the count of open connections.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ConnectionHandle {
    #[derivative(Debug = "ignore")]
    decrement_fn: Option<Box<dyn FnOnce() -> () + Send + Sync>>,
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        match self.decrement_fn.take() {
            Some(decrement_fn) => (decrement_fn)(),
            None => tracing::error!("ConnectionHandle dropped twice!?"),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    /// There were too many connections
    TooManyConnections { current: u64, limit: u64 },
}
