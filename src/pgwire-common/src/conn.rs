// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use mz_ore::netio::AsyncReady;
use mz_server_core::TlsMode;
use tokio::io::{self, AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};
use tokio_openssl::SslStream;
use tokio_postgres::error::SqlState;

use crate::ErrorResponse;

#[derive(Debug)]
pub enum Conn<A> {
    Unencrypted(A),
    Ssl(SslStream<A>),
}

impl<A> Conn<A> {
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
