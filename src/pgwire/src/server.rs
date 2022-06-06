// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use openssl::ssl::{Ssl, SslContext};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt, Interest, ReadBuf, Ready};
use tokio_openssl::SslStream;
use tracing::trace;

use mz_frontegg_auth::FronteggAuthentication;
use mz_ore::netio::AsyncReady;

use crate::codec::{self, FramedConn, ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION};
use crate::message::FrontendStartupMessage;
use crate::protocol;

/// Configures a [`Server`].
#[derive(Debug)]
pub struct Config {
    /// A client for the coordinator with which the server will communicate.
    pub coord_client: mz_coord::Client,
    /// The TLS configuration for the server.
    ///
    /// If not present, then TLS is not enabled, and clients requests to
    /// negotiate TLS will be rejected.
    pub tls: Option<TlsConfig>,
    /// The Frontegg authentication configuration.
    ///
    /// If present, Frontegg authentication is enabled, and users may present
    /// a valid Frontegg API token as a password to authenticate. Otherwise,
    /// password authentication is disabled.
    pub frontegg: Option<FronteggAuthentication>,
}

/// Configures a server's TLS encryption and authentication.
#[derive(Debug)]
pub struct TlsConfig {
    /// The SSL context used to manage incoming TLS negotiations.
    pub context: SslContext,
    /// The TLS mode.
    pub mode: TlsMode,
}

/// Specifies how strictly to enforce TLS encryption and authentication.
#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    /// Clients must negotiate TLS encryption.
    Require,
    /// Clients must negotiate TLS encryption and supply a certificate whose
    /// Common Name (CN) field matches the user name they connect as.
    VerifyUser,
}

/// A server that communicates with clients via the pgwire protocol.
pub struct Server {
    tls: Option<TlsConfig>,
    coord_client: mz_coord::Client,
    frontegg: Option<FronteggAuthentication>,
}

impl Server {
    /// Constructs a new server.
    pub fn new(config: Config) -> Server {
        Server {
            tls: config.tls,
            coord_client: config.coord_client,
            frontegg: config.frontegg,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn handle_connection<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        let mut coord_client = self.coord_client.new_conn()?;
        let conn_id = coord_client.conn_id();
        let mut conn = Conn::Unencrypted(conn);
        loop {
            let message = codec::decode_startup(&mut conn).await?;

            match &message {
                Some(message) => trace!("cid={} recv={:?}", conn_id, message),
                None => trace!("cid={} recv=<eof>", conn_id),
            }

            conn = match message {
                // Clients sometimes hang up during the startup sequence, e.g.
                // because they receive an unacceptable response to an
                // `SslRequest`. This is considered a graceful termination.
                None => return Ok(()),

                Some(FrontendStartupMessage::Startup { version, params }) => {
                    let mut conn = FramedConn::new(conn_id, conn);
                    protocol::run(protocol::RunParams {
                        tls_mode: self.tls.as_ref().map(|tls| tls.mode),
                        coord_client,
                        conn: &mut conn,
                        version,
                        params,
                        frontegg: self.frontegg.as_ref(),
                    })
                    .await?;
                    conn.flush().await?;
                    return Ok(());
                }

                Some(FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                }) => {
                    coord_client.cancel_request(conn_id, secret_key).await;
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    return Ok(());
                }

                Some(FrontendStartupMessage::SslRequest) => match (conn, &self.tls) {
                    (Conn::Unencrypted(mut conn), Some(tls)) => {
                        trace!("cid={} send=AcceptSsl", conn_id);
                        conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                        let mut ssl_stream = SslStream::new(Ssl::new(&tls.context)?, conn)?;
                        if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                            let _ = ssl_stream.get_mut().shutdown().await;
                            return Err(e.into());
                        }
                        Conn::Ssl(ssl_stream)
                    }
                    (mut conn, _) => {
                        trace!("cid={} send=RejectSsl", conn_id);
                        conn.write_all(&[REJECT_ENCRYPTION]).await?;
                        conn
                    }
                },

                Some(FrontendStartupMessage::GssEncRequest) => {
                    trace!("cid={} send=RejectGssEnc", conn_id);
                    conn.write_all(&[REJECT_ENCRYPTION]).await?;
                    conn
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum Conn<A> {
    Unencrypted(A),
    Ssl(SslStream<A>),
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
