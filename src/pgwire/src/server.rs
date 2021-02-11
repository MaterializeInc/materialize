// Copyright Materialize, Inc. All rights reserved.
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

use anyhow::bail;
use async_trait::async_trait;
use log::trace;
use openssl::ssl::{Ssl, SslContext};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt, Interest, ReadBuf, Ready};
use tokio_openssl::SslStream;

use coord::session::Session;
use ore::netio::AsyncReady;

use crate::codec::{self, FramedConn, ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION};
use crate::id_alloc::{IdAllocator, IdExhaustionError};
use crate::message::FrontendStartupMessage;
use crate::protocol::StateMachine;
use crate::secrets::SecretManager;

/// Configures a [`Server`].
#[derive(Debug)]
pub struct Config {
    /// A client for the coordinator with which the server will communicate.
    pub coord_client: coord::Client,
    /// The TLS configuration for the server.
    ///
    /// If not present, then TLS is not enabled, and clients requests to
    /// negotiate TLS will be rejected.
    pub tls: Option<TlsConfig>,
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
    id_alloc: IdAllocator,
    secrets: SecretManager,
    tls: Option<TlsConfig>,
    coord_client: coord::Client,
}

impl Server {
    /// Constructs a new server.
    pub fn new(config: Config) -> Server {
        Server {
            id_alloc: IdAllocator::new(1, 1 << 16),
            secrets: SecretManager::new(),
            tls: config.tls,
            coord_client: config.coord_client,
        }
    }

    /// Handles an incoming pgwire connection.
    pub async fn handle_connection<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        // Allocate state for this connection.
        let conn_id = match self.id_alloc.alloc() {
            Ok(id) => id,
            Err(IdExhaustionError) => {
                bail!("maximum number of connections reached");
            }
        };
        self.secrets.generate(conn_id);

        let res = self.handle_connection_inner(conn_id, conn).await;

        // Clean up state tied to this specific connection.
        self.id_alloc.free(conn_id);
        self.secrets.free(conn_id);

        res
    }

    async fn handle_connection_inner<A>(&self, conn_id: u32, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
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

                Some(FrontendStartupMessage::Startup {
                    version,
                    mut params,
                }) => {
                    let user = params.remove("user").unwrap_or_else(String::new);
                    let coord_client = self.coord_client.for_session(Session::new(conn_id, user));
                    let machine = StateMachine {
                        conn: FramedConn::new(conn_id, conn),
                        conn_id,
                        secret_key: self.secrets.get(conn_id).unwrap(),
                        tls_mode: self.tls.as_ref().map(|tls| tls.mode),
                        coord_client,
                    };
                    machine.run(version, params).await?;
                    return Ok(());
                }

                Some(FrontendStartupMessage::CancelRequest {
                    conn_id,
                    secret_key,
                }) => {
                    if self.secrets.verify(conn_id, secret_key) {
                        self.coord_client.clone().cancel_request(conn_id).await;
                    }
                    // For security, the client is not told whether the cancel
                    // request succeeds or fails.
                    return Ok(());
                }

                Some(FrontendStartupMessage::SslRequest) => match (conn, &self.tls) {
                    (Conn::Unencrypted(mut conn), Some(tls)) => {
                        trace!("cid={} send=AcceptSsl", conn_id);
                        conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                        let mut ssl_stream = SslStream::new(Ssl::new(&tls.context)?, conn)?;
                        Pin::new(&mut ssl_stream).accept().await?;
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
