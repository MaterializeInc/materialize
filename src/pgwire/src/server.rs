// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_ore::netio::AsyncReady;
use mz_pgwire_common::{
    decode_startup, Conn, FrontendStartupMessage, ACCEPT_SSL_ENCRYPTION, REJECT_ENCRYPTION,
};
use mz_server_core::{ConnectionHandler, TlsConfig};
use mz_sql::session::vars::ConnectionCounter;
use openssl::ssl::Ssl;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use tracing::trace;

use crate::codec::FramedConn;
use crate::metrics::{Metrics, MetricsConfig};
use crate::protocol;

/// Configures a [`Server`].
#[derive(Debug)]
pub struct Config {
    /// The label for the mz_connection_status metric.
    pub label: &'static str,
    /// A client for the adapter with which the server will communicate.
    pub adapter_client: mz_adapter::Client,
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
    /// The registry entries that the pgwire server uses to report metrics.
    pub metrics: MetricsConfig,
    /// Whether this is an internal server that permits access to restricted
    /// system resources.
    pub internal: bool,
    /// Global connection limit and count
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

/// A server that communicates with clients via the pgwire protocol.
pub struct Server {
    tls: Option<TlsConfig>,
    adapter_client: mz_adapter::Client,
    frontegg: Option<FronteggAuthentication>,
    metrics: Metrics,
    internal: bool,
    active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

#[async_trait]
impl mz_server_core::Server for Server {
    const NAME: &'static str = "pgwire";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        // Using fully-qualified syntax means we won't accidentally call
        // ourselves (i.e., silently infinitely recurse) if the name or type of
        // `crate::Server::handle_connection` changes.
        Box::pin(crate::Server::handle_connection(self, conn))
    }
}

impl Server {
    /// Constructs a new server.
    pub fn new(config: Config) -> Server {
        Server {
            tls: config.tls,
            adapter_client: config.adapter_client,
            frontegg: config.frontegg,
            metrics: Metrics::new(config.metrics, config.label),
            internal: config.internal,
            active_connection_count: config.active_connection_count,
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub fn handle_connection<A>(
        &self,
        conn: A,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + 'static + Send
    where
        A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + fmt::Debug + 'static,
    {
        let mut adapter_client = self.adapter_client.clone();
        let frontegg = self.frontegg.clone();
        let tls = self.tls.clone();
        let internal = self.internal;
        let metrics = self.metrics.clone();
        let active_connection_count = Arc::clone(&self.active_connection_count);
        // TODO(guswynn): remove this redundant_closure_call
        #[allow(clippy::redundant_closure_call)]
        async move {
            let result = (|| {
                async move {
                    let conn_id = adapter_client.new_conn_id()?;
                    let mut conn = Conn::Unencrypted(conn);
                    loop {
                        let message = decode_startup(&mut conn).await?;

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
                                let mut conn = FramedConn::new(conn_id.clone(), conn);
                                protocol::run(protocol::RunParams {
                                    tls_mode: tls.as_ref().map(|tls| tls.mode),
                                    adapter_client,
                                    conn: &mut conn,
                                    version,
                                    params,
                                    frontegg: frontegg.as_ref(),
                                    internal,
                                    active_connection_count,
                                })
                                .await?;
                                conn.flush().await?;
                                return Ok(());
                            }

                            Some(FrontendStartupMessage::CancelRequest {
                                conn_id,
                                secret_key,
                            }) => {
                                adapter_client.cancel_request(conn_id, secret_key);
                                // For security, the client is not told whether the cancel
                                // request succeeds or fails.
                                return Ok(());
                            }

                            Some(FrontendStartupMessage::SslRequest) => match (conn, &tls) {
                                (Conn::Unencrypted(mut conn), Some(tls)) => {
                                    trace!("cid={} send=AcceptSsl", conn_id);
                                    conn.write_all(&[ACCEPT_SSL_ENCRYPTION]).await?;
                                    let mut ssl_stream =
                                        SslStream::new(Ssl::new(&tls.context)?, conn)?;
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
            })()
            .await;
            metrics.connection_status(result.is_ok()).inc();
            result
        }
    }
}
