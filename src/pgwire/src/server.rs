// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::str::FromStr;

use anyhow::Context;
use async_trait::async_trait;
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_pgwire_common::{
    decode_startup, Conn, ConnectionCounter, FrontendStartupMessage, ACCEPT_SSL_ENCRYPTION,
    CONN_UUID_KEY, MZ_FORWARDED_FOR_KEY, REJECT_ENCRYPTION,
};
use mz_server_core::{Connection, ConnectionHandler, ReloadingTlsConfig};
use openssl::ssl::Ssl;
use tokio::io::AsyncWriteExt;
use tokio_openssl::SslStream;
use tracing::{debug, error, trace};
use uuid::Uuid;

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
    pub tls: Option<ReloadingTlsConfig>,
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
    pub active_connection_counter: ConnectionCounter,
    /// Helm chart version
    pub helm_chart_version: Option<String>,
}

/// A server that communicates with clients via the pgwire protocol.
pub struct Server {
    tls: Option<ReloadingTlsConfig>,
    adapter_client: mz_adapter::Client,
    frontegg: Option<FronteggAuthentication>,
    metrics: Metrics,
    internal: bool,
    active_connection_counter: ConnectionCounter,
    helm_chart_version: Option<String>,
}

#[async_trait]
impl mz_server_core::Server for Server {
    const NAME: &'static str = "pgwire";

    fn handle_connection(&self, conn: Connection) -> ConnectionHandler {
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
            active_connection_counter: config.active_connection_counter,
            helm_chart_version: config.helm_chart_version,
        }
    }

    #[mz_ore::instrument(level = "debug")]
    pub fn handle_connection(
        &self,
        conn: Connection,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + 'static + Send {
        let adapter_client = self.adapter_client.clone();
        let frontegg = self.frontegg.clone();
        let tls = self.tls.clone();
        let internal = self.internal;
        let metrics = self.metrics.clone();
        let active_connection_counter = self.active_connection_counter.clone();
        let helm_chart_version = self.helm_chart_version.clone();

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

                            Some(FrontendStartupMessage::Startup {
                                version,
                                mut params,
                            }) => {
                                // If someone (usually the balancer) forwarded a connection UUID,
                                // then use that, otherwise generate one.
                                let conn_uuid_handle = conn.inner_mut().uuid_handle();
                                let conn_uuid = params
                                    .remove(CONN_UUID_KEY)
                                    .and_then(|uuid| uuid.parse().inspect_err(|e| error!("pgwire connection with invalid conn UUID: {e}")).ok());
                                let conn_uuid_forwarded = conn_uuid.is_some();
                                let conn_uuid = conn_uuid.unwrap_or_else(Uuid::new_v4);
                                conn_uuid_handle.set(conn_uuid);
                                debug!(conn_uuid = %conn_uuid_handle.display(), conn_uuid_forwarded, "starting new pgwire connection in adapter");

                                let direct_peer_addr = conn
                                    .inner_mut()
                                    .peer_addr()
                                    .context("fetching peer addr")?
                                    .ip();
                                let peer_addr= match params.remove(MZ_FORWARDED_FOR_KEY) {
                                    Some(ip_str) => {
                                        match IpAddr::from_str(&ip_str) {
                                            Ok(ip) => Some(ip),
                                            Err(e) => {
                                                error!("pgwire connection with invalid mz_forwarded_for address: {e}");
                                                None
                                            }
                                        }
                                    }
                                    None => Some(direct_peer_addr)
                                };
                                let mut conn = FramedConn::new(
                                    conn_id.clone(),
                                    peer_addr,
                                    conn,
                                );

                                protocol::run(protocol::RunParams {
                                    tls_mode: tls.as_ref().map(|tls| tls.mode),
                                    adapter_client,
                                    conn: &mut conn,
                                    conn_uuid,
                                    version,
                                    params,
                                    frontegg: frontegg.as_ref(),
                                    internal,
                                    active_connection_counter,
                                    helm_chart_version,
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
                                        SslStream::new(Ssl::new(&tls.context.get())?, conn)?;
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
