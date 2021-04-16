// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Embedded HTTP server.
//!
//! materialized embeds an HTTP server for introspection into the running
//! process. At the moment, its primary exports are Prometheus metrics, heap
//! profiles, and catalog dumps.

use std::pin::Pin;
use std::time::Instant;

use futures::future::TryFutureExt;
use hyper::{service, Method, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use openssl::nid::Nid;
use openssl::ssl::{Ssl, SslContext};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_openssl::SslStream;

use coord::session::Session;
use ore::future::OreFutureExt;
use ore::netio::SniffedStream;

mod catalog;
mod memory;
mod metrics;
mod prof;
mod root;
mod sql;
mod util;

const SYSTEM_USER: &str = "mz_system";

const METHODS: &[&[u8]] = &[
    b"OPTIONS", b"GET", b"HEAD", b"POST", b"PUT", b"DELETE", b"TRACE", b"CONNECT",
];

const TLS_HANDSHAKE_START: u8 = 22;

fn sniff_tls(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == TLS_HANDSHAKE_START
}

#[derive(Debug, Clone)]
pub struct Config {
    pub tls: Option<TlsConfig>,
    pub coord_client: coord::Client,
    pub start_time: Instant,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub context: SslContext,
    pub mode: TlsMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    Require,
    AssumeUser,
}

#[derive(Debug)]
pub struct Server {
    tls: Option<TlsConfig>,
    coord_client: coord::Client,
    start_time: Instant,
}

impl Server {
    pub fn new(config: Config) -> Server {
        Server {
            tls: config.tls,
            coord_client: config.coord_client,
            start_time: config.start_time,
        }
    }

    fn tls_mode(&self) -> Option<TlsMode> {
        self.tls.as_ref().map(|tls| tls.mode)
    }

    fn tls_context(&self) -> Option<&SslContext> {
        self.tls.as_ref().map(|tls| &tls.context)
    }

    pub fn match_handshake(&self, buf: &[u8]) -> bool {
        if self.tls.is_some() && sniff_tls(buf) {
            return true;
        }
        let buf = if let Some(pos) = buf.iter().position(|&b| b == b' ') {
            &buf[..pos]
        } else {
            &buf[..]
        };
        METHODS.contains(&buf)
    }

    pub async fn handle_connection<A>(&self, conn: SniffedStream<A>) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let conn = match (&self.tls_context(), sniff_tls(&conn.sniff_buffer())) {
            (Some(tls_context), true) => {
                let mut ssl_stream = SslStream::new(Ssl::new(tls_context)?, conn)?;
                if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                    let _ = ssl_stream.get_mut().shutdown().await;
                    return Err(e.into());
                }
                MaybeHttpsStream::Https(ssl_stream)
            }
            _ => MaybeHttpsStream::Http(conn),
        };

        // Validate that the connection is compatible with the TLS mode.
        //
        // The match here explicitly spells out all cases to be resilient to
        // future changes to TlsMode.
        let user = match (self.tls_mode(), &conn) {
            (None, MaybeHttpsStream::Http(_)) => Ok(SYSTEM_USER.into()),
            (None, MaybeHttpsStream::Https(_)) => unreachable!(),
            (Some(TlsMode::Require), MaybeHttpsStream::Http(_)) => Err("HTTPS is required"),
            (Some(TlsMode::Require), MaybeHttpsStream::Https(_)) => Ok(SYSTEM_USER.into()),
            (Some(TlsMode::AssumeUser), MaybeHttpsStream::Http(_)) => Err("HTTPS is required"),
            (Some(TlsMode::AssumeUser), MaybeHttpsStream::Https(conn)) => conn
                .ssl()
                .peer_certificate()
                .as_ref()
                .and_then(|cert| cert.subject_name().entries_by_nid(Nid::COMMONNAME).next())
                .and_then(|cn| cn.data().as_utf8().ok())
                .map(|cn| cn.to_string())
                .ok_or("invalid user name in client certificate"),
        };

        let svc = service::service_fn(move |req| {
            let user = user.clone();
            let coord_client = self.coord_client.clone();
            let start_time = self.start_time;
            let future = async move {
                let user = match user {
                    Ok(user) => user,
                    Err(e) => return Ok(util::error_response(StatusCode::UNAUTHORIZED, e)),
                };

                let coord_client = coord_client.new_conn()?;
                let session = Session::new(coord_client.conn_id(), user);
                let (mut coord_client, _) = match coord_client.startup(session).await {
                    Ok(coord_client) => coord_client,
                    Err(e) => {
                        return Ok(util::error_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            e.to_string(),
                        ))
                    }
                };

                let res = match (req.method(), req.uri().path()) {
                    (&Method::GET, "/") => root::handle_home(req, &mut coord_client).await,
                    (&Method::GET, "/metrics") => {
                        metrics::handle_prometheus(req, &mut coord_client, start_time).await
                    }
                    (&Method::GET, "/status") => {
                        metrics::handle_status(req, &mut coord_client, start_time).await
                    }
                    (&Method::GET, "/prof") => prof::handle_prof(req, &mut coord_client).await,
                    (&Method::GET, "/memory") => {
                        memory::handle_memory(req, &mut coord_client).await
                    }
                    (&Method::POST, "/prof") => prof::handle_prof(req, &mut coord_client).await,
                    (&Method::POST, "/sql") => sql::handle_sql(req, &mut coord_client).await,
                    (&Method::GET, "/internal/catalog") => {
                        catalog::handle_internal_catalog(req, &mut coord_client).await
                    }
                    _ => root::handle_static(req, &mut coord_client).await,
                };
                coord_client.terminate().await;
                res
            };
            // Hyper will drop the future if the client goes away, in an effort
            // to eagerly cancel work. But the design of the coordinator
            // requires that the future be polled to completion in order for
            // cleanup to occur. Specifically:
            //
            //   * The `SessionClient` *must* call `terminate` before it is
            //     dropped.
            //   * A peek receiver must not be dropped before it receives a
            //     message.
            //
            // Not observing this rule leads to invariant violations that panic;
            // see #6278 for an example.
            //
            // The fix here is to wrap the future in a combinator that will call
            // `tokio::spawn` to poll it to completion if Hyper gives up on it.
            // A bit weird, but it works, and hides this messiness from the code
            // in the future itself. If Rust ever supports asynchronous
            // destructors ("AsyncDrop"), those will admit a more natural
            // solution to the problem.
            future.spawn_if_canceled()
        });
        let http = hyper::server::conn::Http::new();
        http.serve_connection(conn, svc).err_into().await
    }

    // Handler functions are attached by various submodules. They all have a
    // signature of the following form:
    //
    //     fn handle_foo(req) -> impl Future<Output = anyhow::Result<Result<Body>>>
    //
    // If you add a new handler, please add it to the most appropriate
    // submodule, or create a new submodule if necessary. Don't add it here!
}
