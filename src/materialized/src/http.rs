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

use futures::future::{FutureExt, TryFutureExt};
use hyper::{service, Method, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use openssl::nid::Nid;
use openssl::ssl::{Ssl, SslContext};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_openssl::SslStream;

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
    pub worker_count: usize,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub context: SslContext,
    pub mode: TlsMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    Allow,
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
        // Just set this metric once so that it shows up in the metric export.
        metrics::WORKER_COUNT
            .with_label_values(&[&config.worker_count.to_string()])
            .set(1);
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
                Pin::new(&mut ssl_stream).accept().await?;
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
            (Some(TlsMode::Allow), MaybeHttpsStream::Http(_)) => Ok(SYSTEM_USER.into()),
            (Some(TlsMode::Allow), MaybeHttpsStream::Https(_)) => Ok(SYSTEM_USER.into()),
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
            let user = match user {
                Ok(ref user) => user,
                Err(e) => {
                    return async move { Ok(util::error_response(StatusCode::UNAUTHORIZED, e)) }
                        .boxed()
                }
            };

            // TODO(benesch): we ought to verify that `user` actually exists and
            // use a coordinator client that is associated with that user, to
            // ensure that any coordinator operations performed by the HTTP
            // layer are authenticated as that user. But right now all users are
            // superusers so it doesn't actually matter.

            match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => self.handle_home(req).boxed(),
                (&Method::GET, "/metrics") => self.handle_prometheus(req).boxed(),
                (&Method::GET, "/status") => self.handle_status(req).boxed(),
                (&Method::GET, "/prof") => self.handle_prof(req).boxed(),
                (&Method::GET, "/memory") => self.handle_memory(req).boxed(),
                (&Method::POST, "/prof") => self.handle_prof(req).boxed(),
                (&Method::POST, "/sql") => self.handle_sql(req, user).boxed(),
                (&Method::GET, "/internal/catalog") => self.handle_internal_catalog(req).boxed(),
                _ => self.handle_static(req).boxed(),
            }
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
