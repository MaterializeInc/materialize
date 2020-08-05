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

use std::future::Future;
use std::time::Instant;

use futures::channel::mpsc::UnboundedSender;
use futures::future::{FutureExt, TryFutureExt};
use hyper::{service, Body, Method, Request, Response};
use openssl::ssl::SslAcceptor;
use tokio::io::{AsyncRead, AsyncWrite};

use ore::netio::SniffedStream;

mod catalog;
mod metrics;
#[cfg(not(target_os = "macos"))]
mod prof;
mod root;
mod util;

const METHODS: &[&[u8]] = &[
    b"OPTIONS", b"GET", b"HEAD", b"POST", b"PUT", b"DELETE", b"TRACE", b"CONNECT",
];

const TLS_HANDSHAKE_START: u8 = 22;

fn sniff_tls(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == TLS_HANDSHAKE_START
}

pub struct Server {
    tls: Option<SslAcceptor>,
    cmdq_tx: UnboundedSender<coord::Command>,
    /// When this server started
    start_time: Instant,
}

impl Server {
    pub fn new(
        tls: Option<SslAcceptor>,
        cmdq_tx: UnboundedSender<coord::Command>,
        start_time: Instant,
        worker_count: &str,
    ) -> Server {
        // just set this so it shows up in metrics
        metrics::WORKER_COUNT
            .with_label_values(&[worker_count])
            .set(1);
        Server {
            tls,
            cmdq_tx,
            start_time,
        }
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
        match (&self.tls, sniff_tls(&conn.sniff_buffer())) {
            (Some(tls), true) => {
                let conn = tokio_openssl::accept(tls, conn).await?;
                self.handle_connection_inner(conn).await
            }
            _ => self.handle_connection_inner(conn).await,
        }
    }

    async fn handle_connection_inner<A>(&self, conn: A) -> Result<(), anyhow::Error>
    where
        A: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let svc = service::service_fn(move |req| match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => self.handle_home(req).boxed(),
            (&Method::GET, "/metrics") => self.handle_prometheus(req).boxed(),
            (&Method::GET, "/status") => self.handle_status(req).boxed(),
            (&Method::GET, "/prof") => self.handle_prof(req).boxed(),
            (&Method::POST, "/prof") => self.handle_prof(req).boxed(),
            (&Method::GET, "/internal/catalog") => self.handle_internal_catalog(req).boxed(),
            _ => self.handle_static(req).boxed(),
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

    #[cfg(target_os = "macos")]
    fn handle_prof(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        future::ok(Response::builder().status(501).body(Body::from(
            "Profiling is not supported on macOS (HINT: run on Linux with _RJEM_MALLOC_CONF=prof:true)",
        ))?)
    }

    #[cfg(not(target_os = "macos"))]
    fn handle_prof(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        prof::handle(req)
    }
}
