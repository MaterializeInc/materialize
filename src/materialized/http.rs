// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{self, BoxFuture, TryFutureExt};
use hyper::service;
use hyper::{Body, Method, Request, Response};
use prometheus::Encoder;
use tokio::io::{AsyncRead, AsyncWrite};

struct FutureResponse(BoxFuture<'static, Result<Response<Body>, failure::Error>>);

impl From<Response<Body>> for FutureResponse {
    fn from(res: Response<Body>) -> FutureResponse {
        FutureResponse(Box::pin(future::ok(res)))
    }
}

impl From<Result<Response<Body>, failure::Error>> for FutureResponse {
    fn from(res: Result<Response<Body>, failure::Error>) -> FutureResponse {
        FutureResponse(Box::pin(future::ready(res)))
    }
}

impl Future for FutureResponse {
    type Output = Result<Response<Body>, failure::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

const METHODS: &[&[u8]] = &[
    b"OPTIONS", b"GET", b"HEAD", b"POST", b"PUT", b"DELETE", b"TRACE", b"CONNECT",
];

pub fn match_handshake(buf: &[u8]) -> bool {
    let buf = if let Some(pos) = buf.iter().position(|&b| b == b' ') {
        &buf[..pos]
    } else {
        &buf[..]
    };
    METHODS.contains(&buf)
}

pub async fn handle_connection<A: 'static + AsyncRead + AsyncWrite + Unpin>(
    a: A,
    gather_metrics: bool,
) -> Result<(), failure::Error> {
    let svc =
        service::service_fn(
            move |req: Request<Body>| match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => handle_home(req),
                (&Method::GET, "/metrics") => handle_prometheus(req, gather_metrics).into(),
                _ => handle_unknown(req),
            },
        );
    let http = hyper::server::conn::Http::new();
    http.serve_connection(a, svc).err_into().await
}

fn handle_home(_: Request<Body>) -> FutureResponse {
    Response::new(Body::from("materialized v0.0.1")).into()
}

fn handle_prometheus(
    _: Request<Body>,
    gather_metrics: bool,
) -> Result<Response<Body>, failure::Error> {
    let metric_families = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();

    encoder.encode(&metric_families, &mut buffer)?;

    let metrics = String::from_utf8(buffer)?;
    if !gather_metrics {
        log::warn!("requested metrics but they are disabled");
        if !metrics.is_empty() {
            log::error!("gathered metrics despite prometheus being disabled!");
        }
        Ok(Response::builder()
            .status(404)
            .body(Body::from("metrics are disabled"))
            .unwrap())
    } else {
        Ok(Response::new(Body::from(metrics)))
    }
}

fn handle_unknown(_: Request<Body>) -> FutureResponse {
    Response::builder()
        .status(403)
        .body(Body::from("bad request"))
        .unwrap()
        .into()
}
