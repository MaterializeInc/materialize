// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::format_err;
use futures::{future, Future, Poll, Stream};
use hyper::service;
use hyper::{Body, Method, Request, Response};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::glue::*;
use ore::future::FutureExt;

struct FutureResponse(Box<Future<Item = Response<Body>, Error = failure::Error> + Send>);

impl From<Response<Body>> for FutureResponse {
    fn from(res: Response<Body>) -> FutureResponse {
        FutureResponse(Box::new(future::ok(res)))
    }
}

impl From<Result<Response<Body>, failure::Error>> for FutureResponse {
    fn from(res: Result<Response<Body>, failure::Error>) -> FutureResponse {
        FutureResponse(Box::new(future::result(res)))
    }
}

impl Future for FutureResponse {
    type Item = Response<Body>;
    type Error = failure::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
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

pub fn handle_connection<A: 'static + AsyncRead + AsyncWrite>(
    a: A,
    peek_results_mux: PeekResultsMux,
) -> impl Future<Item = (), Error = failure::Error> {
    let svc =
        service::service_fn(
            move |req: Request<Body>| match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => handle_home(req).left(),
                (&Method::POST, "/api/peek-results") => {
                    handle_peek_result(req, peek_results_mux.clone()).right()
                }
                _ => handle_unknown(req).left(),
            },
        );
    let http = hyper::server::conn::Http::new();
    http.serve_connection(a, svc).from_err()
}

fn handle_home(_: Request<Body>) -> FutureResponse {
    Response::new(Body::from("materialized v0.0.1")).into()
}

fn handle_unknown(_: Request<Body>) -> FutureResponse {
    Response::builder()
        .status(403)
        .body(Body::from("bad request"))
        .unwrap()
        .into()
}

fn handle_peek_result(
    req: Request<Body>,
    peek_results_mux: PeekResultsMux,
) -> Box<dyn Future<Item = Response<Body>, Error = failure::Error> + Send> {
    Box::new(
        future::lazy(move || {
            let uuid = req
                .headers()
                .get("X-Materialize-Query-UUID")
                .ok_or_else(|| format_err!("missing uuid header"))?;
            let uuid = uuid::Uuid::parse_str(uuid.to_str()?)?;
            Ok((uuid, req))
        })
        .and_then(move |(uuid, req)| {
            req.into_body().concat2().from_err().and_then(move |body| {
                let rows: Vec<crate::repr::Datum> = bincode::deserialize(&body).unwrap();
                peek_results_mux
                    .read()
                    .map_err(|e| format_err!("{}", e))?
                    .sender(&uuid)?
                    .unbounded_send(rows)?;
                Ok(Response::new(Body::from("ok")))
            })
        }),
    )
}
