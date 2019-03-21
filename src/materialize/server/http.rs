// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::{bail, format_err};
use futures::{future, Future, IntoFuture, Poll};
use hyper::service;
use hyper::{Body, Method, Request, Response};
use std::collections::hash_map::Entry;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

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
    conn_state: super::ConnState,
) -> impl Future<Item = (), Error = failure::Error> {
    let svc =
        service::service_fn(
            move |req: Request<Body>| match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => handle_home(req).left(),
                (&Method::POST, "/api/peek-results") => {
                    handle_peek_result(req, &conn_state).right()
                }
                _ => handle_unknown(req).left(),
            },
        );
    let http = hyper::server::conn::Http::new();
    http.serve_connection(a, svc).err_into()
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
    conn_state: &super::ConnState,
) -> Box<dyn Future<Item = Response<Body>, Error = failure::Error> + Send> {
    let server_state = conn_state.server_state.clone();
    Box::new(
        future::lazy(move || {
            let uuid = req
                .headers()
                .get("X-Materialize-Query-UUID")
                .ok_or(format_err!("missing uuid header"))?;
            let uuid = uuid::Uuid::parse_str(uuid.to_str()?)?;
            let tx = {
                let mut server_state = server_state.write().unwrap();
                match server_state.peek_results.entry(uuid) {
                    Entry::Occupied(mut entry) => {
                        let (tx, rc) = entry.get_mut();
                        if *rc == 1 {
                            let (_, (tx, _)) = entry.remove_entry();
                            tx.clone()
                        } else {
                            *rc -= 1;
                            tx.clone()
                        }
                    }
                    Entry::Vacant(_) => bail!("unknown UUID"),
                }
            };
            Ok((tx, req))
        })
        .and_then(|(tx, req)| {
            use futures::stream::Stream;
            req.into_body().concat2().err_into().and_then(|body| {
                let rows: Vec<Vec<crate::dataflow::Scalar>> = bincode::deserialize(&body).unwrap();
                futures::stream::iter_ok(rows).forward(tx)
            })
        })
        .and_then(|_| Ok(Response::new(Body::from("ok")))),
    )
}
