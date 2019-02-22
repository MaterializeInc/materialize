// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use futures::{future, Future};
use hyper::service;
use hyper::{Body, Method, Request, Response};
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

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
) -> impl Future<Item = (), Error = hyper::Error> {
    let svc = service::service_fn(|req: Request<Body>| {
        if req.method() == Method::GET && req.uri().path() == "/" {
            future::ok::<_, io::Error>(Response::new(Body::from("materialized v0.0.1")))
        } else {
            future::ok::<_, io::Error>(
                Response::builder()
                    .status(403)
                    .body(Body::from("bad request"))
                    .unwrap(),
            )
        }
    });
    let http = hyper::server::conn::Http::new();
    http.serve_connection(a, svc)
}
