// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP endpoints for the homepage and static files.

use std::future::Future;

use askama::Template;
use futures::future;
use hyper::{Body, Method, Request, Response};
use include_dir::{include_dir, Dir};

use crate::http::{util, Server};

#[derive(Template)]
#[template(path = "http/templates/home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_time: &'a str,
    build_sha: &'static str,
}

const STATIC_DIR: Dir = include_dir!("src/http/static");

impl Server {
    pub fn handle_home(
        &self,
        _: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        future::ok(util::template_response(HomeTemplate {
            version: crate::VERSION,
            build_time: crate::BUILD_TIME,
            build_sha: crate::BUILD_SHA,
        }))
    }

    pub fn handle_static(
        &self,
        req: Request<Body>,
    ) -> impl Future<Output = anyhow::Result<Response<Body>>> {
        if req.method() == Method::GET {
            let path = req.uri().path();
            let path = path.strip_prefix("/").unwrap_or(path);
            future::ok(match STATIC_DIR.get_file(path) {
                Some(file) => Response::new(Body::from(file.contents())),
                None => Response::builder()
                    .status(404)
                    .body(Body::from("not found"))
                    .unwrap(),
            })
        } else {
            future::ok(
                Response::builder()
                    .status(403)
                    .body(Body::from("bad request"))
                    .unwrap(),
            )
        }
    }
}
