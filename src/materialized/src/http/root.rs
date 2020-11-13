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
use hyper::{Body, Method, Request, Response, StatusCode};

use crate::http::{util, Server};

#[derive(Template)]
#[template(path = "http/templates/home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_time: &'a str,
    build_sha: &'static str,
}

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
        async move {
            if req.method() == Method::GET {
                let path = req.uri().path();
                let path = path.strip_prefix("/").unwrap_or(path);
                match get_static_file(path).await {
                    Some(body) => Ok(Response::new(body)),
                    None => Ok(util::error_response(StatusCode::NOT_FOUND, "not found")),
                }
            } else {
                Ok(util::error_response(StatusCode::FORBIDDEN, "bad request"))
            }
        }
    }
}

#[cfg(not(feature = "dev-web"))]
const STATIC_DIR: include_dir::Dir = include_dir::include_dir!("src/http/static");

#[cfg(not(feature = "dev-web"))]
async fn get_static_file(path: &str) -> Option<Body> {
    STATIC_DIR.get_file(path).map(|f| Body::from(f.contents()))
}

#[cfg(feature = "dev-web")]
async fn get_static_file(path: &str) -> Option<Body> {
    #[cfg(not(debug_assertions))]
    compile_error!("cannot enable insecure `dev-web` feature in release mode");

    let path = format!("{}/src/http/static/{}", env!("CARGO_MANIFEST_DIR"), path);
    match tokio::fs::read(&path).await {
        Ok(contents) => Some(Body::from(contents)),
        Err(e) => {
            log::debug!("dev-web failed to load static file: {}: {}", path, e);
            None
        }
    }
}
