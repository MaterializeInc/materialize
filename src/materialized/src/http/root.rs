// Copyright Materialize, Inc. and contributors. All rights reserved.
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
#[cfg(feature = "dev-web")]
use tracing::debug;

use crate::http::util;
use crate::BUILD_INFO;

#[derive(Template)]
#[template(path = "http/templates/home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_time: &'a str,
    build_sha: &'static str,
}

pub fn handle_home(
    _: Request<Body>,
    _: &mut mz_coord::SessionClient,
) -> impl Future<Output = anyhow::Result<Response<Body>>> {
    future::ok(util::template_response(HomeTemplate {
        version: BUILD_INFO.version,
        build_time: BUILD_INFO.time,
        build_sha: BUILD_INFO.sha,
    }))
}

pub fn handle_static(
    req: Request<Body>,
    _: &mut mz_coord::SessionClient,
) -> Result<Response<Body>, anyhow::Error> {
    if req.method() == Method::GET {
        let path = req.uri().path();
        let path = path.strip_prefix('/').unwrap_or(path);
        match get_static_file(path) {
            Some(body) => Ok(Response::new(body)),
            None => Ok(util::error_response(StatusCode::NOT_FOUND, "not found")),
        }
    } else {
        Ok(util::error_response(StatusCode::FORBIDDEN, "bad request"))
    }
}

#[cfg(not(feature = "dev-web"))]
const STATIC_DIR: include_dir::Dir =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/src/http/static");

#[cfg(not(feature = "dev-web"))]
fn get_static_file(path: &str) -> Option<Body> {
    STATIC_DIR.get_file(path).map(|f| Body::from(f.contents()))
}

#[cfg(feature = "dev-web")]
fn get_static_file(path: &str) -> Option<Body> {
    use std::fs;

    #[cfg(not(debug_assertions))]
    compile_error!("cannot enable insecure `dev-web` feature in release mode");

    // Prefer the unminified files in static-dev, if they exist.
    let dev_path = format!(
        "{}/src/http/static-dev/{}",
        env!("CARGO_MANIFEST_DIR"),
        path
    );
    let prod_path = format!("{}/src/http/static/{}", env!("CARGO_MANIFEST_DIR"), path);
    match fs::read(dev_path).or_else(|_| fs::read(prod_path)) {
        Ok(contents) => Some(Body::from(contents)),
        Err(e) => {
            debug!("dev-web failed to load static file: {}: {}", path, e);
            None
        }
    }
}
