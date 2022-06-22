// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP endpoints for the homepage and static files.

use askama::Template;
use axum::response::IntoResponse;

use crate::BUILD_INFO;

#[derive(Template)]
#[template(path = "http/templates/home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_time: &'a str,
    build_sha: &'static str,
}

pub async fn handle_home() -> impl IntoResponse {
    mz_http_util::template_response(HomeTemplate {
        version: BUILD_INFO.version,
        build_time: BUILD_INFO.time,
        build_sha: BUILD_INFO.sha,
    })
}

mz_http_util::make_handle_static!(
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/src/http/static"),
    "src/http/static",
    "src/http/static-dev"
);
