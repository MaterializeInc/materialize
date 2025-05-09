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
#[template(path = "home.html")]
struct HomeTemplate<'a> {
    version: &'a str,
    build_sha: &'static str,
    profiling: bool,
}

pub async fn handle_home(profiling: bool) -> impl IntoResponse {
    mz_http_util::template_response(HomeTemplate {
        version: BUILD_INFO.version,
        build_sha: BUILD_INFO.sha,
        profiling,
    })
}

mz_http_util::make_handle_static!(
    dir_1: ::include_dir::include_dir!("$CARGO_MANIFEST_DIR/src/http/static"),
    dir_2: ::include_dir::include_dir!("$OUT_DIR/src/http/static"),
    prod_base_path: "src/http/static",
    dev_base_path: "src/http/static-dev",
);
