// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The HTTP console, for profiling (and in the future, possibly other) functionality.

use std::net::SocketAddr;

use axum::routing::{get, post};
use axum::Router;
use mz_build_info::BuildInfo;

/// Kick off the HTTP server.
pub async fn serve(listen_addr: SocketAddr, build_info: &'static BuildInfo) -> hyper::Result<()> {
    let app = Router::new()
        .route(
            "/prof",
            post(|form| mz_prof::http::handle_post(form, build_info)),
        )
        .route(
            "/prof",
            get(|query, headers| mz_prof::http::handle_get(query, headers, build_info)),
        );
    tracing::info!("serving dataflowd HTTP server on {}", listen_addr);
    axum::Server::bind(&listen_addr)
        .serve(app.into_make_service())
        .await
}
