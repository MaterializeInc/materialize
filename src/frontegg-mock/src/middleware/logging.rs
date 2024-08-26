// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::{extract::Request, middleware::Next, response::Response};
use mz_ore::instrument;
use std::time::Instant;

#[instrument]
pub async fn logging_middleware(request: Request, next: Next) -> Response {
    let start: Instant = Instant::now();
    let method: hyper::Method = request.method().clone();
    let uri = request.uri().clone();

    let response = next.run(request).await;

    let latency = start.elapsed();
    let status = response.status();

    tracing::info!(
        method = %method,
        uri = %uri,
        status = %status,
        latency = ?latency,
        "Request completed"
    );

    response
}
