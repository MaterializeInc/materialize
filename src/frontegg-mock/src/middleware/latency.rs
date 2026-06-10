// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::server::Context;
use axum::{
    extract::{Request, State},
    middleware::Next,
    response::IntoResponse,
};
use std::sync::Arc;

pub async fn latency_middleware(
    State(context): State<Arc<Context>>,
    request: Request,
    next: Next,
) -> impl IntoResponse {
    if let Some(latency) = context.latency {
        tokio::time::sleep(latency).await;
    }
    next.run(request).await
}
