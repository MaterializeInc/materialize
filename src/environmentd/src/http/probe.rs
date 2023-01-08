// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Health check HTTP endpoints.

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::Extension;
use futures::FutureExt;
use http::StatusCode;
use serde::Deserialize;

use crate::http::Delayed;

/// Query parameters for [`handle_ready`].
#[derive(Deserialize)]
pub struct ReadyParams {
    /// Whether to wait for readiness or to return immediately if unready.
    #[serde(default)]
    wait: bool,
}

/// Handles a readiness probe.
pub async fn handle_ready(
    Extension(client): Extension<Delayed<mz_adapter::Client>>,
    query: Query<ReadyParams>,
) -> impl IntoResponse {
    // `environmentd` is ready to serve queries when the adapter client is
    // available.
    let is_ready = if query.wait {
        let _ = client.await;
        true
    } else {
        client.now_or_never().is_some()
    };
    match is_ready {
        false => (StatusCode::SERVICE_UNAVAILABLE, "not ready"),
        true => (StatusCode::OK, "ready"),
    }
}
