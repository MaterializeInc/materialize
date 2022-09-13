// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::response::IntoResponse;
use axum::Json;
use http::StatusCode;

use crate::http::{AuthedClient, HttpSqlRequest};

pub async fn handle_sql(
    mut client: AuthedClient,
    Json(request): Json<HttpSqlRequest>,
) -> impl IntoResponse {
    match client.execute_sql_http_request(request).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}
