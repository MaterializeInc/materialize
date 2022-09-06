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
use serde::Deserialize;

use crate::http::AuthedClient;

#[derive(Deserialize, Debug)]
pub struct SqlRequestParam {
    value: String,
}

#[derive(Deserialize, Debug)]
pub struct SqlRequest {
    query: String,
    params: Option<Vec<SqlRequestParam>>,
}

pub async fn handle_sql(
    AuthedClient(mut client): AuthedClient,
    Json(request): Json<Vec<SqlRequest>>,
) -> impl IntoResponse {
    let mut requests = vec![];
    for SqlRequest { query, params } in &request {
        requests.push((
            query.as_str(),
            params
                .as_ref()
                .map(|params| {
                    params
                        .iter()
                        .map(|SqlRequestParam { value }| value.as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        ));
    }
    match client.simple_execute(requests).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}
