// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Catalog introspection HTTP endpoints.

use axum::response::IntoResponse;
use axum::TypedHeader;
use headers::ContentType;
use http::StatusCode;

use crate::http::AuthedClient;

pub async fn handle_catalog_dump(mut client: AuthedClient) -> impl IntoResponse {
    match client.client.dump_catalog().await.map(|c| c.into_string()) {
        Ok(res) => Ok((TypedHeader(ContentType::json()), res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

pub async fn handle_catalog_check(mut client: AuthedClient) -> impl IntoResponse {
    let response = match client.client.check_catalog().await {
        Ok(_) => serde_json::Value::String("".to_string()),
        Err(inconsistencies) => serde_json::json!({ "err": inconsistencies }),
    };
    (TypedHeader(ContentType::json()), response.to_string())
}

pub async fn handle_coordinator_check(mut client: AuthedClient) -> impl IntoResponse {
    let response = match client.client.check_coordinator().await {
        Ok(_) => serde_json::Value::String("".to_string()),
        Err(inconsistencies) => serde_json::json!({ "err": inconsistencies }),
    };
    (TypedHeader(ContentType::json()), response.to_string())
}
