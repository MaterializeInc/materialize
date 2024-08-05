// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! HTTP endpoints for controlling the coordinator and the controllers.

use axum::response::IntoResponse;
use axum_extra::TypedHeader;
use headers::ContentType;
use http::StatusCode;

use crate::http::AuthedClient;

pub async fn handle_controller_allow_writes(mut client: AuthedClient) -> impl IntoResponse {
    match client
        .client
        .controller_allow_writes()
        .await
        .map(|c| c.to_string())
    {
        Ok(res) => Ok((TypedHeader(ContentType::text()), res)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
