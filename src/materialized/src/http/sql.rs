// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::Form;
use axum::response::IntoResponse;
use axum::Json;
use http::StatusCode;
use serde::Deserialize;

use crate::http::AuthedClient;

#[derive(Deserialize)]
pub struct SqlForm {
    sql: String,
}

pub async fn handle_sql(
    AuthedClient(mut client): AuthedClient,
    Form(SqlForm { sql }): Form<SqlForm>,
) -> impl IntoResponse {
    match client.simple_execute(&sql).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}
