// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::response::IntoResponse;
use axum::Json;
use futures::TryStreamExt;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use mz_coord::SessionClient;
use tokio::pin;

use crate::http::AuthedClient;

#[derive(Deserialize)]
pub struct SqlRequest {
    sql: String,
}

pub async fn handle_sql(
    AuthedClient(mut client): AuthedClient,
    Json(SqlRequest { sql }): Json<SqlRequest>,
) -> impl IntoResponse {
    match client.simple_execute(&sql).await {
        Ok(res) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn handle_sql_ws(
    AuthedClient(mut client): AuthedClient,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(|mut ws| async move {
        if let Err(e) = run_ws(&mut ws, &mut client).await {
            let _ = send_ws_response(
                &mut ws,
                ErrorResponse {
                    error: e.to_string(),
                },
            );
        }
    })
}

async fn run_ws(ws: &mut WebSocket, client: &mut SessionClient) -> Result<(), anyhow::Error> {
    let request = match ws.recv().await.transpose()? {
        None => return Ok(()),
        Some(request) => request.into_text()?,
    };
    let SqlRequest { sql } = serde_json::from_str(&request)?;
    let rx = client.simple_tail(&sql).await?;
    pin!(rx);
    while let Some(row) = rx.try_next().await? {
        send_ws_response(ws, row).await?;
    }
    Ok(())
}

async fn send_ws_response<T>(ws: &mut WebSocket, response: T) -> Result<(), axum::Error>
where
    T: Serialize,
{
    ws.send(Message::Text(serde_json::to_string(&response).unwrap()))
        .await
}
