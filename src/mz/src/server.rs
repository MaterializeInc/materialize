// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use axum::{
    extract::Query,
    response::IntoResponse,
    routing::{get, IntoMakeService},
    Router, Server,
};
use mz_frontegg_auth::AppPassword;
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::error::Error;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    client_id: String,
    secret: String,
}

/// Request handler for the server waiting the browser API token creation
/// Axum requires the handler be async even though we don't await
#[allow(clippy::unused_async)]
async fn request(
    Query(BrowserAPIToken { secret, client_id }): Query<BrowserAPIToken>,
    tx: UnboundedSender<Result<AppPassword, Error>>,
) -> impl IntoResponse {
    if secret.len() == 0 && client_id.len() == 0 {
        tx.send(Err(Error::LoginOperationCanceled))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        return (StatusCode::OK, "Login canceled. You can now close the tab.");
    }

    let client_id = client_id.parse::<Uuid>();
    let secret = secret.parse::<Uuid>();
    if client_id.is_ok() && secret.is_ok() {
        let app_password = AppPassword {
            client_id: client_id.unwrap(),
            secret_key: secret.unwrap(),
        };
        tx.send(Ok(app_password))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        (StatusCode::OK, "You can now close the tab.")
    } else {
        tx.send(Err(Error::InvalidAppPassword))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        (
            StatusCode::OK,
            "Invalid credentials. Please, try again or communicate with support.",
        )
    }
}

/// Server for handling login's information.
pub fn server(
    tx: UnboundedSender<Result<AppPassword, Error>>,
) -> (
    Server<hyper::server::conn::AddrIncoming, IntoMakeService<Router>>,
    u16,
) {
    let app = Router::new().route("/", get(|body| request(body, tx)));
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let server = axum::Server::bind(&addr).serve(app.into_make_service());
    let port = server.local_addr().port();

    (server, port)
}
