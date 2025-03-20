// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::{Future, IntoFuture};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use axum::{
    extract::Query,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use mz_frontegg_auth::AppPassword;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::error::Error;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    client_id: String,
    secret: String,
}

// Please update this link if the logo location changes in the future.
const LOGO_URL: &str = "https://materialize.com/svgs/brand-guide/materialize-purple-mark.svg";

/// Produces an HTML string formatted
/// with a message centered in the middle of the page
/// and Materialize logo on top
fn format_as_html_message(msg: &str) -> Html<String> {
    Html(String::from(&format!(" \
        <body style=\"margin: 0; display: flex; justify-content: center; align-items: center; min-height: 100vh; background-color: #f0f0f0;\">
            <div style=\"text-align: center; padding: 100px; background-color: #ffffff; border-radius: 10px; box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);\"> \
                <img src=\"{}\"> \
                <h2 style=\"padding-top: 20px; font-family: Inter, Arial, sans-serif;\">{}</h2> \
            </div>
        </body>
    ", LOGO_URL, msg)))
}

/// Request handler for the server waiting the browser API token creation
/// Axum requires the handler be async even though we don't await
#[allow(clippy::unused_async)]
async fn request(
    Query(BrowserAPIToken { secret, client_id }): Query<BrowserAPIToken>,
    tx: UnboundedSender<Result<AppPassword, Error>>,
) -> Response {
    if secret.len() == 0 && client_id.len() == 0 {
        tx.send(Err(Error::LoginOperationCanceled))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        return format_as_html_message("Login canceled. You can now close the tab.")
            .into_response();
    }

    let client_id = client_id.parse::<Uuid>();
    let secret = secret.parse::<Uuid>();
    if let (Ok(client_id), Ok(secret)) = (client_id, secret) {
        let app_password = AppPassword {
            client_id,
            secret_key: secret,
        };
        tx.send(Ok(app_password))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        format_as_html_message("You can now close the tab.").into_response()
    } else {
        tx.send(Err(Error::InvalidAppPassword))
            .unwrap_or_else(|_| panic!("Error handling login details."));
        format_as_html_message(
            "Invalid credentials. Please, try again or communicate with support.",
        )
        .into_response()
    }
}

/// Server for handling login's information.
pub async fn server(
    tx: UnboundedSender<Result<AppPassword, Error>>,
) -> (impl Future<Output = Result<(), std::io::Error>>, u16) {
    let app = Router::new().route("/", get(|body| request(body, tx)));
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let listener = TcpListener::bind(addr).await.unwrap_or_else(|e| {
        panic!("error binding to {}: {}", addr, e);
    });
    let port = listener.local_addr().unwrap().port();
    let server = axum::serve(listener, app.into_make_service());

    (server.into_future(), port)
}
