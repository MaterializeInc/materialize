// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;

use anyhow::{bail, Context, Ok, Result};
use axum::http::StatusCode;
use axum::{extract::Query, response::IntoResponse, routing::get, Router};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use tokio::sync::broadcast::{channel, Sender};

use crate::configuration::{Configuration, Endpoint, FronteggAPIToken, FronteggAuth};
use crate::utils::trim_newline;
use crate::BrowserAPIToken;

/// Request handler for the server waiting the browser API token creation
// Axum requires the handler be async even though we don't await
#[allow(clippy::unused_async)]
async fn request(
    Query(BrowserAPIToken {
        email,
        secret,
        client_id,
    }): Query<BrowserAPIToken>,
    tx: Sender<Option<(String, FronteggAPIToken)>>,
) -> impl IntoResponse {
    if !secret.is_empty() {
        tx.send(Some((email, FronteggAPIToken { client_id, secret })))
            .unwrap();
    } else {
        tx.send(None).unwrap();
    }

    (StatusCode::OK, "You can now close the tab.")
}

/// Log the user using the browser, generates an API token and saves the new profile data.
pub(crate) async fn login_with_browser(
    endpoint: Endpoint,
    profile_name: &str,
    config: &mut Configuration,
) -> Result<()> {
    // Open the browser to login user.
    let url = endpoint.web_login_url(profile_name).to_string();
    if let Err(err) = open::that(&url) {
        bail!("An error occurred when opening '{}': {}", url, err)
    }

    // Start the server to handle the request response
    let (tx, mut result) = channel(1);
    let mut close = tx.subscribe();
    let app = Router::new().route("/", get(|body| request(body, tx)));
    mz_ore::task::spawn(|| "server_task", async {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8808));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async move {
                close.recv().await.ok();
            })
            .await
    });

    match result
        .recv()
        .await
        .context("failed to retrive new profile")?
    {
        Some((email, api_token)) => {
            config.create_or_update_profile(endpoint, profile_name.to_string(), email, api_token);
            Ok(())
        }
        None => bail!("failed to login via browser"),
    }
}

/// Generates an API token using an access token
pub(crate) async fn generate_api_token(
    endpoint: &Endpoint,
    client: &Client,
    access_token_response: FronteggAuth,
    description: &String,
) -> Result<FronteggAPIToken, reqwest::Error> {
    let authorization: String = format!("Bearer {}", access_token_response.access_token);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(authorization.as_str()).unwrap(),
    );
    let mut body = HashMap::new();
    body.insert("description", description);

    client
        .post(endpoint.api_token_url())
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<FronteggAPIToken>()
        .await
}

/// Generates an access token using an API token
async fn authenticate_user(
    endpoint: &Endpoint,
    client: &Client,
    email: &str,
    password: &str,
) -> Result<FronteggAuth> {
    let mut access_token_request_body = HashMap::new();
    access_token_request_body.insert("email", email);
    access_token_request_body.insert("password", password);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let response = client
        .post(endpoint.user_auth_url())
        .headers(headers)
        .json(&access_token_request_body)
        .send()
        .await?;

    match response.status() {
        StatusCode::UNAUTHORIZED => {
            bail!("Invalid user or password")
        }
        _ => response
            .json::<FronteggAuth>()
            .await
            .context("failed to parse response from server"),
    }
}

/// Log the user using the console, generates an API token and saves the new profile data.
pub(crate) async fn login_with_console(
    endpoint: Endpoint,
    profile_name: &String,
    config: &mut Configuration,
) -> Result<()> {
    // Handle interactive user input
    let mut email = String::new();

    print!("Email: ");
    let _ = std::io::stdout().flush();
    std::io::stdin().read_line(&mut email).unwrap();
    trim_newline(&mut email);

    print!("Password: ");
    let _ = std::io::stdout().flush();
    let password = rpassword::read_password().unwrap();

    let client = Client::new();

    // Check if there is a secret somewhere.
    // If there is none save the api token someone on the root folder.
    let auth_user = authenticate_user(&endpoint, &client, &email, &password).await?;
    let api_token = generate_api_token(
        &endpoint,
        &client,
        auth_user,
        &String::from("App password for the CLI"),
    )
    .await?;

    config.create_or_update_profile(endpoint, profile_name.to_string(), email, api_token);

    Ok(())
}
