// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Console Impersonation HTTP endpoint.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use http::header::HOST;
use hyper::Uri;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mz_adapter_types::dyncfgs::{CONSOLE_OIDC_CLIENT_ID, CONSOLE_OIDC_SCOPES, OIDC_ISSUER};

use crate::http::Delayed;

pub(crate) struct ConsoleProxyConfig {
    /// Hyper http client, supports https.
    client: Client<HttpsConnector<HttpConnector>, Body>,

    /// URL of upstream console to proxy to (e.g. <https://console.materialize.com>).
    url: String,

    /// Route this is being served from (e.g. /internal-console).
    route_prefix: String,
}

impl ConsoleProxyConfig {
    pub(crate) fn new(proxy_url: Option<String>, route_prefix: String) -> Self {
        let mut url = proxy_url.unwrap_or_else(|| "https://console.materialize.com".to_string());
        if let Some(new) = url.strip_suffix('/') {
            url = new.to_string();
        }
        Self {
            client: Client::builder(TokioExecutor::new()).build(HttpsConnector::new()),
            url,
            route_prefix,
        }
    }
}

/// Returns system variable values the web console needs from
/// environmentd. This endpoint requires no authentication.
pub async fn handle_console_config(
    Extension(adapter_client_rx): Extension<Delayed<mz_adapter::Client>>,
) -> Result<Response, (StatusCode, String)> {
    let adapter_client = adapter_client_rx.await.map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Adapter client unavailable".to_string(),
        )
    })?;

    let system_vars = adapter_client.get_system_vars().await;

    let var_names = [
        // OIDC configuration values needed by the Console to initiate OIDC
        // login.
        OIDC_ISSUER.name(),
        CONSOLE_OIDC_CLIENT_ID.name(),
        CONSOLE_OIDC_SCOPES.name(),
    ];
    let mut config: BTreeMap<&str, String> = BTreeMap::new();
    for var_name in var_names {
        let value = system_vars.get(var_name).map(|v| v.value()).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Unable to get system var {var_name}"),
            )
        })?;
        config.insert(var_name, value);
    }

    Ok((StatusCode::OK, Json(config)).into_response())
}

/// The User Impersonation feature uses a Teleport proxy in front of the
/// Internal HTTP Server, however Teleport has issues with CORS that prevent
/// making requests to that Teleport-proxied app from our production console URLs.
/// To avoid CORS and serve the Console from the same host as the Teleport app,
/// this route proxies the upstream Console to handle requests for
/// HTML, JS, and CSS static files.
pub(crate) async fn handle_internal_console(
    console_config: Extension<Arc<ConsoleProxyConfig>>,
    mut req: Request<Body>,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    let mut path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or(path);
    if let Some(stripped_path_query) = path_query.strip_prefix(&console_config.route_prefix) {
        path_query = stripped_path_query;
    }

    let uri = Uri::try_from(format!("{}{}", &console_config.url, path_query)).unwrap();
    let host = uri.host().unwrap().to_string();
    // Preserve the request, but update the URI to point upstream.
    *req.uri_mut() = uri;

    // If vercel sees the request being served from a different host it tries to redirect to it's own.
    req.headers_mut()
        .insert(HOST, HeaderValue::from_str(&host).unwrap());

    // Call this request against the upstream, return response directly.
    Ok(console_config
        .client
        .request(req)
        .await
        .map_err(|err| {
            tracing::warn!("Error retrieving console url: {}", err);
            StatusCode::BAD_REQUEST
        })?
        .into_response())
}
