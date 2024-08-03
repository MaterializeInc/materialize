// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! Console Impersonation HTTP endpoint.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use http::header::HOST;
use http::HeaderValue;
use hyper::Uri;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

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
        let mut url = proxy_url.unwrap_or("https://console.materialize.com".to_string());
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
