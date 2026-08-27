// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

//! HTTP endpoints for the web console.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use axum::Extension;
use axum::Json;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use http::header::{COOKIE, HOST, LOCATION, SET_COOKIE};
use http::{HeaderMap, HeaderValue};
use hyper::Uri;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use mz_adapter_types::dyncfgs::{CONSOLE_OIDC_CLIENT_ID, CONSOLE_OIDC_SCOPES, OIDC_ISSUER};

use crate::http::Delayed;

/// Query parameter that selects (or, with an empty value, clears) the console
/// preview build this proxy serves to the requesting browser.
const PREVIEW_BUILD_PARAM: &str = "preview_build";

/// Cookie storing the selected preview build label.
const PREVIEW_BUILD_COOKIE: &str = "mz_console_preview_build";

/// Preview selections expire after a day so stale cookies drift back to the
/// default build.
const PREVIEW_BUILD_COOKIE_MAX_AGE_SECS: u64 = 60 * 60 * 24;

pub(crate) struct ConsoleProxyConfig {
    /// Hyper http client, supports https.
    client: Client<HttpsConnector<HttpConnector>, Body>,

    /// URL of upstream console to proxy to (e.g. <https://console.materialize.com>).
    url: String,

    /// Route this is being served from (e.g. /internal-console).
    route_prefix: String,

    /// Host of `url`, under which preview builds are served as subdomains
    /// (e.g. `<label>.internal.console.materialize.com`).
    preview_host_suffix: Option<String>,
}

impl ConsoleProxyConfig {
    pub(crate) fn new(proxy_url: Option<String>, route_prefix: String) -> Self {
        let mut url = proxy_url.unwrap_or_else(|| "https://console.materialize.com".to_string());
        if let Some(new) = url.strip_suffix('/') {
            url = new.to_string();
        }
        let preview_host_suffix = Uri::try_from(url.as_str())
            .ok()
            .and_then(|uri| uri.host().map(|host| host.to_string()));
        Self {
            client: Client::builder(TokioExecutor::new()).build(HttpsConnector::new()),
            url,
            route_prefix,
            preview_host_suffix,
        }
    }

    /// Returns the upstream URL serving the given preview build, or `None` if
    /// the label is invalid or no preview host suffix could be derived.
    ///
    /// NOTE: This proxy runs inside the environment's network, so it must not
    /// be usable for SSRF. Preview builds are only ever fetched over https
    /// from a validated subdomain of the configured upstream host, never from
    /// a caller-provided URL.
    fn preview_url(&self, label: &str) -> Option<String> {
        let suffix = self.preview_host_suffix.as_deref()?;
        if !is_valid_preview_build_label(label) {
            return None;
        }
        Some(format!("https://{label}.{suffix}"))
    }
}

/// A valid preview build label is a DNS label: 1-63 characters of lowercase
/// ASCII alphanumerics and hyphens, not starting or ending with a hyphen.
fn is_valid_preview_build_label(label: &str) -> bool {
    (1..=63).contains(&label.len())
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !label.starts_with('-')
        && !label.ends_with('-')
}

/// OIDC configuration values needed by the Console to initiate OIDC login.
static CONSOLE_CONFIG_VAR_NAMES: LazyLock<[&'static str; 3]> = LazyLock::new(|| {
    [
        OIDC_ISSUER.name(),
        CONSOLE_OIDC_CLIENT_ID.name(),
        CONSOLE_OIDC_SCOPES.name(),
    ]
});

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
    let mut config: BTreeMap<&str, String> = BTreeMap::new();
    for var_name in CONSOLE_CONFIG_VAR_NAMES.iter() {
        let value = system_vars.get(var_name).map(|v| v.value()).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to retrieve system variable {var_name}"),
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
///
/// `?preview_build=<label>` selects a per-browser preview build served from a
/// subdomain of the upstream host; an empty value returns to the default.
pub(crate) async fn handle_internal_console(
    console_config: Extension<Arc<ConsoleProxyConfig>>,
    mut req: Request<Body>,
) -> Result<Response, StatusCode> {
    if let Some(response) = preview_build_selection_response(&console_config, &req)? {
        return Ok(response);
    }

    let upstream_url = preview_build_from_cookie(req.headers())
        .and_then(|label| console_config.preview_url(&label))
        .unwrap_or_else(|| console_config.url.clone());

    let path = req.uri().path();
    let mut path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or(path);
    if let Some(stripped_path_query) = path_query.strip_prefix(&console_config.route_prefix) {
        path_query = stripped_path_query;
    }

    let uri = Uri::try_from(format!("{}{}", upstream_url, path_query)).unwrap();
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

/// Handles the `?preview_build=<label>` selection parameter: stores the
/// selection in a cookie and redirects back to the same path without the
/// parameter, so subsequent asset requests carry the choice. Returns `None`
/// when the parameter is absent.
fn preview_build_selection_response(
    console_config: &ConsoleProxyConfig,
    req: &Request<Body>,
) -> Result<Option<Response>, StatusCode> {
    let query = req.uri().query().unwrap_or("");
    let mut selection = None;
    let mut remaining_query = url::form_urlencoded::Serializer::new(String::new());
    let mut any_remaining = false;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key == PREVIEW_BUILD_PARAM {
            selection = Some(value.into_owned());
        } else {
            remaining_query.append_pair(&key, &value);
            any_remaining = true;
        }
    }
    let Some(label) = selection else {
        return Ok(None);
    };

    let cookie_attributes = format!(
        "Path={}; Secure; HttpOnly; SameSite=Lax",
        console_config.route_prefix
    );
    let cookie = if label.is_empty() {
        // An empty label clears the selection.
        format!("{PREVIEW_BUILD_COOKIE}=; Max-Age=0; {cookie_attributes}")
    } else {
        if console_config.preview_url(&label).is_none() {
            return Err(StatusCode::BAD_REQUEST);
        }
        format!(
            "{PREVIEW_BUILD_COOKIE}={label}; \
             Max-Age={PREVIEW_BUILD_COOKIE_MAX_AGE_SECS}; {cookie_attributes}"
        )
    };

    let mut location = req.uri().path().to_string();
    if any_remaining {
        location.push('?');
        location.push_str(&remaining_query.finish());
    }
    let response = Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(LOCATION, location)
        .header(SET_COOKIE, cookie)
        .body(Body::empty())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Some(response))
}

/// Returns the preview build label from the request's cookies, if one is set
/// and valid. Invalid values are ignored rather than rejected so a stale
/// cookie can never break the default console.
fn preview_build_from_cookie(headers: &HeaderMap) -> Option<String> {
    for header in headers.get_all(COOKIE) {
        let Ok(header) = header.to_str() else {
            continue;
        };
        for pair in header.split(';') {
            let mut parts = pair.trim().splitn(2, '=');
            if parts.next() == Some(PREVIEW_BUILD_COOKIE) {
                let label = parts.next().unwrap_or("");
                if is_valid_preview_build_label(label) {
                    return Some(label.to_string());
                }
            }
        }
    }
    None
}
