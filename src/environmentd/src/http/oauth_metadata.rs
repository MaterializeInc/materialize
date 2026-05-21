// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! OAuth 2.0 Protected Resource Metadata for the MCP endpoints.
//!
//! Implements [RFC 9728] so MCP-aware clients (e.g. Claude Desktop's Custom
//! Connectors, ChatGPT remote MCP) can discover the authorization server
//! they need to authenticate against in order to call our MCP endpoints.
//!
//! The discovery document is published at
//! `/.well-known/oauth-protected-resource` and is **public** (no auth).
//! Clients reach it either by following the `resource_metadata` parameter
//! emitted in a `WWW-Authenticate` challenge from a 401 on `/api/mcp/*`
//! (see [`crate::http::AuthError`]), or by probing the well-known URI
//! directly.
//!
//! The `authorization_servers` field is populated from the `oidc_issuer`
//! dyncfg, which doubles as the issuer that
//! [`mz_authenticator::GenericOidcAuthenticator`] already trusts for token
//! validation. Pointing clients at the same issuer we validate against
//! avoids the failure mode where the metadata document advertises an AS
//! whose tokens we then reject. If `oidc_issuer` is unset, the endpoint
//! returns 404 — the server is honest that no OAuth flow is available.
//!
//! [RFC 9728]: https://datatracker.ietf.org/doc/html/rfc9728

use axum::Extension;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use futures::future::Shared;
use http::StatusCode;
use mz_adapter::Client;
use mz_adapter_types::dyncfgs::OIDC_ISSUER;
use serde::Serialize;
use tokio::sync::oneshot;
use tracing::warn;

/// The well-known path served by this module.
///
/// Per [RFC 9728 §3] this is the OAuth 2.0 Protected Resource Metadata
/// well-known URI; MCP clients probe it as a fallback when no
/// `resource_metadata` parameter is present in a 401's `WWW-Authenticate`.
///
/// [RFC 9728 §3]: https://datatracker.ietf.org/doc/html/rfc9728#section-3
pub const PROTECTED_RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// JSON shape returned by [`PROTECTED_RESOURCE_METADATA_PATH`].
///
/// Fields are a strict subset of [RFC 9728 §2 "Protected Resource
/// Metadata"]; we emit only the fields MCP clients actually need today
/// (resource identifier, list of authorization servers, bearer method)
/// rather than padding the document with optional metadata the resource
/// server is not opinionated about. Adding fields later is
/// backwards-compatible per the RFC's extensibility guidance.
///
/// [RFC 9728 §2]: https://datatracker.ietf.org/doc/html/rfc9728#section-2
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ProtectedResourceMetadata {
    /// The canonical URL of this resource server. Clients use this as the
    /// `resource` parameter in their token request (RFC 8707).
    pub resource: String,
    /// One or more authorization servers a client may use to obtain a
    /// token. Each entry is an issuer URL whose own metadata document
    /// (RFC 8414 / OpenID Connect Discovery) can be fetched at
    /// `<issuer>/.well-known/oauth-authorization-server` or
    /// `<issuer>/.well-known/openid-configuration`.
    pub authorization_servers: Vec<String>,
    /// Mechanisms by which the client may present the bearer token.
    /// We accept the `Authorization` header only.
    pub bearer_methods_supported: Vec<String>,
}

/// Convenience for our delayed-adapter-client extension type.
///
/// Kept aliased here so the handler does not have to import `futures::Shared`
/// directly. Matches the pattern used elsewhere in `crate::http`.
type Delayed<T> = Shared<oneshot::Receiver<T>>;

/// HTTP handler for [`PROTECTED_RESOURCE_METADATA_PATH`].
///
/// Always public — no authentication is performed, by design. RFC 9728
/// places no auth requirements on this endpoint; clients must be able to
/// fetch it before they have a token.
///
/// Returns 503 if the adapter client is not yet available (a brief window
/// at startup), 404 if no `oidc_issuer` is configured (no OAuth is
/// available, so there is nothing to advertise), and 200 with the JSON
/// document otherwise.
pub async fn handle_protected_resource_metadata(
    Extension(adapter_client_rx): Extension<Delayed<Client>>,
    req: Request,
) -> Response {
    // The MCP spec considers the resource server's canonical URI (used in
    // RFC 8707 `resource` parameters and as the `aud` constraint) to be
    // the URL the client used to reach the server. Build that from
    // forwarded headers when present, with a sane fallback to the request
    // Host. We deliberately do not consult a server-side configuration
    // here: if the deployment sits behind a proxy that rewrites Host, the
    // proxy is also responsible for setting `X-Forwarded-*`.
    let scheme = forwarded_scheme(&req).unwrap_or("https").to_string();
    let host = match forwarded_host(&req) {
        Some(h) => h,
        None => {
            warn!("oauth-protected-resource: missing Host header on request");
            return (StatusCode::BAD_REQUEST, "missing Host header").into_response();
        }
    };
    let resource = format!("{scheme}://{host}/api/mcp");

    let Ok(adapter_client) = adapter_client_rx.clone().await else {
        return (StatusCode::SERVICE_UNAVAILABLE, "adapter not ready").into_response();
    };
    let system_vars = adapter_client.get_system_vars().await;
    let Some(issuer) = OIDC_ISSUER.get(system_vars.dyncfgs()) else {
        // No OAuth authorization server is configured. Per RFC 9728 the
        // document MUST contain at least one entry in
        // `authorization_servers`, so the honest response is 404 rather
        // than an empty document that misleads the client.
        return StatusCode::NOT_FOUND.into_response();
    };

    let metadata = ProtectedResourceMetadata {
        resource,
        authorization_servers: vec![issuer.to_string()],
        bearer_methods_supported: vec!["header".to_string()],
    };

    axum::Json(metadata).into_response()
}

/// Builds the absolute URL of the protected resource metadata document
/// for use as the `resource_metadata` parameter in a `WWW-Authenticate`
/// challenge. Returns `None` if the request lacks enough host information
/// to construct a URL; the caller is expected to skip the Bearer challenge
/// in that case rather than emit a malformed value.
pub fn metadata_url(req: &Request) -> Option<String> {
    let host = forwarded_host(req)?;
    let scheme = forwarded_scheme(req).unwrap_or("https");
    Some(format!("{scheme}://{host}{PROTECTED_RESOURCE_METADATA_PATH}"))
}

/// Pulls `X-Forwarded-Host` first, then falls back to the request's `Host`
/// header. Both are checked for validity (non-empty, ASCII-only) before
/// use; an invalid value returns `None` so the caller can refuse the
/// request rather than serve a metadata document with a garbage URL.
fn forwarded_host(req: &Request) -> Option<String> {
    req.headers()
        .get("x-forwarded-host")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.headers().get(http::header::HOST).and_then(|v| v.to_str().ok()))
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Picks the request's outward-facing scheme. Prefers `X-Forwarded-Proto`
/// because requests are usually TLS-terminated at a proxy; if absent,
/// returns `None` and lets the caller default to `https` (TLS is required
/// by OAuth 2.1 for all endpoints anyway, so we never want to advertise
/// `http://`).
fn forwarded_scheme(req: &Request) -> Option<&'static str> {
    req.headers()
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|s| if s.eq_ignore_ascii_case("http") { "http" } else { "https" })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serialized document must use exactly the field names RFC 9728
    /// defines — clients key off them. A rename would break every
    /// connected client silently, so this test pins the wire format.
    #[mz_ore::test]
    fn test_metadata_serialization_matches_rfc9728_field_names() {
        let metadata = ProtectedResourceMetadata {
            resource: "https://mcp.example.com/api/mcp".to_string(),
            authorization_servers: vec!["https://auth.example.com".to_string()],
            bearer_methods_supported: vec!["header".to_string()],
        };
        let json = serde_json::to_value(&metadata).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "resource": "https://mcp.example.com/api/mcp",
                "authorization_servers": ["https://auth.example.com"],
                "bearer_methods_supported": ["header"],
            }),
        );
    }

    /// MCP clients construct the well-known URI from the documented path
    /// suffix. The constant is part of our public contract with them.
    #[mz_ore::test]
    fn test_well_known_path_is_rfc9728_canonical() {
        assert_eq!(
            PROTECTED_RESOURCE_METADATA_PATH,
            "/.well-known/oauth-protected-resource",
        );
    }
}
