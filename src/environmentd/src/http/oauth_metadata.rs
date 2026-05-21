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
//! ## Host derivation
//!
//! The `resource` field and the `resource_metadata` URL embedded in the
//! 401 challenge are both absolute URLs and therefore depend on knowing
//! the externally-visible host of this environment. We resolve that in
//! this order:
//!
//!   1. `HttpConfig::http_host_name` (set by the operator).
//!   2. The request's `Host` header.
//!
//! We deliberately do **not** consult `X-Forwarded-Host` or
//! `X-Forwarded-Proto`: environmentd has no proxy-trust configuration
//! today, and trusting those headers blind would let any client reaching
//! the server directly poison the published metadata URLs (a host-header
//! injection on the OAuth flow). Deployments behind a load balancer that
//! rewrites `Host` are expected to set `http_host_name` explicitly.
//!
//! ## When the document is published
//!
//! The handler returns 404 when no OAuth flow is meaningful for the
//! listener — either because `oidc_issuer` is unset (no authorization
//! server to advertise) or because the listener's authenticator is
//! [`listeners::AuthenticatorKind::None`] (no token would ever be
//! validated). Returning an empty or fake document instead would mislead
//! clients into starting an OAuth dance they cannot complete.
//!
//! [RFC 9728]: https://datatracker.ietf.org/doc/html/rfc9728

use axum::Extension;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::{HeaderValue, StatusCode};
use mz_adapter::Client;
use mz_adapter_types::dyncfgs::OIDC_ISSUER;
use mz_server_core::listeners;
use serde::Serialize;
use tracing::warn;

use crate::http::Delayed;

/// The well-known path served by this module.
///
/// Per [RFC 9728 §3] this is the OAuth 2.0 Protected Resource Metadata
/// well-known URI; MCP clients probe it as a fallback when no
/// `resource_metadata` parameter is present in a 401's `WWW-Authenticate`.
///
/// [RFC 9728 §3]: https://datatracker.ietf.org/doc/html/rfc9728#section-3
pub const PROTECTED_RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// `Cache-Control` value set on the discovery response.
///
/// One hour is a balance: long enough that well-behaved clients reuse the
/// document across a session instead of re-fetching per request, short
/// enough that an admin who changes `oidc_issuer` does not have to wait a
/// long tail of stale caches out. RFC 9728 places no requirement here;
/// this is a pragmatic default.
const METADATA_CACHE_CONTROL: &str = "public, max-age=3600";

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

/// Per-listener inputs the discovery handler needs.
///
/// Carried as an axum `Extension` rather than reading from system config
/// directly because the values are per-listener (`authenticator_kind`,
/// `http_host_name`) — the same environmentd process can serve listeners
/// with different policies.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Externally-visible host (without scheme) for the listener. When
    /// `Some`, this beats any header-derived value.
    pub http_host_name: Option<String>,
    /// Listener's authenticator kind. When `None`, the handler refuses to
    /// publish a document — there is no OAuth flow on an unauthenticated
    /// listener to advertise.
    pub authenticator_kind: listeners::AuthenticatorKind,
}

/// HTTP handler for [`PROTECTED_RESOURCE_METADATA_PATH`].
///
/// Always public — no authentication is performed, by design. RFC 9728
/// places no auth requirements on this endpoint; clients must be able to
/// fetch it before they have a token.
///
/// Returns 404 when this listener does not validate tokens (`None`
/// authenticator) or has no issuer configured, 503 if the adapter client
/// is not yet available (a brief window at startup), 400 if the request
/// has no host information to construct a URL with, and 200 with the
/// JSON document otherwise.
pub async fn handle_protected_resource_metadata(
    Extension(adapter_client_rx): Extension<Delayed<Client>>,
    Extension(config): Extension<DiscoveryConfig>,
    req: Request,
) -> Response {
    // No-auth listener: there is no token to validate, nothing to
    // advertise. Refuse to publish.
    if matches!(config.authenticator_kind, listeners::AuthenticatorKind::None) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let Some(host) = resolve_host(&req, config.http_host_name.as_deref()) else {
        warn!("oauth-protected-resource: no http_host_name configured and request has no Host header");
        return (StatusCode::BAD_REQUEST, "no host available").into_response();
    };
    let scheme = scheme_for(&req);
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

    let mut response = axum::Json(metadata).into_response();
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static(METADATA_CACHE_CONTROL),
    );
    response
}

/// Builds the absolute URL of the protected resource metadata document
/// for use as the `resource_metadata` parameter in a `WWW-Authenticate`
/// challenge. Returns `None` if the request lacks enough host information
/// to construct a URL; the caller is expected to skip the Bearer challenge
/// in that case rather than emit a malformed value.
pub fn metadata_url(req: &Request, http_host_name: Option<&str>) -> Option<String> {
    let host = resolve_host(req, http_host_name)?;
    let scheme = scheme_for(req);
    Some(format!(
        "{scheme}://{host}{PROTECTED_RESOURCE_METADATA_PATH}"
    ))
}

/// Resolves the host string to embed in published absolute URLs.
///
/// Prefers the operator-configured `http_host_name`; falls back to the
/// request's `Host` header. **Never consults `X-Forwarded-*`** — see the
/// module-level "Host derivation" notes for the security rationale.
fn resolve_host(req: &Request, http_host_name: Option<&str>) -> Option<String> {
    if let Some(name) = http_host_name {
        let trimmed = name.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    req.headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Picks the scheme to use in published URLs.
///
/// OAuth 2.1 requires `https` for all endpoints, so we always advertise
/// `https` — the only exception is connections that arrived over plain
/// HTTP, which we infer from the request's URI scheme. This is *only*
/// reached in dev/test setups where the listener is on plain HTTP; in
/// production a TLS-terminating proxy fronts environmentd, but per the
/// host-derivation rules we do not let the proxy's `X-Forwarded-Proto`
/// header influence what we publish.
fn scheme_for(req: &Request) -> &'static str {
    match req.uri().scheme_str() {
        Some("http") => "http",
        _ => "https",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use http::Request;

    /// Builds a request with the given Host header and URI for use in
    /// unit tests.
    fn req_with_host(host: &str) -> Request<Body> {
        Request::builder()
            .uri("https://ignored.example.com/")
            .header(http::header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

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

    /// `http_host_name` beats the request's Host header — operators
    /// configure it to override what the proxy presents, so it must take
    /// precedence.
    #[mz_ore::test]
    fn test_resolve_host_prefers_http_host_name() {
        let req = req_with_host("internal.local:6876");
        assert_eq!(
            resolve_host(&req, Some("public.example.com")).as_deref(),
            Some("public.example.com"),
        );
    }

    /// When `http_host_name` is empty or whitespace, fall back to the
    /// request's Host header rather than emitting a blank string.
    #[mz_ore::test]
    fn test_resolve_host_ignores_blank_config() {
        let req = req_with_host("public.example.com");
        for blank in ["", "   ", "\t"] {
            assert_eq!(
                resolve_host(&req, Some(blank)).as_deref(),
                Some("public.example.com"),
                "blank http_host_name = {blank:?} should fall back to Host",
            );
        }
    }

    /// The Host header is the last fallback when no config is set.
    /// Includes port to make sure the helper is not stripping it.
    #[mz_ore::test]
    fn test_resolve_host_falls_back_to_host_header_with_port() {
        let req = req_with_host("example.com:8080");
        assert_eq!(
            resolve_host(&req, None).as_deref(),
            Some("example.com:8080"),
        );
    }

    /// `X-Forwarded-Host` MUST be ignored — see the module-level
    /// "Host derivation" notes. This is a security-relevant regression
    /// guard: if someone adds back X-Forwarded-Host trust, this test
    /// fails and they have to confront the trust decision explicitly.
    #[mz_ore::test]
    fn test_resolve_host_ignores_x_forwarded_host() {
        let req = Request::builder()
            .uri("https://ignored.example.com/")
            .header(http::header::HOST, "honest.example.com")
            .header("x-forwarded-host", "evil.example.com")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            resolve_host(&req, None).as_deref(),
            Some("honest.example.com"),
            "X-Forwarded-Host must not influence the resolved host",
        );
    }

    /// No host config and no Host header → `None`. Callers turn this
    /// into a 400 (handler) or a missing Bearer challenge (auth
    /// middleware); both are safer than guessing.
    #[mz_ore::test]
    fn test_resolve_host_returns_none_when_unavailable() {
        let req = Request::builder()
            .uri("https://ignored.example.com/")
            .body(Body::empty())
            .unwrap();
        assert_eq!(resolve_host(&req, None), None);
    }

    /// OAuth 2.1 mandates HTTPS for OAuth endpoints, so we always
    /// advertise `https` unless the request actually arrived on plain
    /// HTTP — and we determine that from the URI scheme on the request,
    /// not from `X-Forwarded-Proto`.
    #[mz_ore::test]
    fn test_scheme_for_defaults_to_https() {
        let req = Request::builder()
            .uri("/just/a/path")
            .body(Body::empty())
            .unwrap();
        assert_eq!(scheme_for(&req), "https");
    }

    #[mz_ore::test]
    fn test_scheme_for_honors_http_uri() {
        let req = Request::builder()
            .uri("http://example.com/foo")
            .body(Body::empty())
            .unwrap();
        assert_eq!(scheme_for(&req), "http");
    }

    /// `metadata_url` composes the resolved host with the canonical
    /// well-known suffix. Pinning the assembled value makes accidental
    /// path drift visible (e.g. extra slashes, dropped suffix).
    #[mz_ore::test]
    fn test_metadata_url_assembles_canonical_suffix() {
        let req = req_with_host("example.com");
        assert_eq!(
            metadata_url(&req, Some("public.example.com")).as_deref(),
            Some("https://public.example.com/.well-known/oauth-protected-resource"),
        );
    }
}
