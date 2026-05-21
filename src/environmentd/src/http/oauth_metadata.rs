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

use std::time::Duration;

use axum::Extension;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::{HeaderValue, StatusCode};
use mz_adapter::Client;
use mz_adapter_types::dyncfgs::OIDC_ISSUER;
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_server_core::listeners;
use prometheus::IntCounterVec;
use serde::Serialize;
use tracing::warn;
use url::Url;

use crate::http::Delayed;

/// Upper bound on how long the discovery handler waits for the adapter
/// client to become available. The handler is unauthenticated, so a
/// wedged adapter must not be allowed to pin connections indefinitely.
/// Two seconds is generous for normal startup and tight enough that a
/// real outage surfaces as 503s instead of hung clients.
const ADAPTER_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// The well-known path served by this module.
///
/// Per [RFC 9728 §3] this is the OAuth 2.0 Protected Resource Metadata
/// well-known URI; MCP clients probe it as a fallback when no
/// `resource_metadata` parameter is present in a 401's `WWW-Authenticate`.
///
/// [RFC 9728 §3]: https://datatracker.ietf.org/doc/html/rfc9728#section-3
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// Path-suffixed aliases of [`PROTECTED_RESOURCE_METADATA_PATH`] per
/// RFC 9728 §3.1. A client that hits `/api/mcp/<endpoint>` and follows
/// the protected-resource discovery rules will look up
/// `/.well-known/oauth-protected-resource/<path>` first, falling back
/// to the bare well-known URI. We serve the same document at both: the
/// MCP endpoints share an identical metadata view today, so there is
/// nothing per-endpoint to vary. Registering the aliases keeps strict
/// clients working without forcing them to fall back.
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH_AGENT: &str =
    "/.well-known/oauth-protected-resource/api/mcp/agent";
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH_DEVELOPER: &str =
    "/.well-known/oauth-protected-resource/api/mcp/developer";

/// OAuth scope advertised for the MCP endpoints.
///
/// Today every authenticated caller can hit every MCP route — RBAC is
/// enforced at the SQL layer below, not on the HTTP scope. Advertising a
/// single coarse scope is honest: it tells clients "request a token that
/// names this scope and any valid IdP token will be accepted" without
/// promising scope-based authorization we do not enforce. Splitting into
/// per-route scopes (e.g. an extra `mcp.read.system` for the developer
/// endpoint) requires a separate scope-enforcement step in the auth
/// middleware and is deferred until that lands.
pub(crate) const MCP_SCOPE: &str = "mcp.read";

/// `Cache-Control` value set on the discovery response.
///
/// `private` keeps the document out of shared caches (CDNs, forward
/// proxies) which could otherwise serve a metadata document built for
/// one listener / virtual host to clients of another. End-user clients
/// can still cache it for an hour — long enough to amortise discovery
/// across a session, short enough that an admin who changes
/// `oidc_issuer` does not have to wait a long tail of stale caches out.
const METADATA_CACHE_CONTROL: &str = "private, max-age=3600";

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
pub(crate) struct ProtectedResourceMetadata {
    /// Canonical URL of this resource server. Clients use this as the
    /// `resource` parameter in their token request (RFC 8707).
    pub resource: String,
    /// Authorization servers a client may use to obtain a token. Each
    /// entry is an issuer URL whose metadata document is fetchable at
    /// `<issuer>/.well-known/oauth-authorization-server` (RFC 8414)
    /// or `<issuer>/.well-known/openid-configuration` (OIDC Discovery).
    pub authorization_servers: Vec<String>,
    /// Mechanisms by which the client may present the bearer token.
    /// We accept the `Authorization` header only.
    pub bearer_methods_supported: Vec<String>,
    /// Scopes a client may request a token for; see [`MCP_SCOPE`].
    pub scopes_supported: Vec<String>,
}

/// Prometheus counter for the discovery endpoint, labeled by outcome.
///
/// Cheaply `Clone`: the underlying Prometheus collector handle is
/// `Arc`-shared, so the struct is safe to clone into an axum
/// `Extension` per listener.
///
/// `status` label values are stable strings (not HTTP status numbers)
/// so dashboards do not have to know which integer maps to which
/// failure path:
///
///   * `ok` — 200 with a metadata document.
///   * `no_auth_listener` — 404 because the listener does not validate
///     tokens.
///   * `no_issuer` — 404 because `oidc_issuer` is unset.
///   * `invalid_issuer` — 503 because `oidc_issuer` is not a parseable
///     URL (operator misconfiguration).
///   * `no_host` — 400 because the request had no host information.
///   * `adapter_unavailable` — 503 during startup before the adapter
///     client is ready.
#[derive(Debug, Clone)]
pub struct OAuthMetadataMetrics {
    requests: IntCounterVec,
}

impl OAuthMetadataMetrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            requests: registry.register(metric!(
                name: "mz_oauth_protected_resource_metadata_requests_total",
                help: "Total number of requests to the OAuth Protected Resource Metadata endpoint.",
                var_labels: ["status"],
            )),
        }
    }

    fn inc(&self, status: &'static str) {
        self.requests.with_label_values(&[status]).inc();
    }
}

/// Per-listener inputs the discovery handler needs.
///
/// Carried as an axum `Extension` rather than reading from system config
/// directly because the values are per-listener (`authenticator_kind`,
/// `http_host_name`) — the same environmentd process can serve listeners
/// with different policies.
#[derive(Debug, Clone)]
pub(crate) struct DiscoveryConfig {
    /// Externally-visible host (without scheme). When `Some`, this beats
    /// any header-derived value.
    pub http_host_name: Option<String>,
    /// When `None`, the handler refuses to publish — there is no OAuth
    /// flow on an unauthenticated listener to advertise.
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
pub(crate) async fn handle_protected_resource_metadata(
    Extension(adapter_client_rx): Extension<Delayed<Client>>,
    Extension(config): Extension<DiscoveryConfig>,
    Extension(metrics): Extension<OAuthMetadataMetrics>,
    req: Request,
) -> Response {
    // No-auth listener: there is no token to validate, nothing to
    // advertise. Refuse to publish.
    if matches!(
        config.authenticator_kind,
        listeners::AuthenticatorKind::None
    ) {
        metrics.inc("no_auth_listener");
        return StatusCode::NOT_FOUND.into_response();
    }

    let Some(host) = resolve_host(&req, config.http_host_name.as_deref()) else {
        warn!(
            "oauth-protected-resource: no http_host_name configured and request has no Host header"
        );
        metrics.inc("no_host");
        return (StatusCode::BAD_REQUEST, "no host available").into_response();
    };
    let scheme = scheme_for(&req);
    let resource = format!("{scheme}://{host}/api/mcp");

    // The endpoint is unauthenticated, so a hung adapter must not be
    // allowed to pin connections indefinitely. Bound the wait and
    // surface a 503 instead.
    let adapter_client =
        match tokio::time::timeout(ADAPTER_WAIT_TIMEOUT, adapter_client_rx.clone()).await {
            Ok(Ok(client)) => client,
            Ok(Err(_)) | Err(_) => {
                metrics.inc("adapter_unavailable");
                return (StatusCode::SERVICE_UNAVAILABLE, "adapter not ready").into_response();
            }
        };
    let system_vars = adapter_client.get_system_vars().await;
    let Some(issuer) = OIDC_ISSUER.get(system_vars.dyncfgs()) else {
        // No OAuth authorization server is configured. Per RFC 9728 the
        // document MUST contain at least one entry in
        // `authorization_servers`, so the honest response is 404 rather
        // than an empty document that misleads the client.
        metrics.inc("no_issuer");
        return StatusCode::NOT_FOUND.into_response();
    };
    let issuer = match validate_issuer_url(&issuer) {
        Ok(validated) => validated.to_string(),
        Err(err) => {
            warn!(%issuer, error = %err, "oauth-protected-resource: refusing to publish invalid oidc_issuer");
            metrics.inc("invalid_issuer");
            return (StatusCode::SERVICE_UNAVAILABLE, err).into_response();
        }
    };

    let metadata = ProtectedResourceMetadata {
        resource,
        authorization_servers: vec![issuer],
        bearer_methods_supported: vec!["header".to_string()],
        scopes_supported: vec![MCP_SCOPE.to_string()],
    };

    let mut response = axum::Json(metadata).into_response();
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static(METADATA_CACHE_CONTROL),
    );
    metrics.inc("ok");
    response
}

/// Validates the operator-supplied `oidc_issuer` before it is published.
///
/// RFC 9728 §3 requires entries in `authorization_servers` to be valid
/// URLs; RFC 8414 §2 further requires that the issuer identifier
/// contain no query or fragment components, since the issuer string
/// must match the `iss` claim in tokens byte-for-byte.
///
/// We require:
///
///   * Parseable as a URL.
///   * Scheme of `https` (or `http` for dev) — OAuth 2.1 forbids
///     other schemes for an authorization server.
///   * No userinfo component — operators occasionally embed
///     credentials in URLs by mistake; this endpoint is public and
///     must not leak them.
///   * No query or fragment — required by RFC 8414 §2.
///
/// On success returns the **original** issuer string unchanged. We do
/// not return `url.to_string()` because [`Url`] normalises some forms
/// (notably appending a trailing slash to a bare authority) and a
/// silently mutated issuer would no longer match the `iss` claim in
/// tokens minted by the IdP.
fn validate_issuer_url(issuer: &str) -> Result<&str, &'static str> {
    let url = Url::parse(issuer).map_err(|_| "oidc_issuer is not a parseable URL")?;
    if !matches!(url.scheme(), "https" | "http") {
        return Err("oidc_issuer must use the https or http scheme");
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("oidc_issuer must not contain userinfo");
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("oidc_issuer must not contain a query or fragment");
    }
    Ok(issuer)
}

/// Builds the absolute URL of the protected resource metadata document
/// for use as the `resource_metadata` parameter in a `WWW-Authenticate`
/// challenge. Returns `None` if the request lacks enough host information
/// to construct a URL; the caller is expected to skip the Bearer challenge
/// in that case rather than emit a malformed value.
pub(crate) fn metadata_url(req: &Request, http_host_name: Option<&str>) -> Option<String> {
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
/// module-level "Host derivation" notes.
///
/// The returned value is parsed through [`http::uri::Authority`] before
/// it is returned, so it is guaranteed to be a syntactically valid
/// `host[:port]` per RFC 3986. This is the second layer of defense
/// against header-smuggling attacks: even if a future change accepts a
/// malicious value as input, the parser rejects anything containing
/// characters outside the URI host grammar (notably `"`, whitespace,
/// `;`, etc.) so the value cannot break out of the quoted
/// `resource_metadata="..."` parameter in a `WWW-Authenticate` challenge
/// or smuggle additional fields into the published `resource` URL.
fn resolve_host(req: &Request, http_host_name: Option<&str>) -> Option<String> {
    let candidate = http_host_name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            req.headers()
                .get(http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })?;
    // Round-trip through `http::uri::Authority` to confirm the value is
    // a syntactically valid `host[:port]`. This rejects header-smuggling
    // payloads (quotes, whitespace, control chars, parameter-delimiters)
    // that `HeaderValue::from_str` lets through.
    let authority = candidate.parse::<http::uri::Authority>().ok()?;
    if authority.as_str() != candidate {
        // The Authority parser is permissive about some forms (e.g.
        // `userinfo@host`), so additionally require the parsed form to
        // round-trip exactly. Anything that re-renders differently is
        // refused.
        return None;
    }
    Some(candidate)
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
            scopes_supported: vec!["mcp.read".to_string()],
        };
        let json = serde_json::to_value(&metadata).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "resource": "https://mcp.example.com/api/mcp",
                "authorization_servers": ["https://auth.example.com"],
                "bearer_methods_supported": ["header"],
                "scopes_supported": ["mcp.read"],
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

    /// Defense in depth against header-smuggling: a Host header that
    /// contains characters outside the URI host grammar (quotes,
    /// whitespace, semicolons, etc.) MUST be rejected. Otherwise a
    /// malicious value could break out of the quoted
    /// `resource_metadata="..."` parameter in `WWW-Authenticate` and
    /// inject extra challenges, or smuggle a different host into the
    /// published `resource` URL.
    ///
    /// The payloads here are restricted to bytes that `HeaderValue::from_str`
    /// accepts (`HeaderValue` already blocks CR/LF/NUL and other control
    /// bytes); the point of the URI-grammar parse in `resolve_host` is
    /// to catch the rest — quotes, whitespace, semicolons, etc.
    #[mz_ore::test]
    fn test_resolve_host_rejects_smuggled_characters() {
        for malicious in [
            // The headline regression: closes the quoted
            // resource_metadata parameter and injects a second one.
            "attacker.example.net\" foo=bar",
            // Whitespace splits the `host:port` token.
            "host with space.example.com",
            // Semicolons inside a quoted parameter still terminate
            // `auth-param` values in some lenient parsers.
            "host\";evil=1",
            // Backslash is reserved in URI host grammar.
            "host\\backslash",
            // Quote alone is enough to terminate the parameter.
            "host\"quote",
        ] {
            let req = req_with_host(malicious);
            assert_eq!(
                resolve_host(&req, None),
                None,
                "smuggling payload via Host header must be rejected: {malicious:?}",
            );
            assert_eq!(
                resolve_host(&req, Some(malicious)),
                None,
                "smuggling payload via http_host_name must be rejected: {malicious:?}",
            );
        }
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

    /// The path-suffixed aliases are part of the public contract with
    /// strict RFC 9728 §3.1 clients. Pin them so a typo here surfaces
    /// as a test failure rather than a silent 404 in the field.
    #[mz_ore::test]
    fn test_path_suffixed_alias_paths_are_correct() {
        assert_eq!(
            PROTECTED_RESOURCE_METADATA_PATH_AGENT,
            "/.well-known/oauth-protected-resource/api/mcp/agent",
        );
        assert_eq!(
            PROTECTED_RESOURCE_METADATA_PATH_DEVELOPER,
            "/.well-known/oauth-protected-resource/api/mcp/developer",
        );
    }

    /// The scope value is wire-visible (clients ask the IdP for tokens
    /// with this exact string), so a rename is breaking. Pin it.
    #[mz_ore::test]
    fn test_mcp_scope_constant() {
        assert_eq!(MCP_SCOPE, "mcp.read");
    }

    /// Mirrors the `McpMetrics` pattern: counter families only appear
    /// in `gather()` after a label combination is observed, so each
    /// status value is touched once before gathering.
    #[mz_ore::test]
    fn test_metric_registers() {
        let registry = MetricsRegistry::new();
        let metrics = OAuthMetadataMetrics::register_into(&registry);
        for status in [
            "ok",
            "no_auth_listener",
            "no_issuer",
            "invalid_issuer",
            "no_host",
            "adapter_unavailable",
        ] {
            metrics.inc(status);
        }
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|m| m.name().to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "mz_oauth_protected_resource_metadata_requests_total"),
            "metric should be registered, got: {names:?}",
        );
    }

    /// IPv6 hosts are valid `Authority` syntax (`[::1]:8080`) and must
    /// round-trip cleanly. A regression here silently breaks IPv6-only
    /// deployments.
    #[mz_ore::test]
    fn test_resolve_host_accepts_ipv6_literal() {
        for host in ["[::1]", "[::1]:8080", "[2001:db8::1]:443"] {
            let req = req_with_host(host);
            assert_eq!(
                resolve_host(&req, None).as_deref(),
                Some(host),
                "IPv6 literal must round-trip through Authority: {host:?}",
            );
        }
    }

    /// `validate_issuer_url` accepts well-formed issuer URLs and
    /// returns the **original** string unchanged. Normalisation would
    /// break the byte-for-byte match against the `iss` token claim.
    #[mz_ore::test]
    fn test_validate_issuer_url_accepts_well_formed() {
        for issuer in [
            "https://issuer.example.com",
            "https://issuer.example.com/realms/main",
            "http://localhost:8080",
        ] {
            assert_eq!(
                validate_issuer_url(issuer),
                Ok(issuer),
                "expected {issuer:?} to pass validation",
            );
        }
    }

    /// Rejects values that would surface as a confusing client error
    /// downstream or that would leak embedded secrets in a public
    /// document.
    #[mz_ore::test]
    fn test_validate_issuer_url_rejects_invalid() {
        // Format: (issuer, expected substring of the error reason).
        let cases: &[(&str, &str)] = &[
            ("not a url", "parseable"),
            ("issuer.example.com", "parseable"),
            ("ftp://issuer.example.com", "scheme"),
            ("https://user:pass@issuer.example.com", "userinfo"),
            ("https://user@issuer.example.com", "userinfo"),
            ("https://issuer.example.com?foo=bar", "query"),
            ("https://issuer.example.com#frag", "query"),
        ];
        for (issuer, expected_substr) in cases {
            let err = validate_issuer_url(issuer)
                .expect_err(&format!("expected {issuer:?} to be rejected as invalid",));
            assert!(
                err.contains(expected_substr),
                "for {issuer:?}, expected reason to contain {expected_substr:?}, got {err:?}",
            );
        }
    }
}
