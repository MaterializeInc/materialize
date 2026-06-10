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
//! listener: either because `oidc_issuer` is unset (no authorization
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
use mz_adapter_types::dyncfgs::{OIDC_AUDIENCE, OIDC_ISSUER};
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_server_core::listeners;
use prometheus::IntCounterVec;
use serde::Serialize;
use tracing::warn;
use url::Url;

use crate::http::Delayed;

/// Bounds how long the unauthenticated discovery handler waits for the
/// adapter client. Without this, a wedged adapter could pin connections
/// indefinitely from any caller on the network.
const ADAPTER_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// OAuth 2.1 §3.1 requires `https` for all OAuth endpoints.
const PUBLISHED_SCHEME: &str = "https";

/// The well-known path served by this module.
///
/// Per [RFC 9728 §3] this is the OAuth 2.0 Protected Resource Metadata
/// well-known URI; MCP clients probe it as a fallback when no
/// `resource_metadata` parameter is present in a 401's `WWW-Authenticate`.
///
/// [RFC 9728 §3]: https://datatracker.ietf.org/doc/html/rfc9728#section-3
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH: &str = "/.well-known/oauth-protected-resource";

/// Path-suffixed aliases of [`PROTECTED_RESOURCE_METADATA_PATH`] per
/// RFC 9728 §3.1. Both MCP endpoints serve an identical metadata
/// document today, so the same handler is mounted at all three paths;
/// the aliases exist so strict clients that always probe with a path
/// suffix do not have to fall back to the bare URI.
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH_AGENT: &str =
    "/.well-known/oauth-protected-resource/api/mcp/agent";
pub(crate) const PROTECTED_RESOURCE_METADATA_PATH_DEVELOPER: &str =
    "/.well-known/oauth-protected-resource/api/mcp/developer";

/// OAuth scope advertised for the MCP endpoints. Not enforced
/// server-side (authorization is at the SQL layer via RBAC).
pub(crate) const MCP_SCOPE: &str = "mcp.read";

/// `private` (not the RFC-default `public`): the document varies by host,
/// so shared caches must not serve one listener's document to another.
const METADATA_CACHE_CONTROL: &str = "private, max-age=3600";

/// JSON shape returned by [`PROTECTED_RESOURCE_METADATA_PATH`]. A
/// strict subset of [RFC 9728 §2]; further fields can be added without
/// breaking clients per the RFC's extensibility guidance.
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

/// Closed set of outcomes recorded by [`OauthMetadataMetrics`]. Keeping
/// these as an enum (rather than free-form strings at the call sites) pins
/// the metric's label cardinality and stops typos from silently creating
/// new label values.
#[derive(Debug, Clone, Copy)]
enum MetricStatus {
    Ok,
    Disabled,
    NoIssuer,
    InvalidIssuer,
    NoHost,
    AdapterUnavailable,
}

impl MetricStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Disabled => "disabled",
            Self::NoIssuer => "no_issuer",
            Self::InvalidIssuer => "invalid_issuer",
            Self::NoHost => "no_host",
            Self::AdapterUnavailable => "adapter_unavailable",
        }
    }
}

/// Prometheus counter for the discovery endpoint, labeled by the
/// [`MetricStatus`] of each request so dashboards key off names rather
/// than HTTP status codes.
#[derive(Debug, Clone)]
pub struct OauthMetadataMetrics {
    requests: IntCounterVec,
}

impl OauthMetadataMetrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            requests: registry.register(metric!(
                name: "mz_oauth_protected_resource_metadata_requests_total",
                help: "Total number of requests to the OAuth Protected Resource Metadata endpoint.",
                var_labels: ["status"],
            )),
        }
    }

    fn inc(&self, status: MetricStatus) {
        self.requests.with_label_values(&[status.as_str()]).inc();
    }
}

/// Whether and how an HTTP listener advertises OAuth 2.0 for its MCP
/// routes. Derived once per listener from its authenticator and consulted
/// by both the 401 `WWW-Authenticate` challenge and this discovery handler,
/// so the two never disagree about whether OAuth is on offer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpOAuthDiscovery {
    /// The listener validates no OAuth bearer token, so there is no flow to
    /// advertise. Covers `None`, `Password`, and `Sasl`.
    Disabled,
    /// Self-managed OIDC: the authorization server is the `oidc_issuer`
    /// dyncfg, read at request time so an operator can change it without a
    /// restart.
    Oidc,
    // Frontegg (cloud) validates tokens against its own pre-shared key
    // rather than `oidc_issuer`, and its authorization server is the
    // configured Frontegg URL. Advertising it needs a `Frontegg { issuer }`
    // arm here plus edge plumbing; tracked in DEX-31.
}

impl McpOAuthDiscovery {
    /// The single mapping from a listener's authenticator to its OAuth
    /// advertisement behavior. Only `Oidc` validates bearer tokens against
    /// `oidc_issuer`, so only `Oidc` advertises today.
    pub(crate) fn for_authenticator(kind: listeners::AuthenticatorKind) -> Self {
        match kind {
            listeners::AuthenticatorKind::Oidc => Self::Oidc,
            listeners::AuthenticatorKind::Frontegg
            | listeners::AuthenticatorKind::Password
            | listeners::AuthenticatorKind::Sasl
            | listeners::AuthenticatorKind::None => Self::Disabled,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

/// Per-listener config for the discovery handler. Carried as an axum
/// `Extension` because one environmentd process can serve listeners with
/// different authenticators and `http_host_name`s.
#[derive(Debug, Clone)]
pub(crate) struct DiscoveryConfig {
    /// Operator-configured external host (without scheme). Beats the
    /// `Host` header when set.
    pub http_host_name: Option<String>,
    /// How (or whether) this listener advertises OAuth.
    pub discovery: McpOAuthDiscovery,
}

/// HTTP handler for [`PROTECTED_RESOURCE_METADATA_PATH`].
///
/// Always public: no authentication is performed, by design. RFC 9728
/// places no auth requirements on this endpoint; clients must be able to
/// fetch it before they have a token.
///
/// Returns 404 when this listener does not advertise OAuth (see
/// [`McpOAuthDiscovery`]) or has no issuer configured, 503 if the adapter
/// client is not yet available (a brief window at startup), 400 if the
/// request has no host information to construct a URL with, and 200 with
/// the JSON document otherwise.
pub(crate) async fn handle_protected_resource_metadata(
    Extension(adapter_client_rx): Extension<Delayed<Client>>,
    Extension(config): Extension<DiscoveryConfig>,
    Extension(metrics): Extension<OauthMetadataMetrics>,
    req: Request,
) -> Response {
    if !config.discovery.is_enabled() {
        // Listener validates no OAuth bearer token (None/Password/Sasl), so
        // there is no authorization flow to advertise.
        metrics.inc(MetricStatus::Disabled);
        return StatusCode::NOT_FOUND.into_response();
    }

    let Some(host) = resolve_host(&req, config.http_host_name.as_deref()) else {
        warn!(
            "oauth-protected-resource: no http_host_name configured and request has no Host header"
        );
        metrics.inc(MetricStatus::NoHost);
        return (StatusCode::BAD_REQUEST, "no host available").into_response();
    };
    let resource = format!("{PUBLISHED_SCHEME}://{host}/api/mcp");

    let adapter_client =
        match tokio::time::timeout(ADAPTER_WAIT_TIMEOUT, adapter_client_rx.clone()).await {
            Ok(Ok(client)) => client,
            Ok(Err(_)) | Err(_) => {
                metrics.inc(MetricStatus::AdapterUnavailable);
                return (StatusCode::SERVICE_UNAVAILABLE, "adapter not ready").into_response();
            }
        };
    let system_vars = adapter_client.get_system_vars().await;
    let Some(issuer) = OIDC_ISSUER.get(system_vars.dyncfgs()) else {
        // No OAuth authorization server is configured. Per RFC 9728 the
        // document MUST contain at least one entry in
        // `authorization_servers`, so the honest response is 404 rather
        // than an empty document that misleads the client.
        warn!("oauth-protected-resource: oidc_issuer is unset; cannot publish");
        metrics.inc(MetricStatus::NoIssuer);
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Err(err) = validate_issuer_url(&issuer) {
        // Don't echo the issuer into the WARN: a userinfo-bearing value
        // would turn this log line into a credential dump. The error
        // reason is specific enough.
        warn!(error = %err, "oauth-protected-resource: refusing to publish invalid oidc_issuer");
        metrics.inc(MetricStatus::InvalidIssuer);
        return (StatusCode::SERVICE_UNAVAILABLE, err).into_response();
    }

    // We still publish when no audience is configured (mirroring the
    // authenticator, which warns and skips `aud` validation), but a resource
    // server that does not bind tokens to its own audience is exposed to
    // same-issuer token reuse, so make the gap visible to operators.
    if OIDC_AUDIENCE
        .get(system_vars.dyncfgs())
        .as_array()
        .is_none_or(|audiences| audiences.is_empty())
    {
        warn!(
            "oauth-protected-resource: publishing with oidc_audience unset; tokens from this \
             issuer are not audience-bound to this resource"
        );
    }

    let metadata = ProtectedResourceMetadata {
        resource,
        authorization_servers: vec![issuer.to_string()],
        bearer_methods_supported: vec!["header".to_string()],
        scopes_supported: vec![MCP_SCOPE.to_string()],
    };

    let mut response = axum::Json(metadata).into_response();
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static(METADATA_CACHE_CONTROL),
    );
    metrics.inc(MetricStatus::Ok);
    response
}

/// Validates `oidc_issuer` before it is published. Required: parses as
/// a URL, scheme is `https` or `http`, no userinfo (we publish it on a
/// public endpoint), no query or fragment (RFC 8414 §2). The `http`
/// scheme is permitted to ease local dev; OAuth 2.1 §3.1 forbids it in
/// production but enforcement is the operator's responsibility.
///
/// The caller publishes the **original** value (not a re-serialised
/// `Url`) because `url::Url` silently normalises some forms (e.g. adds
/// a trailing slash to a bare authority), and a mutated issuer would
/// not match the `iss` claim in tokens minted by the IdP.
fn validate_issuer_url(issuer: &str) -> Result<(), &'static str> {
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
    Ok(())
}

/// Builds the absolute URL of the protected resource metadata document
/// for use as the `resource_metadata` parameter in a `WWW-Authenticate`
/// challenge. Returns `None` if the request lacks enough host information
/// to construct a URL; the caller is expected to skip the Bearer challenge
/// in that case rather than emit a malformed value.
pub(crate) fn metadata_url(req: &Request, http_host_name: Option<&str>) -> Option<String> {
    let host = resolve_host(req, http_host_name)?;
    Some(format!(
        "{PUBLISHED_SCHEME}://{host}{PROTECTED_RESOURCE_METADATA_PATH}"
    ))
}

/// Resolves the host string to embed in published absolute URLs.
///
/// Prefers the operator-configured `http_host_name`; falls back to the
/// request's `Host` header. **Never consults `X-Forwarded-*`**. See the
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
    // Reject `user@host` explicitly. `Authority::as_str()` keeps userinfo,
    // so the round-trip below would pass it through, letting an attacker
    // poison the published URLs via a forged `Host: bobby@evil...`.
    if candidate.contains('@') {
        return None;
    }
    // Round-trip through `http::uri::Authority` to confirm the value is
    // a syntactically valid `host[:port]`. This rejects header-smuggling
    // payloads (quotes, whitespace, control chars, parameter-delimiters)
    // that `HeaderValue::from_str` lets through.
    let authority = candidate.parse::<http::uri::Authority>().ok()?;
    if authority.as_str() != candidate {
        return None;
    }
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use http::Request;

    fn req_with_host(host: &str) -> Request<Body> {
        Request::builder()
            .header(http::header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

    /// Pin the serialized field names; clients key off them.
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

    /// The well-known path is part of the public contract with clients.
    #[mz_ore::test]
    fn test_well_known_path_is_rfc9728_canonical() {
        assert_eq!(
            PROTECTED_RESOURCE_METADATA_PATH,
            "/.well-known/oauth-protected-resource",
        );
    }

    /// `http_host_name` beats the request's Host header.
    #[mz_ore::test]
    fn test_resolve_host_prefers_http_host_name() {
        let req = req_with_host("internal.local:6876");
        assert_eq!(
            resolve_host(&req, Some("public.example.com")).as_deref(),
            Some("public.example.com"),
        );
    }

    /// Empty or whitespace-only `http_host_name` falls back to `Host`.
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

    /// Host header is the final fallback; port must be preserved.
    #[mz_ore::test]
    fn test_resolve_host_falls_back_to_host_header_with_port() {
        let req = req_with_host("example.com:8080");
        assert_eq!(
            resolve_host(&req, None).as_deref(),
            Some("example.com:8080"),
        );
    }

    /// Security regression guard: `X-Forwarded-Host` is never trusted
    /// (see module-level "Host derivation" notes).
    #[mz_ore::test]
    fn test_resolve_host_ignores_x_forwarded_host() {
        let req = Request::builder()
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

    /// No host config and no `Host` header → `None`.
    #[mz_ore::test]
    fn test_resolve_host_returns_none_when_unavailable() {
        let req = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(resolve_host(&req, None), None);
    }

    /// Defense in depth against Host-header smuggling: per-payload
    /// comments below describe the specific failure each one would cause.
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

    /// `http::uri::Authority` accepts a `userinfo@host` form, but the
    /// round-trip check in `resolve_host` must reject it: if we accepted
    /// it, an attacker could send `Host: bobby@evil.example.com` and
    /// poison the published `resource` URL with their own prefix.
    #[mz_ore::test]
    fn test_resolve_host_rejects_userinfo() {
        for malicious in [
            "user@host.example.com",
            "user:pass@host.example.com",
            "@host.example.com",
            "user@host.example.com:8080",
            "user:pass@host.example.com:8080",
        ] {
            let req = req_with_host(malicious);
            assert_eq!(
                resolve_host(&req, None),
                None,
                "userinfo in Host header must be rejected: {malicious:?}",
            );
            assert_eq!(
                resolve_host(&req, Some(malicious)),
                None,
                "userinfo in http_host_name must be rejected: {malicious:?}",
            );
        }
    }

    /// Pin the assembled URL: accidental path drift (extra slashes,
    /// dropped suffix) breaks every connected client.
    #[mz_ore::test]
    fn test_metadata_url_assembles_canonical_suffix() {
        let req = req_with_host("example.com");
        assert_eq!(
            metadata_url(&req, Some("public.example.com")).as_deref(),
            Some("https://public.example.com/.well-known/oauth-protected-resource"),
        );
    }

    /// Pin the path-suffixed aliases; strict RFC 9728 §3.1 clients
    /// probe these before the bare URI.
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

    /// Pin the wire-visible scope string; clients ask the IdP for a
    /// token with this exact value.
    #[mz_ore::test]
    fn test_mcp_scope_constant() {
        assert_eq!(MCP_SCOPE, "mcp.read");
    }

    /// Counter families only appear in `gather()` after a label
    /// combination is observed, so each status value is touched first.
    #[mz_ore::test]
    fn test_metric_registers() {
        let registry = MetricsRegistry::new();
        let metrics = OauthMetadataMetrics::register_into(&registry);
        for status in [
            MetricStatus::Ok,
            MetricStatus::Disabled,
            MetricStatus::NoIssuer,
            MetricStatus::InvalidIssuer,
            MetricStatus::NoHost,
            MetricStatus::AdapterUnavailable,
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

    /// IPv6 literals (`[::1]:8080`) are valid `Authority` syntax and
    /// must round-trip. A regression silently breaks IPv6 deployments.
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

    /// Well-formed issuers pass through unchanged; normalisation
    /// would break byte-for-byte match against the `iss` token claim.
    #[mz_ore::test]
    fn test_validate_issuer_url_accepts_well_formed() {
        for issuer in [
            "https://issuer.example.com",
            "https://issuer.example.com/realms/main",
            "http://localhost:8080",
        ] {
            assert_eq!(
                validate_issuer_url(issuer),
                Ok(()),
                "expected {issuer:?} to pass validation",
            );
        }
    }

    /// Pin the authenticator-to-discovery mapping. Today only `Oidc`
    /// advertises OAuth; if Frontegg, Password, Sasl, or None flips to
    /// enabled by accident, MCP clients would be steered into a flow
    /// the listener can't honour.
    #[mz_ore::test]
    fn test_mcp_oauth_discovery_for_authenticator() {
        use listeners::AuthenticatorKind;
        assert_eq!(
            McpOAuthDiscovery::for_authenticator(AuthenticatorKind::Oidc),
            McpOAuthDiscovery::Oidc,
        );
        for kind in [
            AuthenticatorKind::None,
            AuthenticatorKind::Password,
            AuthenticatorKind::Frontegg,
            AuthenticatorKind::Sasl,
        ] {
            assert_eq!(
                McpOAuthDiscovery::for_authenticator(kind),
                McpOAuthDiscovery::Disabled,
                "{kind:?} must not advertise OAuth until explicitly wired up",
            );
        }
        assert!(McpOAuthDiscovery::Oidc.is_enabled());
        assert!(!McpOAuthDiscovery::Disabled.is_enabled());
    }

    /// Rejects values that would confuse clients downstream or leak
    /// embedded secrets in a public document.
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
