---
source: src/environmentd/src/http/oauth_metadata.rs
revision: 4061850066
---

# environmentd::http::oauth_metadata

Implements RFC 9728 OAuth 2.0 Protected Resource Metadata for the MCP endpoints.
Serves `/.well-known/oauth-protected-resource` (and path-suffixed aliases `/.well-known/oauth-protected-resource/api/mcp/agent` and `/.well-known/oauth-protected-resource/api/mcp/developer`) as a public, unauthenticated endpoint so MCP-aware clients can discover the authorization server before acquiring a token.

The `ProtectedResourceMetadata` struct serializes to the RFC 9728 JSON document shape with fields `resource`, `authorization_servers`, `bearer_methods_supported`, and `scopes_supported`. The MCP scope advertised is `mcp.read`.

`McpOAuthDiscovery` is a per-listener enum with three variants: `Disabled` (no OAuth flow to advertise), `Oidc` (authorization server URL read from the `oidc_issuer` dyncfg at request time), and `Frontegg { issuer }` (authorization server URL supplied at startup via `--frontegg-oauth-issuer-url`). `McpOAuthDiscovery::for_authenticator` maps from a `listeners::AuthenticatorKind` to the appropriate variant; `Frontegg` requires a valid issuer URL or falls back to `Disabled`.

`McpOAuthConfig` bundles the per-listener `http_host_name` and `discovery` into an axum `Extension` shared between the 401 `WWW-Authenticate` challenge builder and `handle_protected_resource_metadata`, keeping the two in lockstep.

The handler returns 404 when the listener has `McpOAuthDiscovery::Disabled` or when `oidc_issuer` is unset, 503 when the adapter is not yet available (with a 2-second timeout), 400 when no host can be resolved, and 200 with the JSON document otherwise.

Host resolution (`resolve_host`) prefers the operator-configured `http_host_name` over the request `Host` header. `X-Forwarded-Host` is never consulted. The resolved host is validated through `http::uri::Authority` to reject smuggling payloads (quotes, whitespace, userinfo, control characters).

`OauthMetadataMetrics` is a Prometheus counter labeled by `MetricStatus` (`ok`, `disabled`, `no_issuer`, `invalid_issuer`, `no_host`, `adapter_unavailable`).

`metadata_url` builds the absolute `resource_metadata` URL for embedding in `WWW-Authenticate` 401 challenges.
