---
source: src/server-core/src/listeners.rs
revision: 59b1f165b2
---

# mz-server-core::listeners

Defines the configuration types used to describe named network listeners for both SQL and HTTP protocols.

`VersionedListenersConfig` is a serde-tagged enum with two variants, `V1` (`v0_147_0::ListenersConfig`, tagged `"0.147.0"`) and `V2` (`v26_32_0::ListenersConfig`, tagged `"26.32.0"`). `orchestratord` serializes the variant matching each `environmentd` binary's version so that older binaries receive the legacy single-`allowed_roles`-per-listener schema and newer binaries receive the per-route-group schema.
`RouteGroup` is an enum with `Enabled(AllowedRoles)` and `Disabled` variants. It serializes as `{ "enabled": false }` when disabled and `{ "enabled": true, "allowed_roles": "..." }` when enabled. `HttpRoutesEnabled` holds one `RouteGroup` per HTTP route group (`base`, `webhook`, `internal`, `metrics`, `profiling`, `mcp_agent`, `mcp_developer`, `console_config`), each carrying its own role policy.
`HttpListenerConfig` carries `addr`, `authenticator_kind`, `enable_tls`, and `routes: HttpRoutesEnabled`. Role policy lives per route group in `routes`, not at the listener level.
`BaseListenerConfig` captures the common properties — bind address, `AuthenticatorKind`, `AllowedRoles`, and TLS flag — and is aliased as `SqlListenerConfig` for SQL listeners.
`AuthenticatorKind` enumerates the supported authentication modes: `Frontegg`, `Password`, `Sasl`, `Oidc`, and `None`.
`AllowedRoles` controls whether normal users, internal users, or both may connect.
The `ListenerConfig` trait provides a uniform accessor interface over both listener variants and includes a `validate` method (HTTP rejects SASL authentication, SQL accepts any combination).
