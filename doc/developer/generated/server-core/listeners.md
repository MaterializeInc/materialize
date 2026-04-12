---
source: src/server-core/src/listeners.rs
revision: b1e123e786
---

# mz-server-core::listeners

Defines the configuration types used to describe named network listeners for both SQL and HTTP protocols.

`ListenersConfig` holds two `BTreeMap`s mapping listener names to `SqlListenerConfig` and `HttpListenerConfig` respectively.
`BaseListenerConfig` captures the common properties — bind address, `AuthenticatorKind`, `AllowedRoles`, and TLS flag — while `HttpListenerConfig` adds `HttpRoutesEnabled` to control which HTTP route groups are active (base, webhook, internal, metrics, profiling, `mcp_agent`, `mcp_developer`, and `console_config`).
`AuthenticatorKind` enumerates the supported authentication modes: `Frontegg`, `Password`, `Sasl`, `Oidc`, and `None`.
`AllowedRoles` controls whether normal users, internal users, or both may connect.
The `ListenerConfig` trait provides a uniform accessor interface over both listener variants and includes a `validate` method (HTTP rejects SASL authentication, SQL accepts any combination).
