// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Deserialize,
    Serialize,
    PartialEq,
    JsonSchema
)]
pub enum AuthenticatorKind {
    /// Authenticate users using Frontegg.
    Frontegg,
    /// Authenticate users using internally stored password hashes.
    /// The backend secret must contain external_login_password_mz_system.
    Password,
    /// Authenticate users using SASL.
    Sasl,
    /// Authenticate users using OIDC (JWT tokens).
    Oidc,
    /// Authenticate users using Ory Talos app passwords (derived-JWT exchange).
    Talos,
    /// Do not authenticate users. Trust they are who they say they are without verification.
    #[default]
    None,
}

/// Whether to allow internal users (ie: mz_system) and/or normal users.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AllowedRoles {
    /// Allow normal (ie: customer) users, but not internal (ie: mz_support/mz_system) users.
    Normal,
    /// Allow internal (ie: mz_support/mz_system) users, but not normal (ie: customer) users.
    Internal,
    /// Allow both normal and internal users.
    NormalAndInternal,
}

/// A group of HTTP routes
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SerializedRouteGroup", into = "SerializedRouteGroup")]
pub enum RouteGroup {
    /// Enabled, restricted to these roles.
    Enabled(AllowedRoles),
    /// Disabled.
    Disabled,
}

impl RouteGroup {
    /// Whether the routes in this group are enabled.
    pub fn is_enabled(&self) -> bool {
        matches!(self, RouteGroup::Enabled(_))
    }

    /// The roles allowed to use this group, or `None` if it is disabled.
    pub fn allowed_roles(&self) -> Option<AllowedRoles> {
        match self {
            RouteGroup::Enabled(allowed_roles) => Some(*allowed_roles),
            RouteGroup::Disabled => None,
        }
    }
}

/// The JSON serialization of a route group.
/// Serializes as `{ "enabled": false }` when disabled and
/// `{ "enabled": true, "allowed_roles": "..." }` when enabled.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SerializedRouteGroup {
    enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allowed_roles: Option<AllowedRoles>,
}

impl From<RouteGroup> for SerializedRouteGroup {
    fn from(group: RouteGroup) -> Self {
        match group {
            RouteGroup::Enabled(allowed_roles) => SerializedRouteGroup {
                enabled: true,
                allowed_roles: Some(allowed_roles),
            },
            RouteGroup::Disabled => SerializedRouteGroup {
                enabled: false,
                allowed_roles: None,
            },
        }
    }
}

impl TryFrom<SerializedRouteGroup> for RouteGroup {
    type Error = String;

    fn try_from(serialized: SerializedRouteGroup) -> Result<Self, Self::Error> {
        match (serialized.enabled, serialized.allowed_roles) {
            (true, Some(allowed_roles)) => Ok(RouteGroup::Enabled(allowed_roles)),
            (true, None) => Err("an enabled route group requires `allowed_roles`".to_string()),
            // A disabled group has no roles; any `allowed_roles` is ignored.
            (false, _) => Ok(RouteGroup::Disabled),
        }
    }
}

/// The set of HTTP route groups, each carrying its own `allowed_roles` policy.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HttpRoutesEnabled {
    /// Include the primary customer-facing endpoints, including the SQL APIs and static files.
    pub base: RouteGroup,
    /// Include the /api/webhook/** endpoints.
    pub webhook: RouteGroup,
    /// Include internal endpoints including promotion, catalog, coordinator, and internal-console
    /// APIs.
    pub internal: RouteGroup,
    /// Include metrics and liveness/readiness probe endpoints.
    pub metrics: RouteGroup,
    /// Include /prof/ endpoint, and enable profiling in the / endpoint (included in base).
    pub profiling: RouteGroup,
    /// Include /api/mcp/agent endpoint for Model Context Protocol (AI agents).
    pub mcp_agent: RouteGroup,
    /// Include /api/mcp/developer endpoint for system catalog queries via MCP.
    pub mcp_developer: RouteGroup,
    /// Include /api/console/config endpoint for unauthenticated console configuration.
    pub console_config: RouteGroup,
}

/// A listeners config tagged by its schema version.
///
/// `orchestratord` serializes the variant matching each `environmentd`'s
/// version. serde writes the `version` tag into the JSON from each variant's
/// `rename`, keeping the version string and the schema type it carries from
/// drifting apart.
///
/// NOTE: `environmentd` does not deserialize this wrapper. It parses the
/// concrete `v26_32_0::ListenersConfig` and ignores the `version` tag, so a
/// schema it does not understand surfaces as a structural parse error, not as a
/// variant mismatch here. The `orchestratord` version gate is what guarantees
/// each `environmentd` is served a schema its binary can parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum VersionedListenersConfig {
    /// The legacy schema: a single `allowed_roles` per HTTP listener, with route
    /// groups toggled by bools.
    #[serde(rename = "0.147.0")]
    V1(v0_147_0::ListenersConfig),
    /// The current schema: `allowed_roles` carried per route group.
    #[serde(rename = "26.32.0")]
    V2(v26_32_0::ListenersConfig),
}

/// Base configuration used by both SQL and HTTP listeners.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BaseListenerConfig {
    /// The IP address and port to listen for connections on.
    pub addr: SocketAddr,
    pub authenticator_kind: AuthenticatorKind,
    pub allowed_roles: AllowedRoles,
    pub enable_tls: bool,
}
pub type SqlListenerConfig = BaseListenerConfig;

/// Runtime HTTP listener config. Unlike SQL listeners, the role policy lives per
/// route group (in `routes`), not at the listener level.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpListenerConfig {
    pub addr: SocketAddr,
    pub authenticator_kind: AuthenticatorKind,
    pub enable_tls: bool,
    pub routes: HttpRoutesEnabled,
}

pub trait ListenerConfig {
    fn addr(&self) -> SocketAddr;
    fn authenticator_kind(&self) -> AuthenticatorKind;
    fn enable_tls(&self) -> bool;
    fn validate(&self) -> Result<(), String>;
}
impl ListenerConfig for SqlListenerConfig {
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn authenticator_kind(&self) -> AuthenticatorKind {
        self.authenticator_kind
    }

    fn enable_tls(&self) -> bool {
        self.enable_tls
    }

    fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}
impl ListenerConfig for HttpListenerConfig {
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn authenticator_kind(&self) -> AuthenticatorKind {
        self.authenticator_kind
    }

    fn enable_tls(&self) -> bool {
        self.enable_tls
    }

    fn validate(&self) -> Result<(), String> {
        match self.authenticator_kind {
            AuthenticatorKind::Sasl => {
                Err("SASL authentication is not supported for HTTP listeners".to_string())
            }
            // Talos derive-based auth is wired for pgwire only so far; the HTTP
            // path is not yet supported.
            AuthenticatorKind::Talos => {
                Err("Talos authentication is not yet supported for HTTP listeners".to_string())
            }
            _ => Ok(()),
        }
    }
}

/// The current listener config schema (v26.32.0): `allowed_roles` per route
/// group.
pub mod v26_32_0 {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::{HttpListenerConfig, SqlListenerConfig};

    /// Configuration for network listeners.
    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct ListenersConfig {
        pub sql: BTreeMap<String, SqlListenerConfig>,
        pub http: BTreeMap<String, HttpListenerConfig>,
    }
}

/// The HTTP listener config schema introduced in v0.147.0.
pub mod v0_147_0 {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::{BaseListenerConfig, SqlListenerConfig};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ListenersConfig {
        pub sql: BTreeMap<String, SqlListenerConfig>,
        pub http: BTreeMap<String, HttpListenerConfig>,
    }

    /// The top-level `allowed_roles` (in `base`) applies to every route group;
    /// it is expanded per group during migration.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HttpListenerConfig {
        #[serde(flatten)]
        pub base: BaseListenerConfig,
        pub routes: HttpRoutes,
    }

    /// A bool per route group; all groups share the listener's `allowed_roles`.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    pub struct HttpRoutes {
        pub base: bool,
        pub webhook: bool,
        pub internal: bool,
        pub metrics: bool,
        pub profiling: bool,
        #[serde(default)]
        pub mcp_agent: bool,
        #[serde(default)]
        pub mcp_developer: bool,
        #[serde(default)]
        pub console_config: bool,
    }
}

impl From<v0_147_0::ListenersConfig> for v26_32_0::ListenersConfig {
    fn from(legacy: v0_147_0::ListenersConfig) -> Self {
        let http = legacy
            .http
            .into_iter()
            .map(|(name, listener)| (name, listener.into()))
            .collect();
        v26_32_0::ListenersConfig {
            sql: legacy.sql,
            http,
        }
    }
}

impl From<v0_147_0::HttpListenerConfig> for HttpListenerConfig {
    fn from(legacy: v0_147_0::HttpListenerConfig) -> Self {
        // Migration: every enabled route group inherits the listener's single
        // top-level `allowed_roles`.
        let roles = legacy.base.allowed_roles;
        let group = |enabled| {
            if enabled {
                RouteGroup::Enabled(roles)
            } else {
                RouteGroup::Disabled
            }
        };
        HttpListenerConfig {
            addr: legacy.base.addr,
            authenticator_kind: legacy.base.authenticator_kind,
            enable_tls: legacy.base.enable_tls,
            routes: HttpRoutesEnabled {
                base: group(legacy.routes.base),
                webhook: group(legacy.routes.webhook),
                internal: group(legacy.routes.internal),
                metrics: group(legacy.routes.metrics),
                profiling: group(legacy.routes.profiling),
                mcp_agent: group(legacy.routes.mcp_agent),
                mcp_developer: group(legacy.routes.mcp_developer),
                console_config: group(legacy.routes.console_config),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn parse(json: &str) -> v26_32_0::ListenersConfig {
        serde_json::from_reader(json.as_bytes()).expect("valid listeners config")
    }

    #[mz_ore::test]
    fn legacy_migrates_to_per_group_inheriting_listener_role() {
        // orchestratord builds the legacy `v0_147_0` shape to serve older
        // environmentd and migrates it in-memory; every route group inherits the
        // listener's single top-level role.
        let legacy = v0_147_0::ListenersConfig {
            sql: BTreeMap::new(),
            http: BTreeMap::from([(
                "external".to_string(),
                v0_147_0::HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: "0.0.0.0:6876".parse().expect("addr"),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: v0_147_0::HttpRoutes {
                        base: true,
                        webhook: false,
                        internal: true,
                        metrics: false,
                        profiling: false,
                        mcp_agent: false,
                        mcp_developer: false,
                        console_config: false,
                    },
                },
            )]),
        };
        let migrated: v26_32_0::ListenersConfig = legacy.into();
        let routes = migrated.http["external"].routes;
        assert_eq!(
            routes.base,
            RouteGroup::Enabled(AllowedRoles::NormalAndInternal)
        );
        assert_eq!(
            routes.internal,
            RouteGroup::Enabled(AllowedRoles::NormalAndInternal)
        );
        assert_eq!(routes.webhook, RouteGroup::Disabled);
    }

    #[mz_ore::test]
    fn per_group_schema_parses() {
        // Enabled groups carry their roles; disabled groups omit `allowed_roles`.
        let json = r#"{
            "version": "26.32.0",
            "sql": {},
            "http": {
                "external": {
                    "addr": "0.0.0.0:6876",
                    "authenticator_kind": "None",
                    "enable_tls": false,
                    "routes": {
                        "base": { "enabled": true, "allowed_roles": "Normal" },
                        "webhook": { "enabled": false },
                        "internal": { "enabled": true, "allowed_roles": "Internal" },
                        "metrics": { "enabled": false },
                        "profiling": { "enabled": false },
                        "mcp_agent": { "enabled": false },
                        "mcp_developer": { "enabled": false },
                        "console_config": { "enabled": false }
                    }
                }
            }
        }"#;
        let config = parse(json);
        let routes = config.http["external"].routes;
        assert_eq!(routes.base, RouteGroup::Enabled(AllowedRoles::Normal));
        assert_eq!(routes.internal, RouteGroup::Enabled(AllowedRoles::Internal));
        assert_eq!(routes.webhook, RouteGroup::Disabled);
    }

    #[mz_ore::test]
    fn route_group_serializes_compactly() {
        // Disabled omits `allowed_roles`; enabled includes it.
        assert_eq!(
            serde_json::to_string(&RouteGroup::Disabled).expect("serializes"),
            r#"{"enabled":false}"#
        );
        assert_eq!(
            serde_json::to_string(&RouteGroup::Enabled(AllowedRoles::Internal))
                .expect("serializes"),
            r#"{"enabled":true,"allowed_roles":"Internal"}"#
        );
    }

    #[mz_ore::test]
    fn enabled_route_group_requires_allowed_roles() {
        serde_json::from_str::<RouteGroup>(r#"{ "enabled": true }"#)
            .expect_err("enabled group without allowed_roles must fail");
    }

    #[mz_ore::test]
    fn v26_32_0_schema_round_trip_serialization() {
        let json = r#"{
            "version": "26.32.0",
            "sql": {},
            "http": {
                "external": {
                    "addr": "0.0.0.0:6876",
                    "authenticator_kind": "None",
                    "enable_tls": false,
                    "routes": {
                        "base": { "enabled": true, "allowed_roles": "Normal" },
                        "webhook": { "enabled": false },
                        "internal": { "enabled": true, "allowed_roles": "Internal" },
                        "metrics": { "enabled": false },
                        "profiling": { "enabled": false },
                        "mcp_agent": { "enabled": false },
                        "mcp_developer": { "enabled": false },
                        "console_config": { "enabled": false }
                    }
                }
            }
        }"#;
        let config = parse(json);
        let serialized =
            serde_json::to_string(&VersionedListenersConfig::V2(config)).expect("serializes");
        assert!(
            serialized.contains(r#""version":"26.32.0""#),
            "missing version tag: {serialized}"
        );

        let VersionedListenersConfig::V2(reparsed) =
            serde_json::from_str(&serialized).expect("re-parses")
        else {
            panic!("the `version` tag selected the wrong variant");
        };
        // base and internal survive the serialization roundtrip
        assert_eq!(
            reparsed.http["external"].routes.base,
            RouteGroup::Enabled(AllowedRoles::Normal)
        );
        assert_eq!(
            reparsed.http["external"].routes.internal,
            RouteGroup::Enabled(AllowedRoles::Internal)
        );
    }

    #[mz_ore::test]
    fn v0_147_0_schema_round_trip_serialization() {
        // The legacy variant nests a `#[serde(flatten)]` (the `base` field in
        // `v0_147_0::HttpListenerConfig`) inside the internally-tagged enum.
        // Guard that this round-trips and that serializing stamps the legacy tag.
        let legacy = v0_147_0::ListenersConfig {
            sql: BTreeMap::new(),
            http: BTreeMap::from([(
                "external".to_string(),
                v0_147_0::HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: "0.0.0.0:6876".parse().expect("addr"),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: v0_147_0::HttpRoutes {
                        base: true,
                        webhook: false,
                        internal: true,
                        metrics: false,
                        profiling: false,
                        mcp_agent: false,
                        mcp_developer: false,
                        console_config: false,
                    },
                },
            )]),
        };
        let serialized =
            serde_json::to_string(&VersionedListenersConfig::V1(legacy)).expect("serializes");
        assert!(
            serialized.contains(r#""version":"0.147.0""#),
            "missing legacy version tag: {serialized}"
        );

        let VersionedListenersConfig::V1(reparsed) =
            serde_json::from_str(&serialized).expect("re-parses")
        else {
            panic!("the `version` tag selected the wrong variant");
        };
        // The flattened `base` fields survive the round trip.
        assert_eq!(
            reparsed.http["external"].base.allowed_roles,
            AllowedRoles::NormalAndInternal
        );
        assert!(reparsed.http["external"].routes.internal);
    }
}
