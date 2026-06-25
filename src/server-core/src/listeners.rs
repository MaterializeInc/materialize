// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const LISTENERS_CONFIG_VERSION_0_147_0: &str = "0.147.0";
const LISTENERS_CONFIG_VERSION_26_32_0: &str = "26.32.0";

/// The current listener config schema version.
pub const LISTENERS_CONFIG_VERSION: &str = LISTENERS_CONFIG_VERSION_26_32_0;

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

/// Configuration for network listeners.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ListenersConfig {
    /// Schema version of the config.
    pub version: String,
    pub sql: BTreeMap<String, SqlListenerConfig>,
    pub http: BTreeMap<String, HttpListenerConfig>,
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
        if self.authenticator_kind == AuthenticatorKind::Sasl {
            Err("SASL authentication is not supported for HTTP listeners".to_string())
        } else {
            Ok(())
        }
    }
}
