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

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, PartialEq, JsonSchema)]
pub enum AuthenticatorKind {
    /// Authenticate users using Frontegg.
    Frontegg,
    /// Authenticate users using internally stored password hashes.
    Password,
    /// Authenticate users using SASL.
    Sasl,
    /// Do not authenticate users. Trust they are who they say they are without verification.
    #[default]
    None,
}

impl AuthenticatorKind {
    /// Whether this authenticator kind supports password-style self-managed authentication.
    pub fn password_style_self_managed_auth(&self) -> bool {
        matches!(self, AuthenticatorKind::Password | AuthenticatorKind::Sasl)
    }
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct HttpRoutesEnabled {
    /// Include the primary customer-facing endpoints, including the SQL APIs and static files.
    pub base: bool,
    /// Include the /api/webhook/** endpoints.
    pub webhook: bool,
    /// Include internal endpoints including promotion, catalog, coordinator, and internal-console
    /// APIs.
    pub internal: bool,
    /// Include metrics and liveness/readiness probe endpoints.
    pub metrics: bool,
    /// Include /prof/ endpoint, and enable profiling in the / endpoint (included in base).
    pub profiling: bool,
}

/// Configuration for network listeners.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ListenersConfig {
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpListenerConfig {
    #[serde(flatten)]
    pub base: BaseListenerConfig,
    pub routes: HttpRoutesEnabled,
}

pub trait ListenerConfig {
    fn addr(&self) -> SocketAddr;
    fn authenticator_kind(&self) -> AuthenticatorKind;
    fn allowed_roles(&self) -> AllowedRoles;
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

    fn allowed_roles(&self) -> AllowedRoles {
        self.allowed_roles
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
        self.base.addr
    }

    fn authenticator_kind(&self) -> AuthenticatorKind {
        self.base.authenticator_kind
    }

    fn allowed_roles(&self) -> AllowedRoles {
        self.base.allowed_roles
    }

    fn enable_tls(&self) -> bool {
        self.base.enable_tls
    }

    fn validate(&self) -> Result<(), String> {
        if self.base.authenticator_kind == AuthenticatorKind::Sasl {
            Err("SASL authentication is not supported for HTTP listeners".to_string())
        } else {
            Ok(())
        }
    }
}
