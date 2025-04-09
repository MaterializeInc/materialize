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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AuthenticatorKind {
    Frontegg,
    Password,
    None,
}

/// Whether to allow internal users (ie: mz_system) and/or normal users.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
pub enum AllowedRoles {
    Normal,
    Internal,
    NormalAndInternal,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct HttpRoutesEnabled {
    pub base: bool,
    pub webhook: bool,
    pub internal: bool,
    pub metrics: bool,
    pub profiling: bool,
}

/// Configuration for network listeners.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ListenersConfig {
    pub sql: BTreeMap<String, SqlListenerConfig>,
    pub http: BTreeMap<String, HttpListenerConfig>,
}

/// Base configuration used by both SQL and HTTP listeners.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct BaseListenerConfig {
    /// The IP address and port to listen for connections on.
    pub addr: SocketAddr,
    pub authenticator_kind: AuthenticatorKind,
    pub allowed_roles: AllowedRoles,
    pub enable_tls: bool,
}
pub type SqlListenerConfig = BaseListenerConfig;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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
}
