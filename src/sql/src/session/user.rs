// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use mz_repr::role_id::RoleId;
use mz_repr::user::ExternalUserMetadata;
use serde::Serialize;

pub const SYSTEM_USER_NAME: &str = "mz_system";
pub static SYSTEM_USER: LazyLock<User> = LazyLock::new(|| User {
    name: SYSTEM_USER_NAME.into(),
    external_metadata: None,
});

pub const SUPPORT_USER_NAME: &str = "mz_support";
pub static SUPPORT_USER: LazyLock<User> = LazyLock::new(|| User {
    name: SUPPORT_USER_NAME.into(),
    external_metadata: None,
});

pub const ANALYTICS_USER_NAME: &str = "mz_analytics";
pub static ANALYTICS_USER: LazyLock<User> = LazyLock::new(|| User {
    name: ANALYTICS_USER_NAME.into(),
    external_metadata: None,
});

pub static INTERNAL_USER_NAMES: LazyLock<BTreeSet<String>> = LazyLock::new(|| {
    [&SYSTEM_USER, &SUPPORT_USER, &ANALYTICS_USER]
        .into_iter()
        .map(|user| user.name.clone())
        .collect()
});

pub static INTERNAL_USER_NAME_TO_DEFAULT_CLUSTER: LazyLock<BTreeMap<String, String>> =
    LazyLock::new(|| {
        [
            (&SYSTEM_USER, "mz_system"),
            (&SUPPORT_USER, "mz_catalog_server"),
            (&ANALYTICS_USER, "mz_analytics"),
        ]
        .into_iter()
        .map(|(user, cluster)| (user.name.clone(), cluster.to_string()))
        .collect()
    });

pub static HTTP_DEFAULT_USER: LazyLock<User> = LazyLock::new(|| User {
    name: "anonymous_http_user".into(),
    external_metadata: None,
});

/// Identifies a user.
#[derive(Debug, Clone, Serialize)]
pub struct User {
    /// The name of the user within the system.
    pub name: String,
    /// Metadata about this user in an external system.
    pub external_metadata: Option<ExternalUserMetadata>,
}

impl From<&User> for mz_pgwire_common::UserMetadata {
    fn from(user: &User) -> mz_pgwire_common::UserMetadata {
        mz_pgwire_common::UserMetadata {
            is_admin: user.is_external_admin(),
            should_limit_connections: user.limit_max_connections(),
        }
    }
}

impl PartialEq for User {
    fn eq(&self, other: &User) -> bool {
        self.name == other.name
    }
}

impl User {
    /// Returns whether this is an internal user.
    pub fn is_internal(&self) -> bool {
        INTERNAL_USER_NAMES.contains(&self.name)
    }

    /// Returns whether this user is an admin in an external system.
    pub fn is_external_admin(&self) -> bool {
        self.external_metadata
            .as_ref()
            .map(|metadata| metadata.admin)
            .clone()
            .unwrap_or(false)
    }

    /// Returns whether this user is a superuser.
    pub fn is_superuser(&self) -> bool {
        matches!(self.kind(), UserKind::Superuser)
    }

    /// Returns whether this is user is the `mz_system` user.
    pub fn is_system_user(&self) -> bool {
        self == &*SYSTEM_USER
    }

    /// Returns whether we should limit this user's connections to max_connections
    pub fn limit_max_connections(&self) -> bool {
        !self.is_internal()
    }

    /// Returns the kind of user this is.
    pub fn kind(&self) -> UserKind {
        if self.is_external_admin() || self.is_system_user() {
            UserKind::Superuser
        } else {
            UserKind::Regular
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum UserKind {
    Regular,
    Superuser,
}

pub const MZ_SYSTEM_ROLE_ID: RoleId = RoleId::System(1);
pub const MZ_SUPPORT_ROLE_ID: RoleId = RoleId::System(2);
pub const MZ_ANALYTICS_ROLE_ID: RoleId = RoleId::System(3);
pub const MZ_MONITOR_ROLE_ID: RoleId = RoleId::Predefined(1);
pub const MZ_MONITOR_REDACTED_ROLE_ID: RoleId = RoleId::Predefined(2);

/// Metadata about a Session's role.
///
/// Modeled after PostgreSQL role hierarchy:
/// <https://github.com/postgres/postgres/blob/9089287aa037fdecb5a52cec1926e5ae9569e9f9/src/backend/utils/init/miscinit.c#L461-L493>
#[derive(Debug, Clone)]
pub struct RoleMetadata {
    /// The role that initiated the database context. Fixed for the duration of the connection.
    pub authenticated_role: RoleId,
    /// Initially the same as `authenticated_role`, but can be changed by SET SESSION AUTHORIZATION
    /// (not yet implemented). Used to determine what roles can be used for SET ROLE
    /// (not yet implemented).
    pub session_role: RoleId,
    /// The role of the current execution context. This role is used for all normal privilege
    /// checks.
    pub current_role: RoleId,
}

impl RoleMetadata {
    /// Returns a RoleMetadata with all fields set to `id`.
    pub fn new(id: RoleId) -> RoleMetadata {
        RoleMetadata {
            authenticated_role: id,
            session_role: id,
            current_role: id,
        }
    }
}
