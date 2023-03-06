// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use once_cell::sync::Lazy;
use uuid::Uuid;

pub static SYSTEM_USER: Lazy<User> = Lazy::new(|| User {
    name: "mz_system".into(),
    external_metadata: None,
});

pub static INTROSPECTION_USER: Lazy<User> = Lazy::new(|| User {
    name: "mz_introspection".into(),
    external_metadata: None,
});

pub static INTERNAL_USER_NAMES: Lazy<BTreeSet<String>> = Lazy::new(|| {
    [&SYSTEM_USER, &INTROSPECTION_USER]
        .into_iter()
        .map(|user| user.name.clone())
        .collect()
});

pub static HTTP_DEFAULT_USER: Lazy<User> = Lazy::new(|| User {
    name: "anonymous_http_user".into(),
    external_metadata: None,
});

/// Identifies a user.
#[derive(Debug, Clone)]
pub struct User {
    /// The name of the user within the system.
    pub name: String,
    /// Metadata about this user in an external system.
    pub external_metadata: Option<ExternalUserMetadata>,
}

/// Metadata about a [`User`] in an external system.
#[derive(Debug, Clone)]
pub struct ExternalUserMetadata {
    /// The ID of the user in the external system.
    pub user_id: Uuid,
    /// The ID of the user's active group in the external system.
    pub group_id: Uuid,
    /// Indicates if the user is an admin in the external system.
    pub admin: bool,
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
        self.is_external_admin() || self.name == SYSTEM_USER.name
    }

    /// Returns whether this is user is the `mz_system` user.
    pub fn is_system_user(&self) -> bool {
        self == &*SYSTEM_USER
    }
}
