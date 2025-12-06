// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata about a user in an external system.
#[derive(Debug, Clone, Serialize)]
pub struct ExternalUserMetadata {
    /// The ID of the user in the external system.
    pub user_id: Uuid,
    /// Indicates if the user is an admin in the external system.
    pub admin: bool,
}

/// Represents changes to external user metadata properties.
#[derive(Debug, Clone, Default)]
pub struct ExternalUserMetadataDiff {
    pub user_id: Option<Uuid>,
    pub admin: Option<bool>,
}

impl ExternalUserMetadataDiff {
    pub fn new(previous: Option<&ExternalUserMetadata>, new: &ExternalUserMetadata) -> Self {
        let previous_user_id = previous.map(|m| m.user_id);
        let previous_admin = previous.map(|m| m.admin);
        ExternalUserMetadataDiff {
            user_id: (previous_user_id != Some(new.user_id)).then_some(new.user_id),
            admin: (previous_admin != Some(new.admin)).then_some(new.admin),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalUserMetadata {
    pub superuser: bool,
}
