// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use uuid::Uuid;

/// Metadata about a user in an external system.
#[derive(Debug, Clone, Serialize)]
pub struct ExternalUserMetadata {
    /// The ID of the user in the external system.
    pub user_id: Uuid,
    /// Indicates if the user is an admin in the external system.
    pub admin: bool,
}
