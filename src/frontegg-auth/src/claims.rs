// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use uuid::Uuid;

// TODO: Do we care about the sub? Do we need to validate the sub or other
// things, even if unused?
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claims {
    pub exp: i64,
    pub email: String,
    pub sub: Uuid,
    pub user_id: Option<Uuid>,
    pub tenant_id: Uuid,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
}

impl Claims {
    /// Extracts the most specific user ID present in the token.
    pub fn best_user_id(&self) -> Uuid {
        self.user_id.unwrap_or(self.sub)
    }

    /// Returns true if the claims belong to a frontegg admin.
    pub fn admin(&self, admin_name: &str) -> bool {
        self.roles.iter().any(|role| role == admin_name)
    }
}
