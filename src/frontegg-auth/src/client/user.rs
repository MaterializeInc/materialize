// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::{client::CREATE_USERS_PATH, error::Error, parse::Paginated};

use super::{Client, USERS_PATH};

#[derive(Serialize)]
pub struct UserCreationRequest {
    email: String,
    name: String,
    provider: String,
    #[serde(rename = "roleIds")]
    role_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct User {
    id: String,
    name: String,
    email: String,
    sub: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Role {
    id: String,
    vendor_id: String,
    tenant_id: Option<String>,
    key: String,
    name: String,
    description: String,
    is_default: bool,
    first_user_role: bool,
    created_at: String,
    updated_at: String,
    permissions: Vec<String>,
    level: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserCreationResponse {
    id: String,
    email: String,
    name: String,
    profile_picture_url: String,
    verified: Option<bool>,
    metadata: Option<String>,
    roles: Vec<Role>,
}

impl Client {
    // Lists all existing users.
    pub async fn list_users(&self) -> Result<Vec<User>, Error> {
        let mut users = vec![];
        let mut page = 0;

        loop {
            let req = self.build_request(Method::GET, USERS_PATH);
            let req = req.query(&[("_limit", "50"), ("_offset", &*page.to_string())]);
            let res: Paginated<User> = self.send_request(req).await?;
            for user in res.items {
                users.push(user);
            }
            page += 1;
            if page >= res.metadata.total_pages {
                break;
            }
        }
        Ok(users)
    }

    /// Creates a new user in the authenticated organization.
    pub async fn create_user(
        &self,
        new_user: UserCreationRequest,
    ) -> Result<UserCreationResponse, Error> {
        let req = self.build_request(Method::POST, CREATE_USERS_PATH);

        let req = req.json(&new_user);

        let user_created: UserCreationResponse = self.send_request(req).await?;
        Ok(user_created)
    }
}
