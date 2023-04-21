// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the client's functions for interacting with the Frontegg users API.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::{error::ErrorExtended, parse::Paginated};

use super::Client;

const USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v3"];
const CREATE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v2"];

/// Representation of all the mandatory fields for a user creation request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserCreationRequest {
    email: String,
    name: String,
    provider: String,
    role_ids: Vec<String>,
}

/// A structure that represents a user in Frontegg.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
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

/// Representation of a succesfully response from a user creation.
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
    /// Lists all existing users.
    pub async fn list_users(&self) -> Result<Vec<User>, ErrorExtended> {
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
    ) -> Result<UserCreationResponse, ErrorExtended> {
        let req = self.build_request(Method::POST, CREATE_USERS_PATH);

        let req = req.json(&new_user);

        let user_created: UserCreationResponse = self.send_request(req).await?;
        Ok(user_created)
    }
}
