// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the client's functions for interacting with the
//! Frontegg users API.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::client::Client;
use crate::error::Error;
use crate::parse::Paginated;

const USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v3"];
const CREATE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v2"];

/// Representation of all the mandatory fields for a user creation request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)] // TODO: add docs to all fields
pub struct CreateUserRequest {
    pub email: String,
    pub name: String,
    pub provider: String,
    pub role_ids: Vec<String>,
}

/// A structure that represents a user in Frontegg.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)] // TODO: add docs to all fields
pub struct User {
    pub id: String,
    pub name: String,
    pub email: String,
    pub sub: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)] // TODO: add docs to all fields
pub struct Role {
    pub id: String,
    pub vendor_id: String,
    pub tenant_id: Option<String>,
    pub key: String,
    pub name: String,
    pub description: String,
    pub is_default: bool,
    pub first_user_role: bool,
    pub created_at: String,
    pub updated_at: String,
    pub permissions: Vec<String>,
    pub level: i32,
}

/// Representation of a succesfully response from a user creation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)] // TODO: add docs to all fields
pub struct CreatedUser {
    /// The ID of the user.
    pub id: String,
    /// The email for the user.
    pub email: String,
    /// The name of the user.
    pub name: String,
    pub profile_picture_url: String,
    pub verified: Option<bool>,
    pub metadata: Option<String>,
    /// The roles to which this user belongs.
    pub roles: Vec<Role>,
}

impl Client {
    /// Lists all existing users.
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
    pub async fn create_user(&self, new_user: CreateUserRequest) -> Result<CreatedUser, Error> {
        let req = self.build_request(Method::POST, CREATE_USERS_PATH);
        let req = req.json(&new_user);
        let created_user = self.send_request(req).await?;
        Ok(created_user)
    }
}
