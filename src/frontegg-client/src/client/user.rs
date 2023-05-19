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

use crate::client::role::Role;
use crate::client::Client;
use crate::error::Error;
use crate::parse::{Empty, Paginated};

const USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v3"];
const CREATE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v2"];
const REMOVE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v1"];

/// Representation of all the mandatory fields for a user creation request.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUserRequest {
    /// Email for the user
    pub email: String,
    /// Name for the user
    pub name: String,
    /// Provider for the user.
    /// E.g.: `local`
    pub provider: String,
    /// Roles the user will have in the organization.
    pub role_ids: Vec<uuid::Uuid>,
}

/// Representation of the only required field to request a user removal.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveUserRequest {
    /// The identifier of the user to remove. Equals to the `id` inside the [User] struct.
    pub user_id: String,
}

/// A structure that represents a user in Frontegg.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    /// The ID of the user.
    pub id: String,
    /// The name of the user.
    pub name: String,
    /// The email for the user.
    pub email: String,
    /// Unique identifier for the subject; Currently it is the user ID
    pub sub: String,
}

/// Representation of a succesfully response from a user creation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedUser {
    /// The ID of the user.
    pub id: String,
    /// The email for the user.
    pub email: String,
    /// The name of the user.
    pub name: String,
    /// The profile picture URL of the user.
    pub profile_picture_url: String,
    /// Indicates if the user verified their email.
    pub verified: Option<bool>,
    /// Metadata about the user; it is usually empty.
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

    /// Removes a user from the authenticated organization.
    pub async fn remove_user(&self, remove_user: RemoveUserRequest) -> Result<(), Error> {
        let mut user_path = REMOVE_USERS_PATH.to_vec();
        user_path.push(remove_user.user_id.as_str());

        let req = self.build_request(Method::DELETE, user_path);

        let req = req.json(&remove_user);
        self.send_request::<Empty>(req).await?;

        Ok(())
    }
}
