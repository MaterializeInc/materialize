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

const ROLES_PATH: [&str; 5] = ["frontegg", "identity", "resources", "roles", "v2"];

/// `Role` represents the Frontegg role structure.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Role {
    /// A unique identifier for the role.
    pub id: String,
    /// The identifier of the vendor associated with the role.
    pub vendor_id: String,
    /// The optional identifier of the tenant associated with the role.
    pub tenant_id: Option<String>,
    /// A unique key associated with the role.
    pub key: String,
    /// The name of the role.
    pub name: String,
    /// A description of the role.
    pub description: String,
    /// A boolean indicating whether this role is a default one.
    pub is_default: bool,
    /// A boolean indicating whether this role was the first one assigned to a user.
    pub first_user_role: bool,
    /// A string representing the date and time in ISO 8601 when the role was created.
    ///
    /// E.g.: 2023-04-26T08:37:03.000Z
    pub created_at: String,
    /// A string representing the date and time in ISO 8601 when the role was last updated.
    ///
    /// E.g.: 2023-04-26T08:37:03.000Z
    pub updated_at: String,
    /// A vector of strings representing the permissions associated with the role.
    pub permissions: Vec<String>,
    /// An integer representing the level of the role.
    pub level: i32,
}

impl Client {
    /// Lists all available roles.
    pub async fn list_roles(&self) -> Result<Vec<Role>, Error> {
        let mut roles = vec![];
        let mut page = 0;

        loop {
            let req = self.build_request(Method::GET, ROLES_PATH);
            let req = req.query(&[("_limit", "50"), ("_offset", &*page.to_string())]);
            let res: Paginated<Role> = self.send_request(req).await?;
            for role in res.items {
                roles.push(role);
            }
            page += 1;
            if page >= res.metadata.total_pages {
                break;
            }
        }
        Ok(roles)
    }
}
