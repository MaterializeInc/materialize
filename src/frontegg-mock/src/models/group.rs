// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMapping {
    #[serde(default)]
    pub id: String,
    pub group: String,
    #[serde(default, rename = "roleIds")]
    pub role_ids: Vec<String>,
    #[serde(default, rename = "ssoConfigId")]
    pub sso_config_id: String,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct GroupMappingResponse {
    pub id: String,
    pub group: String,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
    #[serde(rename = "ssoConfigId")]
    pub sso_config_id: String,
    pub enabled: bool,
}

#[derive(Deserialize)]
pub struct GroupMappingUpdateRequest {
    pub group: Option<String>,
    #[serde(rename = "roleIds")]
    pub role_ids: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DefaultRoles {
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Group {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub description: String,
    pub metadata: String,
    pub roles: Vec<Role>,
    pub users: Vec<User>,
    #[serde(rename = "managedBy")]
    pub managed_by: String,
    pub color: String,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Role {
    pub id: String,
    pub key: String,
    pub name: String,
    pub description: String,
    pub is_default: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct User {
    pub id: String,
    pub name: String,
    pub email: String,
}

#[derive(Deserialize)]
pub struct GroupCreateParams {
    pub name: String,
    pub description: Option<String>,
    pub color: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Deserialize)]
pub struct GroupUpdateParams {
    pub name: Option<String>,
    pub description: Option<String>,
    pub color: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Serialize)]
pub struct GroupsResponse {
    pub groups: Vec<Group>,
}

#[derive(Deserialize)]
pub struct AddUsersToGroupParams {
    #[serde(rename = "userIds")]
    pub user_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct RemoveUsersFromGroupParams {
    #[serde(rename = "userIds")]
    pub user_ids: Vec<String>,
}
