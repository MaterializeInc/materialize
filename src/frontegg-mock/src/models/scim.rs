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

#[derive(Deserialize)]
pub struct SCIM2ConfigurationCreateRequest {
    pub source: String,
    #[serde(rename = "connectionName")]
    pub connection_name: String,
    #[serde(rename = "syncToUserManagement")]
    pub sync_to_user_management: bool,
}

#[derive(Serialize, Clone, Debug)]
pub struct SCIM2ConfigurationResponse {
    pub id: String,
    pub source: String,
    #[serde(rename = "tenantId")]
    pub tenant_id: String,
    #[serde(rename = "connectionName")]
    pub connection_name: String,
    #[serde(rename = "syncToUserManagement")]
    pub sync_to_user_management: bool,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    pub token: String,
}

#[derive(Clone, Debug)]
pub struct SCIM2ConfigurationStorage {
    pub id: String,
    pub source: String,
    pub tenant_id: String,
    pub connection_name: String,
    pub sync_to_user_management: bool,
    pub created_at: DateTime<Utc>,
    pub token: String,
}
