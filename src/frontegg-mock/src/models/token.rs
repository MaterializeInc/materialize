// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use mz_frontegg_auth::ClaimMetadata;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct AuthUserRequest {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

// We only use this for the tenant ID, so ignoring all other fields,
// even required ones.
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserProfileResponse {
    pub tenant_id: Uuid,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct ApiToken {
    #[serde(alias = "client_id")]
    pub client_id: Uuid,
    pub secret: Uuid,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(alias = "created_at", skip_deserializing)]
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UserApiTokenResponse {
    #[serde(rename = "clientId")]
    pub client_id: String,
    pub description: String,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    pub secret: String,
}

#[derive(Deserialize)]
pub struct UserApiTokenRequest {
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateTenantApiTokenRequest {
    pub description: String,
    pub metadata: Option<ClaimMetadata>,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantApiTokenConfig {
    pub tenant_id: Uuid,
    pub metadata: Option<ClaimMetadata>,
    pub roles: Vec<String>,
    pub description: Option<String>,
    pub created_by_user_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantApiTokenResponse {
    #[serde(rename = "clientId")]
    pub client_id: Uuid,
    pub description: String,
    pub secret: Uuid,
    #[serde(rename = "createdByUserId")]
    pub created_by_user_id: Uuid,
    pub metadata: Option<ClaimMetadata>,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}
