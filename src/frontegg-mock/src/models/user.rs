// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::models::token::ApiToken;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Clone, Serialize)]
pub struct UserConfig {
    pub id: Uuid,
    pub email: String,
    pub password: String,
    pub tenant_id: Uuid,
    pub initial_api_tokens: Vec<ApiToken>,
    pub roles: Vec<String>,
    pub auth_provider: Option<String>,
    pub verified: Option<bool>,
    pub metadata: Option<String>,
}

impl UserConfig {
    pub fn generate(tenant_id: Uuid, email: impl Into<String>, roles: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            email: email.into(),
            password: Uuid::new_v4().to_string(),
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id: Uuid::new_v4(),
                secret: Uuid::new_v4(),
                description: Some("Initial API Token".to_string()),
                created_at: Utc::now(),
            }],
            roles,
            auth_provider: None,
            verified: None,
            metadata: None,
        }
    }

    pub fn client_id(&self) -> &Uuid {
        &self.initial_api_tokens[0].client_id
    }

    pub fn secret(&self) -> &Uuid {
        &self.initial_api_tokens[0].secret
    }

    pub fn frontegg_password(&self) -> String {
        format!("mzp_{}{}", self.client_id(), self.secret())
    }
}

#[derive(Deserialize, Clone, Serialize)]
pub struct UserCreate {
    pub email: String,
    #[serde(rename = "roleIds")]
    pub role_ids: Option<Vec<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserRole {
    pub id: String,
    pub name: String,
    pub key: String,
}

#[derive(Serialize, Deserialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub verified: bool,
    pub metadata: String,
    pub provider: String,
    pub roles: Vec<UserRole>,
}

#[derive(Serialize, Deserialize)]
pub struct UserRolesResponse {
    pub items: Vec<UserRole>,
    pub _metadata: UserRolesMetadata,
}

#[derive(Serialize, Deserialize)]
pub struct UserRolesMetadata {
    pub total_items: usize,
    pub total_pages: usize,
}

#[derive(Deserialize)]
pub struct UpdateUserRolesRequest {
    pub email: String,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct UsersV3Query {
    #[serde(rename = "_email")]
    pub email: Option<String>,
    #[serde(rename = "_limit")]
    pub limit: Option<usize>,
    #[serde(rename = "_offset")]
    pub offset: Option<usize>,
    pub ids: Option<String>,
    #[serde(rename = "_sortBy")]
    pub sort_by: Option<String>,
    #[serde(rename = "_order")]
    pub order: Option<String>,
    #[serde(rename = "_tenantId")]
    pub tenant_id: Option<Uuid>,
    #[serde(rename = "_includeSubTenants")]
    pub include_sub_tenants: Option<bool>,
}

#[derive(Serialize)]
pub struct UsersV3Response {
    pub items: Vec<UserResponse>,
    pub _metadata: UsersV3Metadata,
}

#[derive(Serialize)]
pub struct UsersV3Metadata {
    pub total_items: usize,
}

#[derive(Deserialize)]
pub struct AddRolesToGroupParams {
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct RemoveRolesFromGroupParams {
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct GetUserPasswordRequest {
    pub email: String,
}

#[derive(Serialize)]
pub struct GetUserPasswordResponse {
    pub email: String,
    pub password: String,
}
