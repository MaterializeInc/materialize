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
use uuid::Uuid;

/// Configuration for a tenant in the mock.
#[derive(Debug, Clone)]
pub struct TenantConfig {
    pub id: Uuid,
    pub name: String,
    pub metadata: serde_json::Value,
    pub creator_name: Option<String>,
    pub creator_email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TenantConfig {
    pub fn new(id: Uuid, name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id,
            name: name.into(),
            metadata: serde_json::json!({}),
            creator_name: None,
            creator_email: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }
}

/// Tenant response matching the Frontegg API format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantResponse {
    #[serde(rename = "tenantId")]
    pub id: Uuid,
    pub name: String,
    /// Metadata serialized as a nested JSON string (what Frontegg API returns)
    #[serde(serialize_with = "serialize_nested_json")]
    pub metadata: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator_email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

fn serialize_nested_json<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // Serialize the value as a JSON string
    let json_string = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&json_string)
}

impl From<&TenantConfig> for TenantResponse {
    fn from(config: &TenantConfig) -> Self {
        Self {
            id: config.id,
            name: config.name.clone(),
            metadata: config.metadata.clone(),
            creator_name: config.creator_name.clone(),
            creator_email: config.creator_email.clone(),
            created_at: config.created_at,
            updated_at: config.updated_at,
            deleted_at: config.deleted_at,
        }
    }
}
