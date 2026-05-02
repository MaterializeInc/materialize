// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use chrono::Utc;
use axum::http::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use crate::models::{TenantConfig, TenantResponse};
use crate::server::Context;

/// Handle GET /tenants/resources/tenants/v1
/// Lists all tenants.
///
/// If no explicit tenants are configured, derives tenants from the users' tenant_ids.
pub async fn handle_list_tenants(
    State(context): State<Arc<Context>>,
) -> Result<Json<Vec<TenantResponse>>, StatusCode> {
    let tenants = context.tenants.lock().unwrap();

    // If explicit tenants are configured, return those
    if !tenants.is_empty() {
        let responses: Vec<TenantResponse> = tenants.values().map(TenantResponse::from).collect();
        return Ok(Json(responses));
    }
    drop(tenants);

    // Otherwise, derive tenants from users
    let users = context.users.lock().unwrap();
    let tenant_ids: BTreeSet<_> = users.values().map(|u| u.tenant_id).collect();

    let now = Utc::now();
    let responses: Vec<TenantResponse> = tenant_ids
        .into_iter()
        .map(|id| TenantResponse {
            id,
            name: format!("Tenant {}", &id.to_string()[..8]),
            metadata: serde_json::json!({}),
            creator_name: None,
            creator_email: None,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        })
        .collect();

    Ok(Json(responses))
}

/// Handle GET /tenants/resources/tenants/v1/:id
/// Gets a single tenant by ID.
///
/// Note: The frontegg client expects a Vec<Tenant> response (and pops the first element).
pub async fn handle_get_tenant(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<TenantResponse>>, StatusCode> {
    let tenants = context.tenants.lock().unwrap();

    // If the tenant exists in the explicit tenants map, return it
    if let Some(tenant) = tenants.get(&id) {
        return Ok(Json(vec![TenantResponse::from(tenant)]));
    }
    drop(tenants);

    // Check if the tenant exists in users
    let users = context.users.lock().unwrap();
    let tenant_exists = users.values().any(|u| u.tenant_id == id);
    drop(users);

    if !tenant_exists {
        return Ok(Json(vec![])); // Empty vec will cause client to return NOT_FOUND
    }

    // Return a derived tenant
    let now = Utc::now();
    let response = TenantResponse {
        id,
        name: format!("Tenant {}", &id.to_string()[..8]),
        metadata: serde_json::json!({}),
        creator_name: None,
        creator_email: None,
        created_at: now,
        updated_at: now,
        deleted_at: None,
    };

    Ok(Json(vec![response]))
}

/// Request body for creating a tenant.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTenantRequest {
    #[serde(default = "Uuid::new_v4")]
    pub tenant_id: Uuid,
    pub name: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub creator_name: Option<String>,
    pub creator_email: Option<String>,
}

/// Handle POST /tenants/resources/tenants/v1
/// Creates a new tenant.
pub async fn handle_create_tenant(
    State(context): State<Arc<Context>>,
    Json(body): Json<CreateTenantRequest>,
) -> Result<Json<TenantResponse>, StatusCode> {
    let now = Utc::now();
    let tenant = TenantConfig {
        id: body.tenant_id,
        name: body.name,
        metadata: body.metadata,
        creator_name: body.creator_name,
        creator_email: body.creator_email,
        created_at: now,
        updated_at: now,
        deleted_at: None,
    };

    let response = TenantResponse::from(&tenant);
    let mut tenants = context.tenants.lock().unwrap();
    tenants.insert(tenant.id, tenant);

    Ok(Json(response))
}

/// Request body for setting tenant metadata.
#[derive(Debug, Deserialize)]
pub struct SetTenantMetadataRequest {
    pub metadata: serde_json::Value,
}

/// Handle POST /tenants/resources/tenants/v1/:id/metadata
/// Sets/updates tenant metadata.
pub async fn handle_set_tenant_metadata(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
    Json(body): Json<SetTenantMetadataRequest>,
) -> Result<Json<TenantResponse>, StatusCode> {
    let mut tenants = context.tenants.lock().unwrap();

    // If the tenant exists in the explicit tenants map, update it
    if let Some(tenant) = tenants.get_mut(&id) {
        // Merge the new metadata with existing metadata
        if let Some(existing) = tenant.metadata.as_object_mut() {
            if let Some(new_obj) = body.metadata.as_object() {
                for (k, v) in new_obj {
                    existing.insert(k.clone(), v.clone());
                }
            }
        } else {
            tenant.metadata = body.metadata;
        }
        tenant.updated_at = Utc::now();
        return Ok(Json(TenantResponse::from(&*tenant)));
    }
    drop(tenants);

    // Check if the tenant exists in users
    let users = context.users.lock().unwrap();
    let tenant_exists = users.values().any(|u| u.tenant_id == id);
    drop(users);

    if !tenant_exists {
        return Err(StatusCode::NOT_FOUND);
    }

    // Create a new tenant entry with the metadata
    let now = Utc::now();
    let tenant = TenantConfig {
        id,
        name: format!("Tenant {}", &id.to_string()[..8]),
        metadata: body.metadata,
        creator_name: None,
        creator_email: None,
        created_at: now,
        updated_at: now,
        deleted_at: None,
    };
    let response = TenantResponse::from(&tenant);
    let mut tenants = context.tenants.lock().unwrap();
    tenants.insert(id, tenant);

    Ok(Json(response))
}
