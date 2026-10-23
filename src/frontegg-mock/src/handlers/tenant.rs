// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mock implementations of the Frontegg vendor tenant API, as consumed by the
//! `frontegg` crate's `Client`.
//!
//! Tenants come from two places: tenants explicitly created through the API,
//! which are stored in [`Context::tenants`], and tenants implied by the
//! `tenant_id` of a configured user, which are synthesized on demand. A
//! user-derived tenant is materialized into [`Context::tenants`] the first time
//! it is modified, so that later reads observe the change. Deleted tenants stay
//! in [`Context::tenants`] with `deleted_at` set, so that a deleted
//! user-derived tenant does not reappear.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::models::{TenantConfig, TenantResponse, UserConfig};
use crate::server::Context;

/// Synthesizes the tenant implied by a configured user's `tenant_id`.
fn derived_tenant(id: Uuid) -> TenantConfig {
    TenantConfig::new(id, format!("Tenant {}", &id.to_string()[..8]))
}

/// Looks up a live (not deleted) tenant, falling back to the tenant implied by
/// a configured user.
fn lookup_tenant(
    tenants: &BTreeMap<Uuid, TenantConfig>,
    users: &BTreeMap<String, UserConfig>,
    id: Uuid,
) -> Option<TenantConfig> {
    match tenants.get(&id) {
        Some(tenant) if tenant.deleted_at.is_some() => None,
        Some(tenant) => Some(tenant.clone()),
        None if users.values().any(|u| u.tenant_id == id) => Some(derived_tenant(id)),
        None => None,
    }
}

/// Handle GET /tenants/resources/tenants/v1
///
/// Lists all live tenants, sorted by ID: those created through the API plus
/// those implied by configured users.
pub async fn handle_list_tenants(State(context): State<Arc<Context>>) -> Json<Vec<TenantResponse>> {
    let users = context.users.lock().unwrap();
    let tenants = context.tenants.lock().unwrap();

    let mut all: BTreeMap<Uuid, TenantConfig> = users
        .values()
        .map(|u| (u.tenant_id, derived_tenant(u.tenant_id)))
        .collect();
    // Explicit entries, including deletion markers, take precedence.
    all.extend(tenants.iter().map(|(id, t)| (*id, t.clone())));

    Json(
        all.values()
            .filter(|t| t.deleted_at.is_none())
            .map(TenantResponse::from)
            .collect(),
    )
}

/// Handle GET /tenants/resources/tenants/v1/{id}
///
/// Like Frontegg, responds with a JSON array: one element if the tenant
/// exists, and empty otherwise. The `frontegg` client pops the single element
/// and maps an empty array to a 404 error.
pub async fn handle_get_tenant(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
) -> Json<Vec<TenantResponse>> {
    let users = context.users.lock().unwrap();
    let tenants = context.tenants.lock().unwrap();
    Json(
        lookup_tenant(&tenants, &users, id)
            .iter()
            .map(TenantResponse::from)
            .collect(),
    )
}

/// Request body for creating a tenant, matching `frontegg::TenantRequest`.
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
///
/// Creates a tenant. Creating a tenant whose ID already exists replaces it.
pub async fn handle_create_tenant(
    State(context): State<Arc<Context>>,
    Json(body): Json<CreateTenantRequest>,
) -> Json<TenantResponse> {
    let now = Utc::now();
    let metadata = match body.metadata {
        serde_json::Value::Null => serde_json::json!({}),
        metadata => metadata,
    };
    let tenant = TenantConfig {
        id: body.tenant_id,
        name: body.name,
        metadata,
        creator_name: body.creator_name,
        creator_email: body.creator_email,
        created_at: now,
        updated_at: now,
        deleted_at: None,
    };

    let response = TenantResponse::from(&tenant);
    context.tenants.lock().unwrap().insert(tenant.id, tenant);
    Json(response)
}

/// Handle DELETE /tenants/resources/tenants/v1/{id}
///
/// Responds with an empty body, which is what the `frontegg` client expects.
pub async fn handle_delete_tenant(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
) -> StatusCode {
    let users = context.users.lock().unwrap();
    let mut tenants = context.tenants.lock().unwrap();
    let Some(mut tenant) = lookup_tenant(&tenants, &users, id) else {
        return StatusCode::NOT_FOUND;
    };
    let now = Utc::now();
    tenant.updated_at = now;
    tenant.deleted_at = Some(now);
    tenants.insert(id, tenant);
    StatusCode::OK
}

/// Request body for setting tenant metadata.
#[derive(Debug, Deserialize)]
pub struct SetTenantMetadataRequest {
    pub metadata: serde_json::Value,
}

/// Handle POST /tenants/resources/tenants/v1/{id}/metadata
///
/// Merges the given keys into the tenant's metadata. Keys that are not
/// mentioned are left in place, as in Frontegg.
pub async fn handle_set_tenant_metadata(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
    Json(body): Json<SetTenantMetadataRequest>,
) -> Result<Json<TenantResponse>, StatusCode> {
    let users = context.users.lock().unwrap();
    let mut tenants = context.tenants.lock().unwrap();
    let mut tenant = lookup_tenant(&tenants, &users, id).ok_or(StatusCode::NOT_FOUND)?;

    match (tenant.metadata.as_object_mut(), body.metadata) {
        (Some(existing), serde_json::Value::Object(new)) => existing.extend(new),
        (_, new) => tenant.metadata = new,
    }
    tenant.updated_at = Utc::now();

    let response = TenantResponse::from(&tenant);
    tenants.insert(id, tenant);
    Ok(Json(response))
}

/// Handle DELETE /tenants/resources/tenants/v1/{id}/metadata/{key}
///
/// Removes a single key from the tenant's metadata.
pub async fn handle_delete_tenant_metadata(
    State(context): State<Arc<Context>>,
    Path((id, key)): Path<(Uuid, String)>,
) -> Result<Json<TenantResponse>, StatusCode> {
    let users = context.users.lock().unwrap();
    let mut tenants = context.tenants.lock().unwrap();
    let mut tenant = lookup_tenant(&tenants, &users, id).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(existing) = tenant.metadata.as_object_mut() {
        existing.remove(&key);
    }
    tenant.updated_at = Utc::now();

    let response = TenantResponse::from(&tenant);
    tenants.insert(id, tenant);
    Ok(Json(response))
}
