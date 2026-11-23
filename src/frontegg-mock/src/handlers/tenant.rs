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
//! Tenants exist only once created through the API; configured users do not
//! imply tenants.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::models::{TenantConfig, TenantResponse};
use crate::server::Context;

/// Handle GET /tenants/resources/tenants/v1
///
/// Lists all tenants, sorted by ID.
pub async fn handle_list_tenants(State(context): State<Arc<Context>>) -> Json<Vec<TenantResponse>> {
    let tenants = context.tenants.lock().unwrap();
    Json(tenants.values().map(TenantResponse::from).collect())
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
    let tenants = context.tenants.lock().unwrap();
    Json(
        tenants
            .get(&id)
            .map(TenantResponse::from)
            .into_iter()
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
/// Creates a tenant. Fails with a 409 if a tenant with the ID already exists.
pub async fn handle_create_tenant(
    State(context): State<Arc<Context>>,
    Json(body): Json<CreateTenantRequest>,
) -> Result<Json<TenantResponse>, StatusCode> {
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

    let mut tenants = context.tenants.lock().unwrap();
    if tenants.contains_key(&tenant.id) {
        return Err(StatusCode::CONFLICT);
    }
    let response = TenantResponse::from(&tenant);
    tenants.insert(tenant.id, tenant);
    Ok(Json(response))
}

/// Handle DELETE /tenants/resources/tenants/v1/{id}
///
/// Responds with an empty body, which is what the `frontegg` client expects.
pub async fn handle_delete_tenant(
    State(context): State<Arc<Context>>,
    Path(id): Path<Uuid>,
) -> StatusCode {
    let removed = context.tenants.lock().unwrap().remove(&id);
    match removed {
        Some(_) => StatusCode::OK,
        None => StatusCode::NOT_FOUND,
    }
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
    let mut tenants = context.tenants.lock().unwrap();
    let tenant = tenants.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    match (tenant.metadata.as_object_mut(), body.metadata) {
        (Some(existing), serde_json::Value::Object(new)) => existing.extend(new),
        (_, new) => tenant.metadata = new,
    }
    tenant.updated_at = Utc::now();

    Ok(Json(TenantResponse::from(&*tenant)))
}

/// Handle DELETE /tenants/resources/tenants/v1/{id}/metadata/{key}
///
/// Removes a single key from the tenant's metadata.
pub async fn handle_delete_tenant_metadata(
    State(context): State<Arc<Context>>,
    Path((id, key)): Path<(Uuid, String)>,
) -> Result<Json<TenantResponse>, StatusCode> {
    let mut tenants = context.tenants.lock().unwrap();
    let tenant = tenants.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(existing) = tenant.metadata.as_object_mut() {
        existing.remove(&key);
    }
    tenant.updated_at = Utc::now();

    Ok(Json(TenantResponse::from(&*tenant)))
}
