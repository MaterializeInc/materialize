// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::models::*;
use crate::server::Context;
use crate::utils::decode_access_token;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use axum_extra::TypedHeader;
use chrono::Utc;
use jsonwebtoken::TokenData;
use std::sync::Arc;
use uuid::Uuid;

pub async fn handle_list_scim_configurations(
    State(context): State<Arc<Context>>,
) -> Json<Vec<SCIM2ConfigurationResponse>> {
    let configs = context.scim_configurations.lock().unwrap();
    let responses: Vec<SCIM2ConfigurationResponse> = configs
        .values()
        .map(|config| SCIM2ConfigurationResponse {
            id: config.id.clone(),
            source: config.source.clone(),
            tenant_id: config.tenant_id.clone(),
            connection_name: config.connection_name.clone(),
            sync_to_user_management: config.sync_to_user_management,
            created_at: config.created_at,
            token: config.token.clone(),
        })
        .collect();
    Json(responses)
}

pub async fn handle_create_scim_configuration(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    Json(request): Json<SCIM2ConfigurationCreateRequest>,
) -> Result<Json<SCIM2ConfigurationResponse>, StatusCode> {
    // Extract claims from the access token
    let claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    let now = Utc::now();
    let new_config = SCIM2ConfigurationStorage {
        id: Uuid::new_v4().to_string(),
        source: request.source,
        tenant_id: claims.tenant_id.to_string(),
        connection_name: request.connection_name,
        sync_to_user_management: request.sync_to_user_management,
        created_at: now,
        token: Uuid::new_v4().to_string(),
    };

    let response = SCIM2ConfigurationResponse {
        id: new_config.id.clone(),
        source: new_config.source.clone(),
        tenant_id: new_config.tenant_id.clone(),
        connection_name: new_config.connection_name.clone(),
        sync_to_user_management: new_config.sync_to_user_management,
        created_at: new_config.created_at,
        token: new_config.token.clone(),
    };

    let mut configs = context.scim_configurations.lock().unwrap();
    configs.insert(new_config.id.clone(), new_config);

    Ok(Json(response))
}

pub async fn handle_delete_scim_configuration(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
) -> StatusCode {
    let mut configs = context.scim_configurations.lock().unwrap();
    if configs.remove(&config_id).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/groupscontrollerv1_getallgroups
pub async fn handle_list_scim_groups(
    State(context): State<Arc<Context>>,
) -> Result<Json<SCIMGroupsResponse>, StatusCode> {
    let groups = context.groups.lock().unwrap();
    let groups_vec: Vec<ScimGroup> = groups.values().cloned().collect();
    Ok(Json(SCIMGroupsResponse { groups: groups_vec }))
}

// https://docs.frontegg.com/reference/groupscontrollerv1_creategroup
pub async fn handle_create_scim_group(
    State(context): State<Arc<Context>>,
    Json(params): Json<GroupCreateParams>,
) -> Result<(StatusCode, Json<ScimGroup>), StatusCode> {
    let now = Utc::now();
    let new_group = ScimGroup {
        id: Uuid::new_v4().to_string(),
        name: params.name,
        description: params.description.unwrap_or_default(),
        metadata: params.metadata.unwrap_or_default(),
        roles: Vec::new(),
        users: Vec::new(),
        managed_by: "".to_string(),
        color: params.color.unwrap_or_default(),
        created_at: now,
        updated_at: now,
    };

    let mut groups = context.groups.lock().unwrap();
    groups.insert(new_group.id.clone(), new_group.clone());

    Ok((StatusCode::CREATED, Json(new_group)))
}

// https://docs.frontegg.com/reference/groupscontrollerv1_getgroupbyid
pub async fn handle_get_scim_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
) -> Result<Json<ScimGroup>, StatusCode> {
    let groups = context.groups.lock().unwrap();
    groups
        .get(&group_id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// https://docs.frontegg.com/reference/groupscontrollerv1_updategroup
pub async fn handle_update_scim_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
    Json(params): Json<GroupUpdateParams>,
) -> Result<Json<ScimGroup>, StatusCode> {
    let mut groups = context.groups.lock().unwrap();
    if let Some(group) = groups.get_mut(&group_id) {
        if let Some(name) = params.name {
            group.name = name;
        }
        if let Some(description) = params.description {
            group.description = description;
        }
        if let Some(color) = params.color {
            group.color = color;
        }
        if let Some(metadata) = params.metadata {
            group.metadata = metadata;
        }
        group.updated_at = Utc::now();
        Ok(Json(group.clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/groupscontrollerv1_deletegroup
pub async fn handle_delete_scim_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
) -> StatusCode {
    let mut groups = context.groups.lock().unwrap();
    if groups.remove(&group_id).is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/groupscontrollerv1_addrolestogroup
pub async fn handle_add_roles_to_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
    Json(payload): Json<AddRolesToGroupParams>,
) -> Result<StatusCode, StatusCode> {
    let mut groups = context.groups.lock().unwrap();
    let roles = Arc::clone(&context.roles);

    if let Some(group) = groups.get_mut(&group_id) {
        for role_id in payload.role_ids {
            if !group.roles.iter().any(|r| r.id == role_id) {
                if let Some(role) = roles.iter().find(|r| r.id == role_id) {
                    group.roles.push(ScimRole {
                        id: role.id.clone(),
                        key: role.key.clone(),
                        name: role.name.clone(),
                        description: format!("Description for {}", role.name),
                        is_default: false,
                    });
                } else {
                    return Err(StatusCode::NOT_FOUND);
                }
            }
        }
        Ok(StatusCode::CREATED)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/groupscontrollerv1_removerolesfromgroup
pub async fn handle_remove_roles_from_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
    Json(payload): Json<RemoveRolesFromGroupParams>,
) -> Result<StatusCode, StatusCode> {
    let mut groups = context.groups.lock().unwrap();

    if let Some(group) = groups.get_mut(&group_id) {
        group
            .roles
            .retain(|role| !payload.role_ids.contains(&role.id));
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
