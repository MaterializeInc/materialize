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
