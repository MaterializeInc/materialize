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
use crate::utils::get_user_roles;
use crate::utils::{decode_access_token, generate_access_token};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use axum_extra::TypedHeader;
use chrono::Utc;
use jsonwebtoken::TokenData;
use mz_frontegg_auth::{ClaimTokenType, Claims};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

// https://docs.frontegg.com/reference/userscontrollerv2_getuserprofile
pub async fn handle_get_user_profile<'a>(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<UserProfileResponse>, StatusCode> {
    let claims: Claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };
    Ok(Json(UserProfileResponse {
        tenant_id: claims.tenant_id,
    }))
}

// https://docs.frontegg.com/reference/userapitokensv1controller_createtenantapitoken
pub async fn handle_post_user_api_token<'a>(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    Json(request): Json<UserApiTokenRequest>,
) -> Result<(StatusCode, Json<UserApiTokenResponse>), StatusCode> {
    let claims: Claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };
    let mut tokens = context.user_api_tokens.lock().unwrap();
    let new_token = ApiToken {
        client_id: Uuid::new_v4(),
        secret: Uuid::new_v4(),
        description: request.description.clone(),
        created_at: Utc::now(),
    };
    tokens.insert(new_token.clone(), claims.email.unwrap());

    let response = UserApiTokenResponse {
        client_id: new_token.client_id.to_string(),
        description: new_token.description.unwrap(),
        created_at: new_token.created_at,
        secret: new_token.secret.to_string(),
    };

    Ok((StatusCode::CREATED, Json(response)))
}

// https://docs.frontegg.com/reference/userapitokensv1controller_getapitokens
pub async fn handle_list_user_api_tokens(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<Vec<UserApiTokenResponse>>, StatusCode> {
    let claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    let user_api_tokens = context.user_api_tokens.lock().unwrap();
    let tokens: Vec<UserApiTokenResponse> = user_api_tokens
        .iter()
        .filter(|(_, email)| *email == claims.email.as_ref().unwrap())
        .map(|(token, _)| UserApiTokenResponse {
            client_id: token.client_id.to_string(),
            description: token.description.clone().unwrap_or_default(),
            created_at: token.created_at,
            secret: token.secret.to_string(),
        })
        .collect();

    Ok(Json(tokens))
}

// https://docs.frontegg.com/reference/userapitokensv1controller_deleteapitoken
pub async fn handle_delete_user_api_token(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    Path(token_id): Path<Uuid>,
) -> StatusCode {
    let claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return StatusCode::UNAUTHORIZED,
    };

    let mut user_api_tokens = context.user_api_tokens.lock().unwrap();

    let removed = user_api_tokens
        .iter()
        .find(|(token, email)| {
            token.client_id == token_id && *email == claims.email.as_ref().unwrap()
        })
        .map(|(token, _)| token.clone());

    if let Some(token_to_remove) = removed {
        user_api_tokens.remove(&token_to_remove);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/tenantapitokensv1controller_gettenantsapitokens
pub async fn handle_list_tenant_api_tokens(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<Vec<TenantApiTokenResponse>>, StatusCode> {
    let _claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    let tenant_api_tokens = context.tenant_api_tokens.lock().unwrap();
    let tokens: Vec<TenantApiTokenResponse> = tenant_api_tokens
        .iter()
        .map(|(api_token, config)| TenantApiTokenResponse {
            client_id: api_token.client_id,
            description: api_token.description.clone().unwrap_or_default(),
            secret: api_token.secret,
            created_by_user_id: config.created_by_user_id,
            metadata: config.metadata.clone(),
            created_at: config.created_at,
            role_ids: config.roles.clone(),
        })
        .collect();

    Ok(Json(tokens))
}

// https://docs.frontegg.com/reference/tenantapitokensv1controller_createtenantapitoken
pub async fn handle_create_tenant_api_token(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    Json(request): Json<CreateTenantApiTokenRequest>,
) -> Result<(StatusCode, Json<TenantApiTokenResponse>), StatusCode> {
    let claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    let new_token = ApiToken {
        client_id: Uuid::new_v4(),
        secret: Uuid::new_v4(),
        description: Some(request.description.clone()),
        created_at: Utc::now(),
    };

    let config = TenantApiTokenConfig {
        tenant_id: claims.tenant_id,
        metadata: request.metadata.clone(),
        roles: request.role_ids.clone(),
        description: Some(request.description.clone()),
        created_by_user_id: claims.sub,
        created_at: new_token.created_at,
    };

    let mut tenant_api_tokens = context.tenant_api_tokens.lock().unwrap();
    tenant_api_tokens.insert(new_token.clone(), config.clone());

    let _access_token = generate_access_token(
        &context,
        ClaimTokenType::TenantApiToken,
        new_token.client_id,
        None,
        None,
        config.tenant_id,
        config.roles.clone(),
        config.metadata.clone(),
    );

    let response = TenantApiTokenResponse {
        client_id: new_token.client_id,
        description: new_token.description.unwrap(),
        secret: new_token.secret,
        created_by_user_id: config.created_by_user_id,
        metadata: config.metadata,
        created_at: new_token.created_at,
        role_ids: config.roles,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

// https://docs.frontegg.com/reference/tenantapitokensv1controller_deletetenantapitoken
pub async fn handle_delete_tenant_api_token(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    Path(token_id): Path<Uuid>,
) -> StatusCode {
    let _claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return StatusCode::UNAUTHORIZED,
    };

    let mut tenant_api_tokens = context.tenant_api_tokens.lock().unwrap();

    let token_to_remove = tenant_api_tokens
        .keys()
        .find(|token| token.client_id == token_id)
        .cloned();

    if let Some(token) = token_to_remove {
        tenant_api_tokens.remove(&token);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/userscontrollerv2_getuserbyid
pub async fn handle_get_user(
    State(context): State<Arc<Context>>,
    Path(user_id): Path<Uuid>,
) -> Result<Json<UserResponse>, StatusCode> {
    let users = context.users.lock().unwrap();
    let role_mapping: BTreeMap<String, UserRole> = context
        .roles
        .iter()
        .map(|role| (role.id.clone(), role.clone()))
        .collect();

    match users.iter().find(|(_, user)| user.id == user_id) {
        Some((_, user)) => {
            let roles = get_user_roles(&user.roles, &role_mapping);

            let user_response = UserResponse {
                id: user.id,
                email: user.email.clone(),
                verified: user.verified.unwrap_or(true),
                metadata: user.metadata.clone().unwrap_or_default(),
                provider: user.auth_provider.clone().unwrap_or_default(),
                roles,
            };

            Ok(Json(user_response))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

// https://docs.frontegg.com/reference/userscontrollerv2_createuser
pub async fn handle_create_user(
    State(context): State<Arc<Context>>,
    Json(new_user): Json<UserCreate>,
) -> Result<(StatusCode, Json<UserResponse>), StatusCode> {
    let mut users = context.users.lock().unwrap();
    let role_mapping: BTreeMap<String, UserRole> = context
        .roles
        .iter()
        .map(|role| (role.id.clone(), role.clone()))
        .collect();

    if users.contains_key(&new_user.email) {
        return Err(StatusCode::CONFLICT);
    }

    let default_tenant_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();

    let role_ids = new_user.role_ids.as_deref().unwrap_or(&[]);
    let mut role_names = Vec::new();

    for role_id in role_ids {
        match role_mapping.get(role_id) {
            Some(role) => role_names.push(role.name.clone()),
            None => return Err(StatusCode::BAD_REQUEST),
        }
    }

    let user_config = UserConfig {
        id: user_id,
        email: new_user.email.clone(),
        password: Uuid::new_v4().to_string(),
        tenant_id: default_tenant_id,
        initial_api_tokens: vec![],
        roles: role_names.clone(),
        auth_provider: None,
        verified: Some(true),
        metadata: None,
    };

    users.insert(new_user.email.clone(), user_config);

    let user_roles = role_ids
        .iter()
        .map(|role_id| role_mapping.get(role_id).unwrap().clone())
        .collect();

    let user_response = UserResponse {
        id: user_id,
        email: new_user.email.clone(),
        verified: true,
        metadata: String::new(),
        provider: String::new(),
        roles: user_roles,
    };

    Ok((StatusCode::CREATED, Json(user_response)))
}

// https://docs.frontegg.com/reference/userscontrollerv1_removeuserfromtenant
pub async fn handle_delete_user(
    State(context): State<Arc<Context>>,
    Path(user_id): Path<Uuid>,
) -> StatusCode {
    let mut users = context.users.lock().unwrap();
    let initial_count = users.len();
    users.retain(|_, user| user.id != user_id);

    if users.len() < initial_count {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/userscontrollerv3_getusers
pub async fn handle_get_users_v3(
    State(context): State<Arc<Context>>,
    Query(query): Query<UsersV3Query>,
) -> Result<Json<UsersV3Response>, (StatusCode, Json<ErrorResponse>)> {
    let users = context.users.lock().unwrap();
    let role_mapping: BTreeMap<String, UserRole> = context
        .roles
        .iter()
        .map(|role| (role.id.clone(), role.clone()))
        .collect();

    let mut filtered_users: Vec<UserResponse> = users
        .iter()
        .filter(|(email, user)| {
            query
                .email
                .as_ref()
                .map_or(true, |q_email| *email == q_email)
                && query.ids.as_ref().map_or(true, |ids| {
                    ids.split(',').any(|id| id == user.id.to_string())
                })
                && query.tenant_id.as_ref().map_or(true, |q_tenant_id| {
                    &user.tenant_id == q_tenant_id || query.include_sub_tenants.unwrap_or(false)
                })
        })
        .map(|(_, user)| UserResponse {
            id: user.id,
            email: user.email.clone(),
            verified: user.verified.unwrap_or(true),
            metadata: user.metadata.clone().unwrap_or_default(),
            provider: user.auth_provider.clone().unwrap_or_default(),
            roles: get_user_roles(&user.roles, &role_mapping),
        })
        .collect();

    // Sort users if sort_by is provided
    if let Some(sort_by) = &query.sort_by {
        let sort_by = SortBy::try_from(sort_by.as_str()).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    errors: vec!["_sortBy must be a valid enum value".to_string()],
                }),
            )
        })?;

        let order = query
            .order
            .as_deref()
            .map(Order::try_from)
            .transpose()
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        errors: vec![
                            "_order must be one of the following values: ASC, DESC".to_string()
                        ],
                    }),
                )
            })?;

        filtered_users.sort_by(|a, b| {
            let cmp = match sort_by {
                SortBy::Email => a.email.cmp(&b.email),
                SortBy::Id => a.id.cmp(&b.id),
            };
            if order == Some(Order::DESC) {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }

    let total_items = filtered_users.len();

    // Apply pagination
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(total_items);
    filtered_users = filtered_users
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();

    Ok(Json(UsersV3Response {
        items: filtered_users,
        _metadata: UsersV3Metadata { total_items },
    }))
}

pub async fn handle_update_user_roles(
    State(context): State<Arc<Context>>,
    Json(request): Json<UpdateUserRolesRequest>,
) -> Result<Json<UserResponse>, StatusCode> {
    let mut users = context.users.lock().unwrap();
    let role_mapping: BTreeMap<String, UserRole> = context
        .roles
        .iter()
        .map(|role| (role.id.clone(), role.clone()))
        .collect();

    if let Some(user) = users.get_mut(&request.email) {
        user.roles.clone_from(&request.role_ids);

        let updated_roles = get_user_roles(&user.roles, &role_mapping);

        let user_response = UserResponse {
            id: user.id,
            email: user.email.clone(),
            verified: user.verified.unwrap_or(true),
            metadata: user.metadata.clone().unwrap_or_default(),
            provider: user.auth_provider.clone().unwrap_or_default(),
            roles: updated_roles,
        };

        Ok(Json(user_response))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/permissionscontrollerv2_getallroles
pub async fn handle_roles_request(State(context): State<Arc<Context>>) -> Json<UserRolesResponse> {
    let roles = Arc::<Vec<UserRole>>::clone(&context.roles);

    let response = UserRolesResponse {
        items: roles.to_vec(),
        _metadata: UserRolesMetadata {
            total_items: roles.len(),
            total_pages: 1,
        },
    };

    Json(response)
}

// https://docs.frontegg.com/reference/groupscontrollerv1_adduserstogroup
pub async fn handle_add_users_to_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
    Json(payload): Json<AddUsersToGroupParams>,
) -> Result<StatusCode, StatusCode> {
    let mut groups = context.groups.lock().unwrap();
    if let Some(group) = groups.get_mut(&group_id) {
        for user_id in payload.user_ids {
            if !group.users.iter().any(|u| u.id == user_id) {
                group.users.push(User {
                    id: user_id,
                    name: "".to_string(),
                    email: "".to_string(),
                });
            }
        }
        Ok(StatusCode::CREATED)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/groupscontrollerv1_removeusersfromgroup
pub async fn handle_remove_users_from_group(
    State(context): State<Arc<Context>>,
    Path(group_id): Path<String>,
    Json(payload): Json<RemoveUsersFromGroupParams>,
) -> StatusCode {
    let mut groups = context.groups.lock().unwrap();

    if let Some(group) = groups.get_mut(&group_id) {
        group
            .users
            .retain(|user| !payload.user_ids.contains(&user.id));
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn internal_handle_get_user_password(
    State(context): State<Arc<Context>>,
    Json(request): Json<GetUserPasswordRequest>,
) -> Result<Json<GetUserPasswordResponse>, StatusCode> {
    let users = context.users.lock().unwrap();

    if let Some(user) = users.get(&request.email) {
        Ok(Json(GetUserPasswordResponse {
            email: user.email.clone(),
            password: user.password.clone(),
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
