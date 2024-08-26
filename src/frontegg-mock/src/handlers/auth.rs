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
use crate::utils::{generate_access_token, generate_refresh_token, RefreshTokenTarget};
use axum::{extract::State, http::StatusCode, Json};
use mz_frontegg_auth::{ApiTokenResponse, ClaimTokenType};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub async fn handle_post_auth_api_token(
    State(context): State<Arc<Context>>,
    Json(request): Json<ApiToken>,
) -> Result<Json<ApiTokenResponse>, StatusCode> {
    *context.auth_requests.lock().unwrap() += 1;

    if !context.enable_auth.load(Ordering::Relaxed) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let user_api_tokens = context.user_api_tokens.lock().unwrap();
    let access_token = match user_api_tokens
        .iter()
        .find(|(token, _)| token.client_id == request.client_id && token.secret == request.secret)
    {
        Some((_, email)) => {
            let users = context.users.lock().unwrap();
            let user = users
                .get(email)
                .expect("API tokens are only created by logged in valid users.");
            generate_access_token(
                &context,
                ClaimTokenType::UserApiToken,
                request.client_id,
                Some(email.to_owned()),
                Some(user.id),
                user.tenant_id,
                user.roles.clone(),
                None,
            )
        }
        None => {
            let tenant_api_tokens = context.tenant_api_tokens.lock().unwrap();
            match tenant_api_tokens.iter().find(|(token, _)| {
                token.client_id == request.client_id && token.secret == request.secret
            }) {
                Some((_, config)) => generate_access_token(
                    &context,
                    ClaimTokenType::TenantApiToken,
                    request.client_id,
                    None,
                    None,
                    config.tenant_id,
                    config.roles.clone(),
                    config.metadata.clone(),
                ),
                None => return Err(StatusCode::UNAUTHORIZED),
            }
        }
    };
    let refresh_token = generate_refresh_token(&context, RefreshTokenTarget::ApiToken(request));
    Ok(Json(ApiTokenResponse {
        expires: "".to_string(),
        expires_in: context.expires_in_secs,
        access_token,
        refresh_token,
    }))
}

pub async fn handle_post_auth_user(
    State(context): State<Arc<Context>>,
    Json(request): Json<AuthUserRequest>,
) -> Result<Json<ApiTokenResponse>, StatusCode> {
    *context.auth_requests.lock().unwrap() += 1;

    if !context.enable_auth.load(Ordering::Relaxed) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let users = context.users.lock().unwrap();
    let user = match users.get(&request.email) {
        Some(user) if request.password == user.password => user.to_owned(),
        _ => return Err(StatusCode::UNAUTHORIZED),
    };
    let access_token = generate_access_token(
        &context,
        ClaimTokenType::UserToken,
        user.id,
        Some(request.email.clone()),
        Some(user.id),
        user.tenant_id,
        user.roles.clone(),
        None,
    );
    let refresh_token = generate_refresh_token(&context, RefreshTokenTarget::User(request));
    Ok(Json(ApiTokenResponse {
        expires: "".to_string(),
        expires_in: context.expires_in_secs,
        access_token,
        refresh_token,
    }))
}

pub async fn handle_post_token_refresh(
    State(context): State<Arc<Context>>,
    Json(previous_refresh_token): Json<RefreshTokenRequest>,
) -> Result<Json<ApiTokenResponse>, StatusCode> {
    // Always count refresh attempts, even if enable_refresh is false.
    *context.refreshes.lock().unwrap() += 1;

    if !context.enable_auth.load(Ordering::Relaxed) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let maybe_target = context
        .refresh_tokens
        .lock()
        .unwrap()
        .remove(&previous_refresh_token.refresh_token);
    let Some(target) = maybe_target else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    match target {
        RefreshTokenTarget::User(request) => {
            handle_post_auth_user(State(context), Json(request)).await
        }
        RefreshTokenTarget::ApiToken(request) => {
            handle_post_auth_api_token(State(context), Json(request)).await
        }
    }
}
