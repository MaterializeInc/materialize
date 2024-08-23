// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::models::{ApiToken, AuthUserRequest, UserRole};
use crate::server::Context;
use jsonwebtoken::TokenData;
use mz_frontegg_auth::{ClaimMetadata, ClaimTokenType, Claims};
use std::collections::BTreeMap;
use uuid::Uuid;

pub fn decode_access_token(
    context: &Context,
    token: &str,
) -> Result<TokenData<Claims>, jsonwebtoken::errors::Error> {
    jsonwebtoken::decode(
        token,
        &context.decoding_key,
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256),
    )
}

pub fn generate_access_token(
    context: &Context,
    token_type: ClaimTokenType,
    sub: Uuid,
    email: Option<String>,
    user_id: Option<Uuid>,
    tenant_id: Uuid,
    roles: Vec<String>,
    metadata: Option<ClaimMetadata>,
) -> String {
    let mut permissions = Vec::new();
    roles.iter().for_each(|role| {
        if let Some(role_permissions) = context.role_permissions.get(role.as_str()) {
            permissions.extend_from_slice(role_permissions);
        }
    });
    permissions.sort();
    permissions.dedup();
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &Claims {
            token_type,
            exp: context.now.as_secs() + context.expires_in_secs,
            email,
            iss: context.issuer.clone(),
            sub,
            user_id,
            tenant_id,
            roles,
            permissions,
            metadata,
        },
        &context.encoding_key,
    )
    .unwrap()
}

pub fn generate_refresh_token(context: &Context, target: RefreshTokenTarget) -> String {
    let refresh_token = Uuid::new_v4().to_string();
    context
        .refresh_tokens
        .lock()
        .unwrap()
        .insert(refresh_token.clone(), target);
    refresh_token
}

pub fn get_user_roles(
    role_ids_or_names: &[String],
    role_mapping: &BTreeMap<String, UserRole>,
) -> Vec<UserRole> {
    role_ids_or_names
        .iter()
        .map(|id_or_name| {
            role_mapping
                .get(id_or_name)
                .cloned()
                .unwrap_or_else(|| UserRole {
                    id: id_or_name.clone(),
                    name: id_or_name.clone(),
                    key: id_or_name.clone(),
                })
        })
        .collect()
}

pub enum RefreshTokenTarget {
    User(AuthUserRequest),
    ApiToken(ApiToken),
}
