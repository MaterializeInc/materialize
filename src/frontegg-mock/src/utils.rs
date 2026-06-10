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
    groups: Vec<String>,
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
    // Stamp groups under the `groups` JWT claim (the default `GROUP_CLAIM`
    // dyncfg value) via the flattened `unknown_claims` bag so the mock can
    // exercise the dyncfg-driven group extraction path. Always emit the claim
    // (even as `[]`) so revocation-from-all-groups produces a present-but-empty
    // claim rather than an omitted claim, matching the semantics group sync
    // relies on (None = no sync; Some([]) = revoke).
    let mut unknown_claims = BTreeMap::new();
    unknown_claims.insert(
        "groups".to_string(),
        serde_json::Value::Array(groups.into_iter().map(serde_json::Value::String).collect()),
    );
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
            unknown_claims,
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

pub fn get_user_groups(context: &Context, user_id: &Uuid) -> Vec<String> {
    let user_id_str = user_id.to_string();
    let mut groups: Vec<String> = context
        .groups
        .lock()
        .unwrap()
        .values()
        .filter(|g| g.users.iter().any(|u| u.id == user_id_str))
        .map(|g| g.name.clone())
        .collect();
    groups.sort();
    groups.dedup();
    groups
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
