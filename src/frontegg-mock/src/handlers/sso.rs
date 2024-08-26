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
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

// https://docs.frontegg.com/reference/ssoconfigurationcontrollerv1_getssoconfigurations
pub async fn handle_list_sso_configs(
    State(context): State<Arc<Context>>,
) -> Result<Json<Vec<SSOConfigResponse>>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    let config_list: Vec<SSOConfigResponse> = configs
        .values()
        .cloned()
        .map(SSOConfigResponse::from)
        .collect();
    Ok(Json(config_list))
}

// https://docs.frontegg.com/reference/ssoconfigurationcontrollerv1_createssoconfiguration
pub async fn handle_create_sso_config(
    State(context): State<Arc<Context>>,
    Json(new_config): Json<SSOConfigCreateRequest>,
) -> Result<(StatusCode, Json<SSOConfigResponse>), StatusCode> {
    let config_storage = SSOConfigStorage {
        id: Uuid::new_v4().to_string(),
        enabled: new_config.enabled,
        sso_endpoint: new_config.sso_endpoint.unwrap_or_default(),
        public_certificate: new_config
            .public_certificate
            .map(|cert| BASE64.encode(cert.as_bytes()))
            .unwrap_or_default(),
        sign_request: new_config.sign_request,
        acs_url: new_config.acs_url.unwrap_or_default(),
        sp_entity_id: new_config.sp_entity_id.unwrap_or_default(),
        config_type: new_config.config_type.unwrap_or_else(|| "saml".to_string()),
        oidc_client_id: new_config.oidc_client_id.unwrap_or_default(),
        oidc_secret: new_config.oidc_secret.unwrap_or_default(),
        domains: Vec::new(),
        groups: Vec::new(),
        default_roles: DefaultRoles {
            role_ids: Vec::new(),
        },
        generated_verification: Some(Uuid::new_v4().to_string()),
        created_at: Some(Utc::now()),
        updated_at: Some(Utc::now()),
        config_metadata: None,
        override_active_tenant: Some(true),
        sub_account_access_limit: Some(0),
        skip_email_domain_validation: Some(false),
        role_ids: Vec::new(),
    };

    let mut configs = context.sso_configs.lock().unwrap();
    configs.insert(config_storage.id.clone(), config_storage.clone());

    let response = SSOConfigResponse::from(config_storage);
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn handle_get_sso_config(
    State(context): State<Arc<Context>>,
    Path(id): Path<String>,
) -> Result<Json<SSOConfigResponse>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    configs
        .get(&id)
        .cloned()
        .map(SSOConfigResponse::from)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn handle_update_sso_config(
    State(context): State<Arc<Context>>,
    Path(id): Path<String>,
    Json(updated_config): Json<SSOConfigUpdateRequest>,
) -> Result<Json<SSOConfigResponse>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&id) {
        if let Some(enabled) = updated_config.enabled {
            config.enabled = enabled;
        }
        if let Some(sso_endpoint) = updated_config.sso_endpoint {
            config.sso_endpoint = sso_endpoint;
        }
        if let Some(public_certificate) = updated_config.public_certificate {
            config.public_certificate = BASE64.encode(public_certificate.as_bytes());
        }
        if let Some(sign_request) = updated_config.sign_request {
            config.sign_request = sign_request;
        }
        if let Some(acs_url) = updated_config.acs_url {
            config.acs_url = acs_url;
        }
        if let Some(sp_entity_id) = updated_config.sp_entity_id {
            config.sp_entity_id = sp_entity_id;
        }
        if let Some(config_type) = updated_config.config_type {
            config.config_type = config_type;
        }
        if let Some(oidc_client_id) = updated_config.oidc_client_id {
            config.oidc_client_id = oidc_client_id;
        }
        if let Some(oidc_secret) = updated_config.oidc_secret {
            config.oidc_secret = oidc_secret;
        }

        config.updated_at = Some(Utc::now());

        let response = SSOConfigResponse::from(config.clone());
        Ok(Json(response))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssoconfigurationcontrollerv1_deletessoconfiguration
pub async fn handle_delete_sso_config(
    State(context): State<Arc<Context>>,
    Path(id): Path<String>,
) -> StatusCode {
    let mut configs = context.sso_configs.lock().unwrap();
    if configs.remove(&id).is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn handle_list_domains(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
) -> Result<Json<Vec<DomainResponse>>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get(&config_id) {
        let domains: Vec<DomainResponse> = config
            .domains
            .iter()
            .cloned()
            .map(DomainResponse::from)
            .collect();
        Ok(Json(domains))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssodomaincontrollerv1_createssodomain
pub async fn handle_create_domain(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
    Json(mut new_domain): Json<Domain>,
) -> Result<Json<DomainResponse>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        new_domain.id = Uuid::new_v4().to_string();
        new_domain.sso_config_id = config_id;
        new_domain.validated = false;
        config.domains.push(new_domain.clone());
        Ok(Json(DomainResponse::from(new_domain)))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssorolescontrollerv1_getssodefaultroles
pub async fn handle_get_default_roles(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
) -> Result<Json<DefaultRoles>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get(&config_id) {
        Ok(Json(config.default_roles.clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssorolescontrollerv1_setssodefaultroles
pub async fn handle_set_default_roles(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
    Json(default_roles): Json<DefaultRoles>,
) -> Result<(StatusCode, Json<DefaultRoles>), StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        config.default_roles = default_roles.clone();
        for role_id in &default_roles.role_ids {
            if !config.role_ids.contains(role_id) {
                config.role_ids.push(role_id.clone());
            }
        }
        Ok((StatusCode::CREATED, Json(default_roles)))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn handle_get_domain(
    State(context): State<Arc<Context>>,
    Path((config_id, domain_id)): Path<(String, String)>,
) -> Result<Json<DomainResponse>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get(&config_id) {
        config
            .domains
            .iter()
            .find(|domain| domain.id == domain_id)
            .cloned()
            .map(DomainResponse::from)
            .map(Json)
            .ok_or(StatusCode::NOT_FOUND)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn handle_update_domain(
    State(context): State<Arc<Context>>,
    Path((config_id, domain_id)): Path<(String, String)>,
    Json(updated_domain): Json<DomainUpdateRequest>,
) -> Result<Json<DomainResponse>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        if let Some(domain) = config.domains.iter_mut().find(|d| d.id == domain_id) {
            if let Some(new_domain) = updated_domain.domain {
                domain.domain = new_domain;
            }
            if let Some(new_validated) = updated_domain.validated {
                domain.validated = new_validated;
            }
            Ok(Json(DomainResponse::from(domain.clone())))
        } else {
            Err(StatusCode::NOT_FOUND)
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssodomaincontrollerv1_deletessodomain
pub async fn handle_delete_domain(
    State(context): State<Arc<Context>>,
    Path((config_id, domain_id)): Path<(String, String)>,
) -> StatusCode {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        let initial_len = config.domains.len();
        config.domains.retain(|d| d.id != domain_id);
        if config.domains.len() < initial_len {
            StatusCode::OK
        } else {
            StatusCode::NOT_FOUND
        }
    } else {
        StatusCode::NOT_FOUND
    }
}

// https://docs.frontegg.com/reference/ssogroupscontrollerv1_getssogroup
pub async fn handle_list_group_mappings(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
) -> Result<Json<Vec<GroupMappingResponse>>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get(&config_id) {
        let groups: Vec<GroupMappingResponse> = config
            .groups
            .iter()
            .cloned()
            .map(GroupMappingResponse::from)
            .collect();
        Ok(Json(groups))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssogroupscontrollerv1_createssogroup
pub async fn handle_create_group_mapping(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
    Json(new_group): Json<GroupMapping>,
) -> Result<Json<GroupMappingResponse>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        let group = GroupMapping {
            id: Uuid::new_v4().to_string(),
            group: new_group.group,
            role_ids: new_group.role_ids,
            sso_config_id: config_id,
            enabled: true,
        };
        config.groups.push(group.clone());
        Ok(Json(GroupMappingResponse::from(group)))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssogroupscontrollerv1_getssogroup
pub async fn handle_get_group_mapping(
    State(context): State<Arc<Context>>,
    Path((config_id, group_id)): Path<(String, String)>,
) -> Result<Json<GroupMappingResponse>, StatusCode> {
    let configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get(&config_id) {
        config
            .groups
            .iter()
            .find(|g| g.id == group_id)
            .cloned()
            .map(GroupMappingResponse::from)
            .map(Json)
            .ok_or(StatusCode::NOT_FOUND)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssogroupscontrollerv1_updatessogroup
pub async fn handle_update_group_mapping(
    State(context): State<Arc<Context>>,
    Path((config_id, group_id)): Path<(String, String)>,
    Json(updated_group): Json<GroupMappingUpdateRequest>,
) -> Result<Json<GroupMappingResponse>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        if let Some(group) = config.groups.iter_mut().find(|g| g.id == group_id) {
            if let Some(new_group) = updated_group.group {
                group.group = new_group;
            }
            if let Some(new_role_ids) = updated_group.role_ids {
                group.role_ids = new_role_ids;
            }
            if let Some(new_enabled) = updated_group.enabled {
                group.enabled = new_enabled;
            }
            Ok(Json(GroupMappingResponse::from(group.clone())))
        } else {
            Err(StatusCode::NOT_FOUND)
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// https://docs.frontegg.com/reference/ssogroupscontrollerv1_deletessogroup
pub async fn handle_delete_group_mapping(
    State(context): State<Arc<Context>>,
    Path((config_id, group_id)): Path<(String, String)>,
) -> StatusCode {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        let initial_len = config.groups.len();
        config.groups.retain(|g| g.id != group_id);
        if config.groups.len() < initial_len {
            StatusCode::OK
        } else {
            StatusCode::NOT_FOUND
        }
    } else {
        StatusCode::NOT_FOUND
    }
}
