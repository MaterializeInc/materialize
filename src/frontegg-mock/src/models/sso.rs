// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::group::{DefaultRoles, GroupMapping, GroupMappingResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct SSOConfigCreateRequest {
    #[serde(default)]
    pub enabled: bool,
    #[serde(rename = "ssoEndpoint")]
    pub sso_endpoint: Option<String>,
    #[serde(rename = "publicCertificate")]
    pub public_certificate: Option<String>,
    #[serde(default)]
    #[serde(rename = "signRequest")]
    pub sign_request: bool,
    #[serde(rename = "acsUrl")]
    pub acs_url: Option<String>,
    #[serde(rename = "spEntityId")]
    pub sp_entity_id: Option<String>,
    #[serde(rename = "type")]
    pub config_type: Option<String>,
    #[serde(rename = "oidcClientId")]
    pub oidc_client_id: Option<String>,
    #[serde(rename = "oidcSecret")]
    pub oidc_secret: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SSOConfigStorage {
    pub id: String,
    pub enabled: bool,
    pub sso_endpoint: String,
    pub public_certificate: String,
    pub sign_request: bool,
    pub acs_url: String,
    pub sp_entity_id: String,
    pub config_type: String,
    pub oidc_client_id: String,
    pub oidc_secret: String,
    pub domains: Vec<Domain>,
    pub groups: Vec<GroupMapping>,
    pub default_roles: DefaultRoles,
    pub generated_verification: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub config_metadata: Option<serde_json::Value>,
    pub override_active_tenant: Option<bool>,
    pub skip_email_domain_validation: Option<bool>,
    pub sub_account_access_limit: Option<i32>,
    pub role_ids: Vec<String>,
}

#[derive(Serialize)]
pub struct SSOConfigResponse {
    pub id: String,
    pub enabled: bool,
    #[serde(rename = "ssoEndpoint")]
    pub sso_endpoint: String,
    #[serde(rename = "publicCertificate")]
    pub public_certificate: String,
    #[serde(rename = "signRequest")]
    pub sign_request: bool,
    #[serde(rename = "acsUrl")]
    pub acs_url: String,
    #[serde(rename = "spEntityId")]
    pub sp_entity_id: String,
    #[serde(rename = "type")]
    pub config_type: String,
    #[serde(rename = "oidcClientId")]
    pub oidc_client_id: String,
    #[serde(rename = "oidcSecret")]
    pub oidc_secret: String,
    pub domains: Vec<DomainResponse>,
    pub groups: Vec<GroupMappingResponse>,
    #[serde(rename = "defaultRoles")]
    pub default_roles: DefaultRoles,
    #[serde(rename = "generatedVerification")]
    pub generated_verification: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updatedAt")]
    pub updated_at: DateTime<Utc>,
    #[serde(rename = "configMetadata")]
    pub config_metadata: Option<serde_json::Value>,
    #[serde(rename = "overrideActiveTenant")]
    pub override_active_tenant: bool,
    #[serde(rename = "skipEmailDomainValidation")]
    pub skip_email_domain_validation: bool,
    #[serde(rename = "subAccountAccessLimit")]
    pub sub_account_access_limit: i32,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
}

#[derive(Deserialize)]
pub struct SSOConfigUpdateRequest {
    pub enabled: Option<bool>,
    #[serde(rename = "ssoEndpoint")]
    pub sso_endpoint: Option<String>,
    #[serde(rename = "publicCertificate")]
    pub public_certificate: Option<String>,
    #[serde(rename = "signRequest")]
    pub sign_request: Option<bool>,
    #[serde(rename = "acsUrl")]
    pub acs_url: Option<String>,
    #[serde(rename = "spEntityId")]
    pub sp_entity_id: Option<String>,
    #[serde(rename = "type")]
    pub config_type: Option<String>,
    #[serde(rename = "oidcClientId")]
    pub oidc_client_id: Option<String>,
    #[serde(rename = "oidcSecret")]
    pub oidc_secret: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Domain {
    #[serde(default)]
    pub id: String,
    pub domain: String,
    #[serde(default)]
    pub validated: bool,
    #[serde(default, rename = "ssoConfigId")]
    pub sso_config_id: String,
    #[serde(skip_deserializing)]
    pub txt_record: String,
}

#[derive(Serialize)]
pub struct DomainResponse {
    pub id: String,
    pub domain: String,
    pub validated: bool,
    #[serde(rename = "ssoConfigId")]
    pub sso_config_id: String,
    #[serde(rename = "txtRecord")]
    pub txt_record: String,
}

#[derive(Deserialize)]
pub struct DomainUpdateRequest {
    pub domain: Option<String>,
    pub validated: Option<bool>,
}

impl From<SSOConfigStorage> for SSOConfigResponse {
    fn from(storage: SSOConfigStorage) -> Self {
        SSOConfigResponse {
            id: storage.id,
            enabled: storage.enabled,
            sso_endpoint: storage.sso_endpoint,
            public_certificate: storage.public_certificate,
            sign_request: storage.sign_request,
            acs_url: storage.acs_url,
            sp_entity_id: storage.sp_entity_id,
            config_type: storage.config_type,
            oidc_client_id: storage.oidc_client_id,
            oidc_secret: storage.oidc_secret,
            domains: storage
                .domains
                .into_iter()
                .map(DomainResponse::from)
                .collect(),
            groups: storage
                .groups
                .into_iter()
                .map(GroupMappingResponse::from)
                .collect(),
            default_roles: storage.default_roles,
            generated_verification: storage.generated_verification,
            created_at: storage.created_at.unwrap_or_else(Utc::now),
            updated_at: storage.updated_at.unwrap_or_else(Utc::now),
            config_metadata: storage.config_metadata,
            override_active_tenant: storage.override_active_tenant.unwrap_or(false),
            skip_email_domain_validation: storage.skip_email_domain_validation.unwrap_or(false),
            sub_account_access_limit: storage.sub_account_access_limit.unwrap_or(0),
            role_ids: storage.role_ids,
        }
    }
}

impl From<Domain> for DomainResponse {
    fn from(domain: Domain) -> Self {
        let txt_record = format!(
            "_saml-domain-challenge.{}.{}.{}",
            Uuid::new_v4(),
            Uuid::new_v4(),
            domain.domain
        );

        DomainResponse {
            id: domain.id,
            domain: domain.domain,
            validated: domain.validated,
            sso_config_id: domain.sso_config_id,
            txt_record,
        }
    }
}

impl From<GroupMapping> for GroupMappingResponse {
    fn from(group: GroupMapping) -> Self {
        GroupMappingResponse {
            id: group.id,
            group: group.group,
            role_ids: group.role_ids,
            sso_config_id: group.sso_config_id,
            enabled: group.enabled,
        }
    }
}
