// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::RefreshTokenTarget;
use axum::routing::{delete, get, post, put};
use axum::{middleware, Router};
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_ore::task::JoinHandle;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::handlers::*;
use crate::middleware::*;
use crate::models::*;

const AUTH_API_TOKEN_PATH: &str = "/identity/resources/auth/v1/api-token";
const AUTH_USER_PATH: &str = "/identity/resources/auth/v1/user";
const AUTH_API_TOKEN_REFRESH_PATH: &str = "/identity/resources/auth/v1/api-token/token/refresh";
const GROUPS_PATH: &str = "/frontegg/identity/resources/groups/v1";
const GROUP_PATH: &str = "/frontegg/identity/resources/groups/v1/:id";
const GROUP_ROLES_PATH: &str = "/frontegg/identity/resources/groups/v1/:id/roles";
const GROUP_USERS_PATH: &str = "/frontegg/identity/resources/groups/v1/:id/users";
const GROUP_PATH_WITH_SLASH: &str = "/frontegg/identity/resources/groups/v1/:id/";
const MEMBERS_PATH: &str = "/frontegg/team/resources/members/v1";
const USERS_ME_PATH: &str = "/identity/resources/users/v2/me";
const USERS_API_TOKENS_PATH: &str = "/identity/resources/users/api-tokens/v1";
const USER_API_TOKENS_PATH: &str = "/identity/resources/users/api-tokens/v1/:id";
const TENANT_API_TOKENS_PATH: &str = "/identity/resources/tenants/api-tokens/v1";
const TENANT_API_TOKEN_PATH: &str = "/identity/resources/tenants/api-tokens/v1/:id";
const USER_PATH: &str = "/identity/resources/users/v1/:id";
const USER_CREATE_PATH: &str = "/identity/resources/users/v2";
const USERS_V3_PATH: &str = "/identity/resources/users/v3";
const ROLES_PATH: &str = "/identity/resources/roles/v2";
const SCIM_CONFIGURATIONS_PATH: &str = "/frontegg/directory/resources/v1/configurations/scim2";
const SCIM_CONFIGURATION_PATH: &str = "/frontegg/directory/resources/v1/configurations/scim2/:id";
const SSO_CONFIGS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations";
const SSO_CONFIG_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id";
const SSO_CONFIG_DOMAINS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/domains";
const SSO_CONFIG_DOMAIN_PATH: &str =
    "/frontegg/team/resources/sso/v1/configurations/:id/domains/:domain_id";
const SSO_CONFIG_GROUPS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/groups";
const SSO_CONFIG_GROUP_PATH: &str =
    "/frontegg/team/resources/sso/v1/configurations/:id/groups/:group_id";
const SSO_CONFIG_ROLES_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/roles";

// Internal endpoints for testing
const INTERNAL_USER_PASSWORD_PATH: &str = "/api/internal-mock/user-password";

pub struct FronteggMockServer {
    pub base_url: String,
    pub refreshes: Arc<Mutex<u64>>,
    pub enable_auth: Arc<AtomicBool>,
    pub auth_requests: Arc<Mutex<u64>>,
    pub role_updates_tx: UnboundedSender<(String, Vec<String>)>,
    pub handle: JoinHandle<Result<(), std::io::Error>>,
}

impl FronteggMockServer {
    /// Starts a [`FronteggMockServer`], must be started from within a [`tokio::runtime::Runtime`].
    pub async fn start(
        addr: Option<&SocketAddr>,
        issuer: String,
        encoding_key: EncodingKey,
        decoding_key: DecodingKey,
        users: BTreeMap<String, UserConfig>,
        tenant_api_tokens: BTreeMap<ApiToken, TenantApiTokenConfig>,
        role_permissions: Option<BTreeMap<String, Vec<String>>>,
        now: NowFn,
        expires_in_secs: i64,
        latency: Option<Duration>,
        roles: Option<Vec<UserRole>>,
    ) -> Result<FronteggMockServer, anyhow::Error> {
        let (role_updates_tx, role_updates_rx) = unbounded_channel();

        let enable_auth = Arc::new(AtomicBool::new(true));
        let refreshes = Arc::new(Mutex::new(0u64));
        let auth_requests = Arc::new(Mutex::new(0u64));

        let user_api_tokens: BTreeMap<ApiToken, String> = users
            .iter()
            .map(|(email, user)| {
                user.initial_api_tokens
                    .iter()
                    .map(|token| (token.clone(), email.clone()))
            })
            .flatten()
            .collect();
        let role_permissions = role_permissions.unwrap_or_else(|| {
            BTreeMap::from([
                (
                    "MaterializePlatformAdmin".to_owned(),
                    vec![
                        "materialize.environment.write".to_owned(),
                        "materialize.invoice.read".to_owned(),
                    ],
                ),
                (
                    "MaterializePlatform".to_owned(),
                    vec!["materialize.environment.read".to_owned()],
                ),
            ])
        });

        // Provide default roles if None is provided
        let roles = roles.unwrap_or_else(|| {
            vec![
                UserRole {
                    id: uuid::Uuid::new_v4().to_string(),
                    name: "Organization Admin".to_string(),
                    key: "MaterializePlatformAdmin".to_string(),
                },
                UserRole {
                    id: uuid::Uuid::new_v4().to_string(),
                    name: "Organization Member".to_string(),
                    key: "MaterializePlatform".to_string(),
                },
            ]
        });

        let context = Arc::new(Context {
            issuer,
            encoding_key,
            decoding_key,
            users: Mutex::new(users),
            user_api_tokens: Mutex::new(user_api_tokens),
            tenant_api_tokens: Mutex::new(tenant_api_tokens),
            role_updates_rx: Mutex::new(role_updates_rx),
            role_permissions,
            now,
            expires_in_secs,
            latency,
            refresh_tokens: Mutex::new(BTreeMap::new()),
            refreshes: Arc::clone(&refreshes),
            enable_auth: Arc::clone(&enable_auth),
            auth_requests: Arc::clone(&auth_requests),
            roles: Arc::new(roles),
            sso_configs: Mutex::new(BTreeMap::new()),
            groups: Mutex::new(BTreeMap::new()),
            scim_configurations: Mutex::new(BTreeMap::new()),
        });

        let router = Router::new()
            .route(AUTH_API_TOKEN_PATH, post(handle_post_auth_api_token))
            .route(AUTH_USER_PATH, post(handle_post_auth_user))
            .route(AUTH_API_TOKEN_REFRESH_PATH, post(handle_post_token_refresh))
            .route(USERS_ME_PATH, get(handle_get_user_profile))
            .route(
                USERS_API_TOKENS_PATH,
                get(handle_list_user_api_tokens).post(handle_post_user_api_token),
            )
            .route(USER_API_TOKENS_PATH, delete(handle_delete_user_api_token))
            .route(
                TENANT_API_TOKENS_PATH,
                get(handle_list_tenant_api_tokens).post(handle_create_tenant_api_token),
            )
            .route(
                TENANT_API_TOKEN_PATH,
                delete(handle_delete_tenant_api_token),
            )
            .route(USER_PATH, get(handle_get_user).delete(handle_delete_user))
            .route(USER_CREATE_PATH, post(handle_create_user))
            .route(USERS_V3_PATH, get(handle_get_users_v3))
            .route(MEMBERS_PATH, put(handle_update_user_roles))
            .route(ROLES_PATH, get(handle_roles_request))
            .route(
                SSO_CONFIGS_PATH,
                get(handle_list_sso_configs).post(handle_create_sso_config),
            )
            .route(
                SSO_CONFIG_PATH,
                get(handle_get_sso_config)
                    .patch(handle_update_sso_config)
                    .delete(handle_delete_sso_config),
            )
            .route(
                SSO_CONFIG_DOMAINS_PATH,
                get(handle_list_domains).post(handle_create_domain),
            )
            .route(
                SSO_CONFIG_DOMAIN_PATH,
                get(handle_get_domain)
                    .patch(handle_update_domain)
                    .delete(handle_delete_domain),
            )
            .route(
                SSO_CONFIG_GROUPS_PATH,
                get(handle_list_group_mappings).post(handle_create_group_mapping),
            )
            .route(
                SSO_CONFIG_GROUP_PATH,
                get(handle_get_group_mapping)
                    .patch(handle_update_group_mapping)
                    .delete(handle_delete_group_mapping),
            )
            .route(
                SSO_CONFIG_ROLES_PATH,
                get(handle_get_default_roles).put(handle_set_default_roles),
            )
            .route(
                GROUPS_PATH,
                get(handle_list_groups).post(handle_create_group),
            )
            .route(
                GROUP_PATH,
                get(handle_get_group)
                    .patch(handle_update_group)
                    .delete(handle_delete_group),
            )
            .route(GROUP_PATH_WITH_SLASH, get(handle_get_group))
            .route(
                GROUP_ROLES_PATH,
                post(handle_add_roles_to_group).delete(handle_remove_roles_from_group),
            )
            .route(
                GROUP_USERS_PATH,
                post(handle_add_users_to_group).delete(handle_remove_users_from_group),
            )
            .route(
                SCIM_CONFIGURATIONS_PATH,
                get(handle_list_scim_configurations).post(handle_create_scim_configuration),
            )
            .route(
                SCIM_CONFIGURATION_PATH,
                delete(handle_delete_scim_configuration),
            )
            .route(
                INTERNAL_USER_PASSWORD_PATH,
                post(internal_handle_get_user_password),
            )
            .layer(middleware::from_fn(logging_middleware))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&context),
                latency_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                Arc::clone(&context),
                role_update_middleware,
            ))
            .with_state(context);

        let addr = match addr {
            Some(addr) => Cow::Borrowed(addr),
            None => Cow::Owned(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        };
        let listener = TcpListener::bind(*addr).await.unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        );
        let handle = mz_ore::task::spawn(|| "mzcloud-mock-server", server.into_future());

        Ok(FronteggMockServer {
            base_url,
            refreshes,
            enable_auth,
            auth_requests,
            role_updates_tx,
            handle,
        })
    }

    pub fn wait_for_auth(&self, expires_in_secs: u64) {
        let expected = *self.auth_requests.lock().unwrap() + 1;
        Retry::default()
            .factor(1.0)
            .max_duration(Duration::from_secs(expires_in_secs + 20))
            .retry(|_| {
                let refreshes = *self.auth_requests.lock().unwrap();
                if refreshes >= expected {
                    Ok(())
                } else {
                    Err(format!(
                        "expected refresh count {}, got {}",
                        expected, refreshes
                    ))
                }
            })
            .unwrap();
    }

    pub fn auth_api_token_url(&self) -> String {
        format!("{}{}", &self.base_url, AUTH_API_TOKEN_PATH)
    }
}

pub struct Context {
    pub issuer: String,
    pub encoding_key: EncodingKey,
    pub decoding_key: DecodingKey,
    pub users: Mutex<BTreeMap<String, UserConfig>>,
    pub user_api_tokens: Mutex<BTreeMap<ApiToken, String>>,
    pub tenant_api_tokens: Mutex<BTreeMap<ApiToken, TenantApiTokenConfig>>,
    pub role_updates_rx: Mutex<UnboundedReceiver<(String, Vec<String>)>>,
    pub role_permissions: BTreeMap<String, Vec<String>>,
    pub now: NowFn,
    pub expires_in_secs: i64,
    pub latency: Option<Duration>,
    pub refresh_tokens: Mutex<BTreeMap<String, RefreshTokenTarget>>,
    pub refreshes: Arc<Mutex<u64>>,
    pub enable_auth: Arc<AtomicBool>,
    pub auth_requests: Arc<Mutex<u64>>,
    pub roles: Arc<Vec<UserRole>>,
    pub sso_configs: Mutex<BTreeMap<String, SSOConfigStorage>>,
    pub groups: Mutex<BTreeMap<String, Group>>,
    pub scim_configurations: Mutex<BTreeMap<String, SCIM2ConfigurationStorage>>,
}
