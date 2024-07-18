// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::Path;
use axum::extract::State;
use axum::headers::authorization::Bearer;
use axum::headers::Authorization;
use axum::http::{Request, StatusCode};
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router, TypedHeader};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, TokenData};
use mz_frontegg_auth::{ApiTokenResponse, ClaimMetadata, ClaimTokenType, Claims};
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_ore::task::JoinHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

const AUTH_API_TOKEN_PATH: &str = "/identity/resources/auth/v1/api-token";
const AUTH_USER_PATH: &str = "/identity/resources/auth/v1/user";
const AUTH_API_TOKEN_REFRESH_PATH: &str = "/identity/resources/auth/v1/api-token/token/refresh";
const USERS_ME_PATH: &str = "/identity/resources/users/v2/me";
const USERS_API_TOKENS_PATH: &str = "/identity/resources/users/api-tokens/v1";
const USER_PATH: &str = "/identity/resources/users/v1/:id";
const USER_CREATE_PATH: &str = "/identity/resources/users/v2";
const ROLES_PATH: &str = "/identity/resources/roles/v2";
const SSO_CONFIGS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations";
const SSO_CONFIG_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id";
const SSO_CONFIG_DOMAINS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/domains";
const SSO_CONFIG_DOMAIN_PATH: &str =
    "/frontegg/team/resources/sso/v1/configurations/:id/domains/:domain_id";
const SSO_CONFIG_GROUPS_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/groups";
const SSO_CONFIG_GROUP_PATH: &str =
    "/frontegg/team/resources/sso/v1/configurations/:id/groups/:group_id";
const SSO_CONFIG_ROLES_PATH: &str = "/frontegg/team/resources/sso/v1/configurations/:id/roles";

pub struct FronteggMockServer {
    pub base_url: String,
    pub refreshes: Arc<Mutex<u64>>,
    pub enable_auth: Arc<AtomicBool>,
    pub auth_requests: Arc<Mutex<u64>>,
    pub role_updates_tx: UnboundedSender<(String, Vec<String>)>,
    pub handle: JoinHandle<Result<(), hyper::Error>>,
}

impl FronteggMockServer {
    /// Starts a [`FronteggMockServer`], must be started from within a [`tokio::runtime::Runtime`].
    pub fn start(
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
        });

        let router = Router::new()
            .route(AUTH_API_TOKEN_PATH, post(handle_post_auth_api_token))
            .route(AUTH_USER_PATH, post(handle_post_auth_user))
            .route(AUTH_API_TOKEN_REFRESH_PATH, post(handle_post_token_refresh))
            .route(USERS_ME_PATH, get(handle_get_user_profile))
            .route(USERS_API_TOKENS_PATH, post(handle_post_user_api_token))
            .route(USER_PATH, get(handle_get_user).delete(handle_delete_user))
            .route(USER_CREATE_PATH, post(handle_create_user))
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
        let server = axum::Server::bind(&addr)
            .serve(router.into_make_service_with_connect_info::<SocketAddr>());
        let base_url = format!("http://{}", server.local_addr());
        let handle = mz_ore::task::spawn(|| "mzcloud-mock-server", server);

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

fn decode_access_token(
    context: &Context,
    token: &str,
) -> Result<TokenData<Claims>, jsonwebtoken::errors::Error> {
    jsonwebtoken::decode(
        token,
        &context.decoding_key,
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256),
    )
}

fn generate_access_token(
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

fn generate_refresh_token(context: &Context, target: RefreshTokenTarget) -> String {
    let refresh_token = Uuid::new_v4().to_string();
    context
        .refresh_tokens
        .lock()
        .unwrap()
        .insert(refresh_token.clone(), target);
    refresh_token
}

fn get_user_roles(
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

async fn latency_middleware<B>(
    State(context): State<Arc<Context>>,
    request: Request<B>,
    next: Next<B>,
) -> impl IntoResponse {
    // In some cases we want to add latency to test de-duplicating results.
    if let Some(latency) = context.latency {
        tokio::time::sleep(latency).await;
    }
    next.run(request).await
}

async fn role_update_middleware<B>(
    State(context): State<Arc<Context>>,
    request: Request<B>,
    next: Next<B>,
) -> impl IntoResponse {
    {
        let mut role_updates_rx = context.role_updates_rx.lock().unwrap();
        while let Ok((email, roles)) = role_updates_rx.try_recv() {
            let mut users = context.users.lock().unwrap();
            users.get_mut(&email).unwrap().roles = roles;
        }
    }
    next.run(request).await
}

async fn handle_post_auth_api_token(
    State(context): State<Arc<Context>>,
    Json(request): Json<ApiToken>,
) -> Result<Json<ApiTokenResponse>, StatusCode> {
    *context.auth_requests.lock().unwrap() += 1;

    if !context.enable_auth.load(Ordering::Relaxed) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let user_api_tokens = context.user_api_tokens.lock().unwrap();
    let access_token = match user_api_tokens.get(&request) {
        Some(email) => {
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
            match tenant_api_tokens.get(&request) {
                Some(config) => generate_access_token(
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

async fn handle_post_auth_user(
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

async fn handle_post_token_refresh(
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

// https://docs.frontegg.com/reference/userscontrollerv2_getuserprofile
async fn handle_get_user_profile<'a>(
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
async fn handle_post_user_api_token<'a>(
    State(context): State<Arc<Context>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
) -> Result<Json<ApiToken>, StatusCode> {
    let claims: Claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };
    let mut tokens = context.user_api_tokens.lock().unwrap();
    let new_token = ApiToken {
        client_id: Uuid::new_v4(),
        secret: Uuid::new_v4(),
    };
    tokens.insert(new_token.clone(), claims.email.unwrap());
    Ok(Json(new_token))
}

// https://docs.frontegg.com/reference/userscontrollerv2_getuserbyid
async fn handle_get_user(
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
async fn handle_create_user(
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
async fn handle_delete_user(
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

// https://docs.frontegg.com/reference/permissionscontrollerv2_getallroles
async fn handle_roles_request(State(context): State<Arc<Context>>) -> Json<UserRolesResponse> {
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

async fn handle_list_sso_configs(
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

async fn handle_create_sso_config(
    State(context): State<Arc<Context>>,
    Json(new_config): Json<SSOConfigCreateRequest>,
) -> Result<(StatusCode, Json<SSOConfigResponse>), StatusCode> {
    let config_storage = SSOConfigStorage {
        id: Uuid::new_v4().to_string(),
        enabled: new_config.enabled,
        sso_endpoint: new_config.sso_endpoint,
        public_certificate: BASE64.encode(new_config.public_certificate.as_bytes()),
        sign_request: new_config.sign_request,
        acs_url: new_config.acs_url,
        sp_entity_id: new_config.sp_entity_id,
        config_type: new_config.config_type,
        oidc_client_id: new_config.oidc_client_id,
        oidc_secret: new_config.oidc_secret,
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

async fn handle_get_sso_config(
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

async fn handle_update_sso_config(
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

async fn handle_delete_sso_config(
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

async fn handle_list_domains(
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

async fn handle_create_domain(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
    Json(mut new_domain): Json<Domain>,
) -> Result<Json<Domain>, StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        new_domain.id = Uuid::new_v4().to_string();
        new_domain.sso_config_id = config_id;
        new_domain.validated = false;
        config.domains.push(new_domain.clone());
        Ok(Json(new_domain))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn handle_get_default_roles(
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

async fn handle_set_default_roles(
    State(context): State<Arc<Context>>,
    Path(config_id): Path<String>,
    Json(default_roles): Json<DefaultRoles>,
) -> Result<(StatusCode, Json<DefaultRoles>), StatusCode> {
    let mut configs = context.sso_configs.lock().unwrap();
    if let Some(config) = configs.get_mut(&config_id) {
        config.default_roles = default_roles.clone();
        config.role_ids.clone_from(&default_roles.role_ids);
        Ok((StatusCode::CREATED, Json(default_roles)))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn handle_get_domain(
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

async fn handle_update_domain(
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

async fn handle_delete_domain(
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

async fn handle_list_group_mappings(
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

async fn handle_create_group_mapping(
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

async fn handle_get_group_mapping(
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

async fn handle_update_group_mapping(
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

async fn handle_delete_group_mapping(
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

// Structs and Enums
#[derive(Deserialize)]
struct AuthUserRequest {
    email: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshTokenRequest {
    refresh_token: String,
}

// We only use this for the tenant ID, so ignoring all other fields,
// even required ones.
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct UserProfileResponse {
    tenant_id: Uuid,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct ApiToken {
    #[serde(alias = "client_id")]
    pub client_id: Uuid,
    pub secret: Uuid,
}

pub struct TenantApiTokenConfig {
    pub tenant_id: Uuid,
    pub metadata: Option<ClaimMetadata>,
    pub roles: Vec<String>,
}

#[derive(Deserialize, Clone, Serialize)]
pub struct UserConfig {
    pub id: Uuid,
    pub email: String,
    pub password: String,
    pub tenant_id: Uuid,
    pub initial_api_tokens: Vec<ApiToken>,
    pub roles: Vec<String>,
    pub auth_provider: Option<String>,
    pub verified: Option<bool>,
    pub metadata: Option<String>,
}

impl UserConfig {
    pub fn generate(tenant_id: Uuid, email: impl Into<String>, roles: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            email: email.into(),
            password: Uuid::new_v4().to_string(),
            tenant_id,
            initial_api_tokens: vec![ApiToken {
                client_id: Uuid::new_v4(),
                secret: Uuid::new_v4(),
            }],
            roles,
            auth_provider: None,
            verified: None,
            metadata: None,
        }
    }

    pub fn client_id(&self) -> &Uuid {
        &self.initial_api_tokens[0].client_id
    }

    pub fn secret(&self) -> &Uuid {
        &self.initial_api_tokens[0].secret
    }

    pub fn frontegg_password(&self) -> String {
        format!("mzp_{}{}", self.client_id(), self.secret())
    }
}

#[derive(Deserialize, Clone, Serialize)]
pub struct UserCreate {
    pub email: String,
    #[serde(rename = "roleIds")]
    pub role_ids: Option<Vec<String>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserRole {
    pub id: String,
    pub name: String,
    pub key: String,
}

#[derive(Serialize, Deserialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub verified: bool,
    pub metadata: String,
    pub provider: String,
    pub roles: Vec<UserRole>,
}

#[derive(Serialize, Deserialize)]
struct UserRolesResponse {
    items: Vec<UserRole>,
    _metadata: UserRolesMetadata,
}

#[derive(Serialize, Deserialize)]
struct UserRolesMetadata {
    total_items: usize,
    total_pages: usize,
}

#[derive(Deserialize)]
pub struct SSOConfigCreateRequest {
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
}

#[derive(Serialize)]
pub struct DomainResponse {
    pub id: String,
    pub domain: String,
    pub validated: bool,
    #[serde(rename = "ssoConfigId")]
    pub sso_config_id: String,
}

#[derive(Deserialize)]
pub struct DomainUpdateRequest {
    pub domain: Option<String>,
    pub validated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMapping {
    #[serde(default)]
    pub id: String,
    pub group: String,
    #[serde(default, rename = "roleIds")]
    pub role_ids: Vec<String>,
    #[serde(default, rename = "ssoConfigId")]
    pub sso_config_id: String,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct GroupMappingResponse {
    pub id: String,
    pub group: String,
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
    #[serde(rename = "ssoConfigId")]
    pub sso_config_id: String,
    pub enabled: bool,
}

#[derive(Deserialize)]
pub struct GroupMappingUpdateRequest {
    pub group: Option<String>,
    #[serde(rename = "roleIds")]
    pub role_ids: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DefaultRoles {
    #[serde(rename = "roleIds")]
    pub role_ids: Vec<String>,
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
        DomainResponse {
            id: domain.id,
            domain: domain.domain,
            validated: domain.validated,
            sso_config_id: domain.sso_config_id,
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
enum RefreshTokenTarget {
    User(AuthUserRequest),
    ApiToken(ApiToken),
}

struct Context {
    issuer: String,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    users: Mutex<BTreeMap<String, UserConfig>>,
    user_api_tokens: Mutex<BTreeMap<ApiToken, String>>,
    tenant_api_tokens: Mutex<BTreeMap<ApiToken, TenantApiTokenConfig>>,
    role_updates_rx: Mutex<UnboundedReceiver<(String, Vec<String>)>>,
    role_permissions: BTreeMap<String, Vec<String>>,
    now: NowFn,
    expires_in_secs: i64,
    latency: Option<Duration>,
    refresh_tokens: Mutex<BTreeMap<String, RefreshTokenTarget>>,
    refreshes: Arc<Mutex<u64>>,
    enable_auth: Arc<AtomicBool>,
    auth_requests: Arc<Mutex<u64>>,
    roles: Arc<Vec<UserRole>>,
    sso_configs: Mutex<BTreeMap<String, SSOConfigStorage>>,
}
