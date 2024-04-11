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
use jsonwebtoken::{DecodingKey, EncodingKey, TokenData};
use mz_frontegg_auth::{ApiTokenResponse, Claims};
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
        role_permissions: Option<BTreeMap<String, Vec<String>>>,
        now: NowFn,
        expires_in_secs: i64,
        latency: Option<Duration>,
    ) -> Result<FronteggMockServer, anyhow::Error> {
        let (role_updates_tx, role_updates_rx) = unbounded_channel();

        let enable_auth = Arc::new(AtomicBool::new(true));
        let refreshes = Arc::new(Mutex::new(0u64));
        let auth_requests = Arc::new(Mutex::new(0u64));

        let user_api_tokens: BTreeMap<UserApiToken, String> = users
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

        let context = Arc::new(Context {
            issuer,
            encoding_key,
            decoding_key,
            users: Mutex::new(users),
            user_api_tokens: Mutex::new(user_api_tokens),
            role_updates_rx: Mutex::new(role_updates_rx),
            role_permissions,
            now,
            expires_in_secs,
            latency,
            refresh_tokens: Mutex::new(BTreeMap::new()),
            refreshes: Arc::clone(&refreshes),
            enable_auth: Arc::clone(&enable_auth),
            auth_requests: Arc::clone(&auth_requests),
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
    email: String,
    tenant_id: Uuid,
    roles: Vec<Role>,
) -> String {
    let mut permissions = Vec::new();
    roles.iter().for_each(|role| {
        if let Some(role_permissions) = context.role_permissions.get(role.name.as_str()) {
            permissions.extend_from_slice(role_permissions);
        }
    });
    permissions.sort();
    permissions.dedup();
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &Claims {
            exp: context.now.as_secs() + context.expires_in_secs,
            email,
            iss: context.issuer.clone(),
            sub: Uuid::new_v4(),
            user_id: None,
            tenant_id,
            roles: roles.iter().map(|role| role.name.clone()).collect(),
            permissions,
        },
        &context.encoding_key,
    )
    .unwrap()
}

fn generate_refresh_token(context: &Context, email: String) -> String {
    let refresh_token = Uuid::new_v4().to_string();
    context
        .refresh_tokens
        .lock()
        .unwrap()
        .insert(refresh_token.clone(), email);
    refresh_token
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
        while let Ok((email, role_names)) = role_updates_rx.try_recv() {
            let roles: Vec<Role> = role_names
                .into_iter()
                .map(|name| Role {
                    id: "placeholder_id".to_string(),
                    name,
                })
                .collect();

            let mut users = context.users.lock().unwrap();
            if let Some(user) = users.get_mut(&email) {
                user.roles = roles;
            } else {
                eprintln!("User with email {} not found", email);
            }
        }
    }
    next.run(request).await
}

async fn handle_post_auth_api_token(
    State(context): State<Arc<Context>>,
    Json(request): Json<UserApiToken>,
) -> Result<Json<ApiTokenResponse>, StatusCode> {
    *context.auth_requests.lock().unwrap() += 1;

    if !context.enable_auth.load(Ordering::Relaxed) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let user_api_tokens = context.user_api_tokens.lock().unwrap();
    let (email, user) = match user_api_tokens.get(&request) {
        Some(email) => {
            let users = context.users.lock().unwrap();
            let user = users
                .get(email)
                .expect("API tokens are only created by logged in valid users.");
            (email, user.to_owned())
        }
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let refresh_token = generate_refresh_token(&context, email.clone());
    let access_token = generate_access_token(
        &context,
        email.to_owned(),
        user.tenant_id,
        user.roles.clone(),
    );
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
    let refresh_token = generate_refresh_token(&context, request.email.clone());
    let access_token =
        generate_access_token(&context, request.email, user.tenant_id, user.roles.clone());
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

    let maybe_email = context
        .refresh_tokens
        .lock()
        .unwrap()
        .remove(&previous_refresh_token.refresh_token);
    let Some(email) = maybe_email else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let users = context.users.lock().unwrap();
    let user = match users.get(&email) {
        Some(user) => user.to_owned(),
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let refresh_token = generate_refresh_token(&context, email.clone());
    let access_token = generate_access_token(&context, email, user.tenant_id, user.roles.clone());

    Ok(Json(ApiTokenResponse {
        expires: "".to_string(),
        expires_in: context.expires_in_secs,
        access_token,
        refresh_token,
    }))
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
) -> Result<Json<UserApiToken>, StatusCode> {
    let claims: Claims = match decode_access_token(&context, authorization.token()) {
        Ok(TokenData { claims, .. }) => claims,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };
    let mut tokens = context.user_api_tokens.lock().unwrap();
    let new_token = UserApiToken {
        client_id: Uuid::new_v4(),
        secret: Uuid::new_v4(),
    };
    tokens.insert(new_token.clone(), claims.email);
    Ok(Json(new_token))
}

// https://docs.frontegg.com/reference/userscontrollerv1_getuserbyid
async fn handle_get_user(
    State(context): State<Arc<Context>>,
    Path(user_id): Path<Uuid>,
) -> impl IntoResponse {
    let users = context.users.lock().unwrap();
    match users.iter().find(|(_, user)| user.id == Some(user_id)) {
        Some((_, user)) => Json(user.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// https://docs.frontegg.com/reference/userscontrollerv1_removeuserfromtenant
async fn handle_delete_user(
    State(context): State<Arc<Context>>,
    Path(user_id): Path<Uuid>,
) -> impl IntoResponse {
    let mut users = context.users.lock().unwrap();
    let initial_count = users.len();
    users.retain(|_, user| user.id != Some(user_id));

    if users.len() < initial_count {
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

// https://docs.frontegg.com/reference/userscontrollerv1_createuser
async fn handle_create_user(
    State(context): State<Arc<Context>>,
    Json(new_user): Json<UserCreate>,
) -> impl IntoResponse {
    let mut users = context.users.lock().unwrap();

    if users.contains_key(&new_user.email) {
        return (StatusCode::CONFLICT, "User already exists").into_response();
    }

    let default_tenant_id = Uuid::new_v4();

    // As per the Frontegg API, when creating a user, the role_ids field is optional.
    // The create request payload includes a list of role IDs
    // On successful creation, the response payload includes a list of role names
    let role_names = match new_user.role_ids {
        Some(ref role_ids) => {
            if role_ids.is_empty() {
                Vec::new()
            } else {
                role_ids
                    .iter()
                    .filter_map(|role_id| match role_id.as_str() {
                        "1" => Some(Role {
                            id: "1".to_string(),
                            name: "Organization Admin".to_string(),
                        }),
                        "2" => Some(Role {
                            id: "2".to_string(),
                            name: "Organization Member".to_string(),
                        }),
                        _ => None,
                    })
                    .collect()
            }
        }
        None => Vec::new(),
    };

    let user_config = UserConfig {
        id: Some(Uuid::new_v4()),
        email: new_user.email.clone(),
        password: Uuid::new_v4().to_string(),
        tenant_id: default_tenant_id,
        initial_api_tokens: vec![],
        roles: role_names,
        auth_provider: None,
        verified: None,
    };

    let response = (StatusCode::CREATED, Json(&user_config)).into_response();

    users.insert(new_user.email.clone(), user_config);
    response
}

// https://docs.frontegg.com/reference/permissionscontrollerv2_getallroles
async fn handle_roles_request(State(_context): State<Arc<Context>>) -> impl IntoResponse {
    let roles = vec![
        Role {
            id: "1".to_string(),
            name: "Organization Admin".to_string(),
        },
        Role {
            id: "2".to_string(),
            name: "Organization Member".to_string(),
        },
    ];

    let response = RolesResponse {
        items: roles,
        _metadata: RolesMetadata {
            total_items: 2,
            total_pages: 1,
        },
    };

    Json(response)
}

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
pub struct UserApiToken {
    #[serde(alias = "client_id")]
    pub client_id: Uuid,
    pub secret: Uuid,
}

#[derive(Deserialize, Clone, Serialize)]
pub struct UserConfig {
    pub id: Option<Uuid>,
    pub email: String,
    pub password: String,
    pub tenant_id: Uuid,
    pub initial_api_tokens: Vec<UserApiToken>,
    pub roles: Vec<Role>,
    pub auth_provider: Option<String>,
    pub verified: Option<bool>,
}

impl UserConfig {
    pub fn generate(tenant_id: Uuid, email: impl Into<String>, roles: Vec<String>) -> Self {
        Self {
            id: Some(Uuid::new_v4()),
            email: email.into(),
            password: Uuid::new_v4().to_string(),
            tenant_id,
            initial_api_tokens: vec![UserApiToken {
                client_id: Uuid::new_v4(),
                secret: Uuid::new_v4(),
            }],
            roles: roles
                .into_iter()
                .map(|name| Role {
                    id: "placeholder_id".to_string(),
                    name,
                })
                .collect(),
            auth_provider: None,
            verified: None,
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
pub struct Role {
    pub id: String,
    pub name: String,
}

#[derive(Serialize, Deserialize)]
struct RolesResponse {
    items: Vec<Role>,
    _metadata: RolesMetadata,
}

#[derive(Serialize, Deserialize)]
struct RolesMetadata {
    total_items: usize,
    total_pages: usize,
}
struct Context {
    issuer: String,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    users: Mutex<BTreeMap<String, UserConfig>>,
    user_api_tokens: Mutex<BTreeMap<UserApiToken, String>>,
    role_updates_rx: Mutex<UnboundedReceiver<(String, Vec<String>)>>,
    role_permissions: BTreeMap<String, Vec<String>>,
    now: NowFn,
    expires_in_secs: i64,
    latency: Option<Duration>,
    // Uuid -> email
    refresh_tokens: Mutex<BTreeMap<String, String>>,
    refreshes: Arc<Mutex<u64>>,
    enable_auth: Arc<AtomicBool>,
    auth_requests: Arc<Mutex<u64>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefreshToken<'a> {
    refresh_token: &'a str,
}
