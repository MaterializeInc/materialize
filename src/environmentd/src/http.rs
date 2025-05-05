// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Embedded HTTP server.
//!
//! environmentd embeds an HTTP server for introspection into the running
//! process. At the moment, its primary exports are Prometheus metrics, heap
//! profiles, and catalog dumps.

// Axum handlers must use async, but often don't actually use `await`.
#![allow(clippy::unused_async)]

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use axum::error_handling::HandleErrorLayer;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Query, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::{Extension, Json, Router, routing};
use futures::future::{Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Method, StatusCode};
use hyper_openssl::SslStream;
use hyper_openssl::client::legacy::MaybeHttpsStream;
use hyper_util::rt::TokioIo;
use mz_adapter::session::{Session, SessionConfig};
use mz_adapter::{AdapterError, AdapterNotice, Client, SessionClient, WebhookAppenderCache};
use mz_auth::password::Password;
use mz_authenticator::Authenticator;
use mz_frontegg_auth::Error as FronteggError;
use mz_http_util::DynamicFilterTarget;
use mz_ore::cast::u64_to_usize;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME, epoch_to_uuid_v7};
use mz_ore::str::StrExt;
use mz_pgwire_common::{ConnectionCounter, ConnectionHandle};
use mz_repr::user::ExternalUserMetadata;
use mz_server_core::listeners::{AllowedRoles, AuthenticatorKind, HttpRoutesEnabled};
use mz_server_core::{Connection, ConnectionHandler, ReloadingSslContext, Server};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::{
    HTTP_DEFAULT_USER, INTERNAL_USER_NAMES, SUPPORT_USER_NAME, SYSTEM_USER_NAME,
};
use mz_sql::session::vars::{Value, Var, VarInput, WELCOME_MESSAGE};
use openssl::ssl::Ssl;
use prometheus::{
    COMPUTE_METRIC_QUERIES, FRONTIER_METRIC_QUERIES, STORAGE_METRIC_QUERIES, USAGE_METRIC_QUERIES,
};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, watch};
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::{Service, ServiceBuilder};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use crate::BUILD_INFO;
use crate::deployment::state::DeploymentStateHandle;
use crate::http::sql::SqlError;

mod catalog;
mod console;
mod memory;
mod metrics;
mod probe;
mod prometheus;
mod root;
mod sql;
mod webhook;

pub use metrics::Metrics;
pub use sql::{SqlResponse, WebSocketAuth, WebSocketResponse};

/// Maximum allowed size for a request.
pub const MAX_REQUEST_SIZE: usize = u64_to_usize(2 * bytesize::MB);

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub source: &'static str,
    pub tls: Option<ReloadingTlsConfig>,
    pub authenticator_kind: AuthenticatorKind,
    pub authenticator_rx: Shared<Receiver<Arc<Authenticator>>>,
    pub adapter_client_rx: Shared<Receiver<Client>>,
    pub allowed_origin: AllowOrigin,
    pub active_connection_counter: ConnectionCounter,
    pub helm_chart_version: Option<String>,
    pub concurrent_webhook_req: Arc<tokio::sync::Semaphore>,
    pub metrics: Metrics,
    pub metrics_registry: MetricsRegistry,
    pub allowed_roles: AllowedRoles,
    pub internal_route_config: InternalRouteConfig,
    pub routes_enabled: HttpRoutesEnabled,
}

#[derive(Debug, Clone)]
pub struct InternalRouteConfig {
    pub deployment_state_handle: DeploymentStateHandle,
    pub internal_console_redirect_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReloadingTlsConfig {
    pub context: ReloadingSslContext,
    pub mode: TlsMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    Disable,
    Require,
}

#[derive(Clone)]
pub struct WsState {
    authenticator_rx: Delayed<Arc<Authenticator>>,
    adapter_client_rx: Delayed<mz_adapter::Client>,
    active_connection_counter: ConnectionCounter,
    helm_chart_version: Option<String>,
    allowed_roles: AllowedRoles,
}

#[derive(Clone)]
pub struct WebhookState {
    adapter_client_rx: Delayed<mz_adapter::Client>,
    webhook_cache: WebhookAppenderCache,
}

#[derive(Debug)]
pub struct HttpServer {
    tls: Option<ReloadingTlsConfig>,
    router: Router,
}

impl HttpServer {
    pub fn new(
        HttpConfig {
            source,
            tls,
            authenticator_kind,
            authenticator_rx,
            adapter_client_rx,
            allowed_origin,
            active_connection_counter,
            helm_chart_version,
            concurrent_webhook_req,
            metrics,
            metrics_registry,
            allowed_roles,
            internal_route_config,
            routes_enabled,
        }: HttpConfig,
    ) -> HttpServer {
        let tls_mode = tls.as_ref().map(|tls| tls.mode).unwrap_or(TlsMode::Disable);
        let webhook_cache = WebhookAppenderCache::new();

        let auth_middleware_authenticator_rx = authenticator_rx.clone();
        let auth_middleware = middleware::from_fn(move |req, next| {
            let authenticator_rx = auth_middleware_authenticator_rx.clone();
            async move {
                let authenticator = authenticator_rx
                    .await
                    .expect("sender not dropped before sending once");
                http_auth(req, next, tls_mode, authenticator, allowed_roles).await
            }
        });

        let mut router = Router::new();
        let mut base_router = Router::new();
        if routes_enabled.base {
            base_router = base_router
                .route(
                    "/",
                    routing::get(move || async move {
                        root::handle_home(routes_enabled.profiling).await
                    }),
                )
                .route("/api/sql", routing::post(sql::handle_sql))
                .route("/memory", routing::get(memory::handle_memory))
                .route(
                    "/hierarchical-memory",
                    routing::get(memory::handle_hierarchical_memory),
                )
                .route("/static/*path", routing::get(root::handle_static));

            let mut ws_router = Router::new()
                .route("/api/experimental/sql", routing::get(sql::handle_sql_ws))
                .with_state(WsState {
                    authenticator_rx: authenticator_rx.clone(),
                    adapter_client_rx: adapter_client_rx.clone(),
                    active_connection_counter: active_connection_counter.clone(),
                    helm_chart_version,
                    allowed_roles,
                });
            if let AuthenticatorKind::None = authenticator_kind {
                ws_router = ws_router.layer(middleware::from_fn(x_materialize_user_header_auth));
            }
            router = router.merge(ws_router);
        }
        if routes_enabled.profiling {
            base_router = base_router.nest("/prof/", mz_prof_http::router(&BUILD_INFO));
        }

        if routes_enabled.webhook {
            let webhook_router = Router::new()
                .route(
                    "/api/webhook/:database/:schema/:id",
                    routing::post(webhook::handle_webhook),
                )
                .with_state(WebhookState {
                    adapter_client_rx: adapter_client_rx.clone(),
                    webhook_cache,
                })
                .layer(
                    CorsLayer::new()
                        .allow_methods(Method::POST)
                        .allow_origin(AllowOrigin::mirror_request())
                        .allow_headers(Any),
                )
                .layer(
                    ServiceBuilder::new()
                        .layer(HandleErrorLayer::new(handle_load_error))
                        .load_shed()
                        .layer(GlobalConcurrencyLimitLayer::with_semaphore(
                            concurrent_webhook_req,
                        )),
                );
            router = router.merge(webhook_router);
        }

        if routes_enabled.internal {
            let console_config = Arc::new(console::ConsoleProxyConfig::new(
                internal_route_config.internal_console_redirect_url.clone(),
                "/internal-console".to_string(),
            ));
            base_router = base_router
                .route(
                    "/api/opentelemetry/config",
                    routing::put({
                        move |_: axum::Json<DynamicFilterTarget>| async {
                            (
                                StatusCode::BAD_REQUEST,
                                "This endpoint has been replaced. \
                            Use the `opentelemetry_filter` system variable."
                                    .to_string(),
                            )
                        }
                    }),
                )
                .route(
                    "/api/stderr/config",
                    routing::put({
                        move |_: axum::Json<DynamicFilterTarget>| async {
                            (
                                StatusCode::BAD_REQUEST,
                                "This endpoint has been replaced. \
                            Use the `log_filter` system variable."
                                    .to_string(),
                            )
                        }
                    }),
                )
                .route("/api/tracing", routing::get(mz_http_util::handle_tracing))
                .route(
                    "/api/catalog/dump",
                    routing::get(catalog::handle_catalog_dump),
                )
                .route(
                    "/api/catalog/check",
                    routing::get(catalog::handle_catalog_check),
                )
                .route(
                    "/api/coordinator/check",
                    routing::get(catalog::handle_coordinator_check),
                )
                .route(
                    "/api/coordinator/dump",
                    routing::get(catalog::handle_coordinator_dump),
                )
                .route(
                    "/internal-console",
                    routing::get(|| async { Redirect::temporary("/internal-console/") }),
                )
                .route(
                    "/internal-console/*path",
                    routing::get(console::handle_internal_console),
                )
                .route(
                    "/internal-console/",
                    routing::get(console::handle_internal_console),
                )
                .layer(Extension(console_config));
            let leader_router = Router::new()
                .route("/api/leader/status", routing::get(handle_leader_status))
                .route("/api/leader/promote", routing::post(handle_leader_promote))
                .route(
                    "/api/leader/skip-catchup",
                    routing::post(handle_leader_skip_catchup),
                )
                .layer(auth_middleware.clone())
                .with_state(internal_route_config.deployment_state_handle.clone());
            router = router.merge(leader_router);
        }

        if routes_enabled.metrics {
            let metrics_router = Router::new()
                .route(
                    "/metrics",
                    routing::get(move || async move {
                        mz_http_util::handle_prometheus(&metrics_registry).await
                    }),
                )
                .route(
                    "/metrics/mz_usage",
                    routing::get(|client: AuthedClient| async move {
                        let registry = sql::handle_promsql(client, USAGE_METRIC_QUERIES).await;
                        mz_http_util::handle_prometheus(&registry).await
                    }),
                )
                .route(
                    "/metrics/mz_frontier",
                    routing::get(|client: AuthedClient| async move {
                        let registry = sql::handle_promsql(client, FRONTIER_METRIC_QUERIES).await;
                        mz_http_util::handle_prometheus(&registry).await
                    }),
                )
                .route(
                    "/metrics/mz_compute",
                    routing::get(|client: AuthedClient| async move {
                        let registry = sql::handle_promsql(client, COMPUTE_METRIC_QUERIES).await;
                        mz_http_util::handle_prometheus(&registry).await
                    }),
                )
                .route(
                    "/metrics/mz_storage",
                    routing::get(|client: AuthedClient| async move {
                        let registry = sql::handle_promsql(client, STORAGE_METRIC_QUERIES).await;
                        mz_http_util::handle_prometheus(&registry).await
                    }),
                )
                .route(
                    "/api/livez",
                    routing::get(mz_http_util::handle_liveness_check),
                )
                .route("/api/readyz", routing::get(probe::handle_ready))
                .layer(auth_middleware.clone())
                .layer(Extension(adapter_client_rx.clone()))
                .layer(Extension(active_connection_counter.clone()));
            router = router.merge(metrics_router);
        }

        base_router = base_router
            .layer(auth_middleware.clone())
            .layer(Extension(adapter_client_rx.clone()))
            .layer(Extension(active_connection_counter.clone()))
            .layer(
                CorsLayer::new()
                    .allow_credentials(false)
                    .allow_headers([
                        AUTHORIZATION,
                        CONTENT_TYPE,
                        HeaderName::from_static("x-materialize-version"),
                    ])
                    .allow_methods(Any)
                    .allow_origin(allowed_origin)
                    .expose_headers(Any)
                    .max_age(Duration::from_secs(60) * 60),
            );

        match authenticator_kind {
            AuthenticatorKind::None => {
                base_router = base_router.layer(middleware::from_fn(x_materialize_user_header_auth))
            }
            _ => {}
        }

        router = router
            .merge(base_router)
            .apply_default_layers(source, metrics);

        HttpServer { tls, router }
    }
}

impl Server for HttpServer {
    const NAME: &'static str = "http";

    fn handle_connection(&self, conn: Connection) -> ConnectionHandler {
        let router = self.router.clone();
        let tls_config = self.tls.clone();
        let mut conn = TokioIo::new(conn);
        Box::pin(async {
            let direct_peer_addr = conn.inner().peer_addr().context("fetching peer addr")?;
            let peer_addr = conn
                .inner_mut()
                .take_proxy_header_address()
                .await
                .map(|a| a.source)
                .unwrap_or(direct_peer_addr);

            let (conn, conn_protocol) = match tls_config {
                Some(tls_config) => {
                    let mut ssl_stream =
                        SslStream::new(Ssl::new(&tls_config.context.get())?, conn)?;
                    if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                        let _ = ssl_stream.get_mut().inner_mut().shutdown().await;
                        return Err(e.into());
                    }
                    (MaybeHttpsStream::Https(ssl_stream), ConnProtocol::Https)
                }
                _ => (MaybeHttpsStream::Http(conn), ConnProtocol::Http),
            };
            let mut make_tower_svc = router
                .layer(Extension(conn_protocol))
                .into_make_service_with_connect_info::<SocketAddr>();
            let tower_svc = make_tower_svc.call(peer_addr).await.unwrap();
            let hyper_svc = hyper::service::service_fn(|req| tower_svc.clone().call(req));
            let http = hyper::server::conn::http1::Builder::new();
            http.serve_connection(conn, hyper_svc)
                .with_upgrades()
                .err_into()
                .await
        })
    }
}

pub async fn handle_leader_status(
    State(deployment_state_handle): State<DeploymentStateHandle>,
) -> impl IntoResponse {
    let status = deployment_state_handle.status();
    (StatusCode::OK, Json(json!({ "status": status })))
}

pub async fn handle_leader_promote(
    State(deployment_state_handle): State<DeploymentStateHandle>,
) -> impl IntoResponse {
    match deployment_state_handle.try_promote() {
        Ok(()) => {
            // TODO(benesch): the body here is redundant. Should just return
            // 204.
            let status = StatusCode::OK;
            let body = Json(json!({
                "result": "Success",
            }));
            (status, body)
        }
        Err(()) => {
            // TODO(benesch): the nesting here is redundant given the error
            // code. Should just return the `{"message": "..."}` object.
            let status = StatusCode::BAD_REQUEST;
            let body = Json(json!({
                "result": {"Failure": {"message": "cannot promote leader while initializing"}},
            }));
            (status, body)
        }
    }
}

pub async fn handle_leader_skip_catchup(
    State(deployment_state_handle): State<DeploymentStateHandle>,
) -> impl IntoResponse {
    match deployment_state_handle.try_skip_catchup() {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(()) => {
            let status = StatusCode::BAD_REQUEST;
            let body = Json(json!({
                "message": "cannot skip catchup in this phase of initialization; try again later",
            }));
            (status, body).into_response()
        }
    }
}

async fn x_materialize_user_header_auth(mut req: Request, next: Next) -> impl IntoResponse {
    // TODO integrate this into the single auth flow
    // or migrate teleport to basic auth.
    if let Some(username) = req.headers().get("x-materialize-user").map(|h| h.to_str()) {
        let username = match username {
            Ok(name @ (SUPPORT_USER_NAME | SYSTEM_USER_NAME)) => name.to_string(),
            _ => {
                return Err(AuthError::MismatchedUser(format!(
                    "user specified in x-materialize-user must be {SUPPORT_USER_NAME} or {SYSTEM_USER_NAME}"
                )));
            }
        };
        req.extensions_mut().insert(AuthedUser {
            name: username,
            external_metadata_rx: None,
        });
    }
    Ok(next.run(req).await)
}

type Delayed<T> = Shared<oneshot::Receiver<T>>;

#[derive(Clone)]
enum ConnProtocol {
    Http,
    Https,
}

#[derive(Clone, Debug)]
pub struct AuthedUser {
    name: String,
    external_metadata_rx: Option<watch::Receiver<ExternalUserMetadata>>,
}

pub struct AuthedClient {
    pub client: SessionClient,
    pub connection_guard: Option<ConnectionHandle>,
}

impl AuthedClient {
    async fn new<F>(
        adapter_client: &Client,
        user: AuthedUser,
        peer_addr: IpAddr,
        active_connection_counter: ConnectionCounter,
        helm_chart_version: Option<String>,
        session_config: F,
        options: BTreeMap<String, String>,
        now: NowFn,
    ) -> Result<Self, AdapterError>
    where
        F: FnOnce(&mut Session),
    {
        let conn_id = adapter_client.new_conn_id()?;
        let mut session = adapter_client.new_session(SessionConfig {
            conn_id,
            uuid: epoch_to_uuid_v7(&(now)()),
            user: user.name,
            client_ip: Some(peer_addr),
            external_metadata_rx: user.external_metadata_rx,
            //TODO(dov): Add support for internal user metadata when we support auth here
            internal_user_metadata: None,
            helm_chart_version,
        });
        let connection_guard = active_connection_counter.allocate_connection(session.user())?;

        session_config(&mut session);
        let system_vars = adapter_client.get_system_vars().await;
        for (key, val) in options {
            const LOCAL: bool = false;
            if let Err(err) =
                session
                    .vars_mut()
                    .set(&system_vars, &key, VarInput::Flat(&val), LOCAL)
            {
                session.add_notice(AdapterNotice::BadStartupSetting {
                    name: key.to_string(),
                    reason: err.to_string(),
                })
            }
        }
        let adapter_client = adapter_client.startup(session).await?;
        Ok(AuthedClient {
            client: adapter_client,
            connection_guard,
        })
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthedClient
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        req: &mut http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        #[derive(Debug, Default, Deserialize)]
        struct Params {
            #[serde(default)]
            options: String,
        }
        let params: Query<Params> = Query::from_request_parts(req, state)
            .await
            .unwrap_or_default();

        let peer_addr = req
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .expect("ConnectInfo extension guaranteed to exist")
            .0
            .ip();

        let user = req.extensions.get::<AuthedUser>().unwrap();
        let adapter_client = req
            .extensions
            .get::<Delayed<mz_adapter::Client>>()
            .unwrap()
            .clone();
        let adapter_client = adapter_client.await.map_err(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "adapter client missing").into_response()
        })?;
        let active_connection_counter = req.extensions.get::<ConnectionCounter>().unwrap();
        let helm_chart_version = None;

        let options = if params.options.is_empty() {
            // It's possible 'options' simply wasn't provided, we don't want that to
            // count as a failure to deserialize
            BTreeMap::<String, String>::default()
        } else {
            match serde_json::from_str(&params.options) {
                Ok(options) => options,
                Err(_e) => {
                    // If we fail to deserialize options, fail the request.
                    let code = StatusCode::BAD_REQUEST;
                    let msg = format!("Failed to deserialize {} map", "options".quoted());
                    return Err((code, msg).into_response());
                }
            }
        };

        let client = AuthedClient::new(
            &adapter_client,
            user.clone(),
            peer_addr,
            active_connection_counter.clone(),
            helm_chart_version,
            |session| {
                session
                    .vars_mut()
                    .set_default(WELCOME_MESSAGE.name(), VarInput::Flat(&false.format()))
                    .expect("known to exist")
            },
            options,
            SYSTEM_TIME.clone(),
        )
        .await
        .map_err(|e| {
            let status = match e {
                AdapterError::UserSessionsDisallowed | AdapterError::NetworkPolicyDenied(_) => {
                    StatusCode::FORBIDDEN
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(SqlError::from(e))).into_response()
        })?;

        Ok(client)
    }
}

#[derive(Debug, Error)]
enum AuthError {
    #[error("HTTPS is required")]
    HttpsRequired,
    #[error("invalid username in client certificate")]
    InvalidLogin(String),
    #[error("{0}")]
    Frontegg(#[from] FronteggError),
    #[error("missing authorization header")]
    MissingHttpAuthentication,
    #[error("{0}")]
    MismatchedUser(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        warn!("HTTP request failed authentication: {}", self);
        // We omit most detail from the error message we send to the client, to
        // avoid giving attackers unnecessary information.
        let message = match self {
            AuthError::HttpsRequired => self.to_string(),
            _ => "unauthorized".into(),
        };
        (
            StatusCode::UNAUTHORIZED,
            [(http::header::WWW_AUTHENTICATE, "Basic realm=Materialize")],
            message,
        )
            .into_response()
    }
}

async fn http_auth(
    mut req: Request,
    next: Next,
    tls_mode: TlsMode,
    authenticator: Arc<Authenticator>,
    allowed_roles: AllowedRoles,
) -> impl IntoResponse + use<> {
    // First, extract the username from the certificate, validating that the
    // connection matches the TLS configuration along the way.
    let conn_protocol = req.extensions().get::<ConnProtocol>().unwrap();
    match (tls_mode, &conn_protocol) {
        (TlsMode::Disable, ConnProtocol::Http) => {}
        (TlsMode::Disable, ConnProtocol::Https { .. }) => unreachable!(),
        (TlsMode::Require, ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (TlsMode::Require, ConnProtocol::Https { .. }) => {}
    }
    // If we've already passed some other auth, just use that.
    if req.extensions().get::<AuthedUser>().is_some() {
        return Ok(next.run(req).await);
    }
    let creds = if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
        Some(Credentials::Password {
            username: basic.username().to_owned(),
            password: Password(basic.password().to_owned()),
        })
    } else if let Some(bearer) = req.headers().typed_get::<Authorization<Bearer>>() {
        Some(Credentials::Token {
            token: bearer.token().to_owned(),
        })
    } else {
        None
    };

    let user = auth(&authenticator, creds, allowed_roles).await?;

    // Add the authenticated user as an extension so downstream handlers can
    // inspect it if necessary.
    req.extensions_mut().insert(user);

    // Run the request.
    Ok(next.run(req).await)
}

async fn init_ws(
    WsState {
        authenticator_rx,
        adapter_client_rx,
        active_connection_counter,
        helm_chart_version,
        allowed_roles,
    }: &WsState,
    existing_user: Option<AuthedUser>,
    peer_addr: IpAddr,
    ws: &mut WebSocket,
) -> Result<AuthedClient, anyhow::Error> {
    let authenticator = authenticator_rx.clone().await.expect("sender not dropped");
    // TODO: Add a timeout here to prevent resource leaks by clients that
    // connect then never send a message.
    let init_msg = ws.recv().await.ok_or_else(|| anyhow::anyhow!("closed"))??;
    let ws_auth: WebSocketAuth = loop {
        match init_msg {
            Message::Text(data) => break serde_json::from_str(&data)?,
            Message::Binary(data) => break serde_json::from_slice(&data)?,
            // Handled automatically by the server.
            Message::Ping(_) => {
                continue;
            }
            Message::Pong(_) => {
                continue;
            }
            Message::Close(_) => {
                anyhow::bail!("closed");
            }
        }
    };

    let (user, options) = if let Some(existing_user) = existing_user {
        match ws_auth {
            WebSocketAuth::OptionsOnly { options } => (existing_user, options),
            _ => {
                warn!("Unexpected bearer or basic auth provided when using user header");
                anyhow::bail!("unexpected")
            }
        }
    } else {
        let (creds, options) = match ws_auth {
            WebSocketAuth::Basic {
                user,
                password,
                options,
            } => {
                let creds = Credentials::Password {
                    username: user,
                    password,
                };
                (creds, options)
            }
            WebSocketAuth::Bearer { token, options } => {
                let creds = Credentials::Token { token };
                (creds, options)
            }
            WebSocketAuth::OptionsOnly { .. } => {
                anyhow::bail!("expected auth information");
            }
        };
        let user = auth(&authenticator, Some(creds), *allowed_roles).await?;
        (user, options)
    };

    let client = AuthedClient::new(
        &adapter_client_rx.clone().await?,
        user,
        peer_addr,
        active_connection_counter.clone(),
        helm_chart_version.clone(),
        |_session| (),
        options,
        SYSTEM_TIME.clone(),
    )
    .await?;

    Ok(client)
}

enum Credentials {
    Password {
        username: String,
        password: Password,
    },
    Token {
        token: String,
    },
}

async fn auth(
    authenticator: &Authenticator,
    creds: Option<Credentials>,
    allowed_roles: AllowedRoles,
) -> Result<AuthedUser, AuthError> {
    let (name, external_metadata_rx) = match authenticator {
        Authenticator::Frontegg(frontegg) => {
            warn!("frontegg auth");
            match creds {
                Some(Credentials::Password { username, password }) => {
                    let auth_session = frontegg.authenticate(&username, &password.0).await?;
                    let name = auth_session.user().into();
                    let external_metadata_rx = Some(auth_session.external_metadata_rx());
                    (name, external_metadata_rx)
                }
                Some(Credentials::Token { token }) => {
                    let claims = frontegg.validate_access_token(&token, None)?;
                    let (_, external_metadata_rx) = watch::channel(ExternalUserMetadata {
                        user_id: claims.user_id,
                        admin: claims.is_admin,
                    });
                    (claims.user, Some(external_metadata_rx))
                }
                None => return Err(AuthError::MissingHttpAuthentication),
            }
        }
        Authenticator::None => {
            warn!("no auth");
            // If no authentication, use whatever is in the HTTP auth
            // header (without checking the password), or fall back to the
            // default user.
            let name = match creds {
                Some(Credentials::Password { username, .. }) => username,
                _ => HTTP_DEFAULT_USER.name.to_owned(),
            };
            (name, None)
        }
    };

    // TODO move this somewhere it can be shared with PGWIRE
    let is_internal_user = INTERNAL_USER_NAMES.contains(&name);
    // this is a superset of internal users
    let is_reserved_user = mz_adapter::catalog::is_reserved_role_name(name.as_str());
    let role_allowed = match allowed_roles {
        AllowedRoles::Normal => !is_reserved_user,
        AllowedRoles::Internal => is_internal_user,
        AllowedRoles::NormalAndInternal => !is_reserved_user || is_internal_user,
    };
    if !role_allowed {
        return Err(AuthError::InvalidLogin(name));
    }

    Ok(AuthedUser {
        name,
        external_metadata_rx,
    })
}

/// Default layers that should be applied to all routes, and should get applied to both the
/// internal http and external http routers.
trait DefaultLayers {
    fn apply_default_layers(self, source: &'static str, metrics: Metrics) -> Self;
}

impl DefaultLayers for Router {
    fn apply_default_layers(self, source: &'static str, metrics: Metrics) -> Self {
        self.layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(metrics::PrometheusLayer::new(source, metrics))
    }
}

/// Glue code to make [`tower`] work with [`axum`].
///
/// `axum` requires `Layer`s not return Errors, i.e. they must be `Result<_, Infallible>`,
/// instead you must return a type that can be converted into a response. `tower` on the other
/// hand does return Errors, so to make the two work together we need to convert our `tower` errors
/// into responses.
async fn handle_load_error(error: tower::BoxError) -> impl IntoResponse {
    if error.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Cow::from("too many requests, try again later"),
        );
    }

    // Note: This should be unreachable because at the time of writing our only use case is a
    // layer that emits `tower::load_shed::error::Overloaded`, which is handled above.
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("Unhandled internal error: {}", error)),
    )
}
