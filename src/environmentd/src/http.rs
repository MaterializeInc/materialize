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
use axum::{routing, Extension, Json, Router};
use futures::future::{FutureExt, Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Method, StatusCode};
use hyper_openssl::client::legacy::MaybeHttpsStream;
use hyper_openssl::SslStream;
use hyper_util::rt::TokioIo;
use mz_adapter::session::{Session, SessionConfig};
use mz_adapter::{AdapterError, AdapterNotice, Client, SessionClient, WebhookAppenderCache};
use mz_frontegg_auth::{Authenticator as FronteggAuthentication, Error as FronteggError};
use mz_http_util::DynamicFilterTarget;
use mz_ore::cast::u64_to_usize;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::str::StrExt;
use mz_pgwire_common::{ConnectionCounter, ConnectionHandle};
use mz_repr::user::ExternalUserMetadata;
use mz_server_core::{Connection, ConnectionHandler, ReloadingSslContext, Server};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::{HTTP_DEFAULT_USER, SUPPORT_USER_NAME, SYSTEM_USER_NAME};
use mz_sql::session::vars::{Value, Var, VarInput, WELCOME_MESSAGE};
use openssl::ssl::Ssl;
use prometheus::{
    COMPUTE_METRIC_QUERIES, FRONTIER_METRIC_QUERIES, STORAGE_METRIC_QUERIES, USAGE_METRIC_QUERIES,
};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, watch};
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::{Service, ServiceBuilder};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};
use uuid::Uuid;

use crate::deployment::state::DeploymentStateHandle;
use crate::http::sql::SqlError;
use crate::BUILD_INFO;

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
    pub frontegg: Option<FronteggAuthentication>,
    pub adapter_client: mz_adapter::Client,
    pub allowed_origin: AllowOrigin,
    pub active_connection_counter: ConnectionCounter,
    pub helm_chart_version: Option<String>,
    pub concurrent_webhook_req: Arc<tokio::sync::Semaphore>,
    pub metrics: Metrics,
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
    frontegg: Arc<Option<FronteggAuthentication>>,
    adapter_client_rx: Delayed<mz_adapter::Client>,
    active_connection_counter: ConnectionCounter,
    helm_chart_version: Option<String>,
}

#[derive(Clone)]
pub struct WebhookState {
    adapter_client: mz_adapter::Client,
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
            frontegg,
            adapter_client,
            allowed_origin,
            active_connection_counter,
            helm_chart_version,
            concurrent_webhook_req,
            metrics,
        }: HttpConfig,
    ) -> HttpServer {
        let tls_mode = tls.as_ref().map(|tls| tls.mode).unwrap_or(TlsMode::Disable);
        let frontegg = Arc::new(frontegg);
        let base_frontegg = Arc::clone(&frontegg);
        let (adapter_client_tx, adapter_client_rx) = oneshot::channel();
        adapter_client_tx
            .send(adapter_client.clone())
            .expect("rx known to be live");
        let adapter_client_rx = adapter_client_rx.shared();
        let webhook_cache = WebhookAppenderCache::new();

        let base_router = base_router(BaseRouterConfig { profiling: false })
            .layer(middleware::from_fn(move |req, next| {
                let base_frontegg = Arc::clone(&base_frontegg);
                async move { http_auth(req, next, tls_mode, base_frontegg.as_ref().as_ref()).await }
            }))
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
        let ws_router = Router::new()
            .route("/api/experimental/sql", routing::get(sql::handle_sql_ws))
            .with_state(WsState {
                frontegg,
                adapter_client_rx,
                active_connection_counter,
                helm_chart_version,
            });

        let webhook_router = Router::new()
            .route(
                "/api/webhook/:database/:schema/:id",
                routing::post(webhook::handle_webhook),
            )
            .with_state(WebhookState {
                adapter_client,
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

        let router = Router::new()
            .merge(base_router)
            .merge(ws_router)
            .merge(webhook_router)
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

pub struct InternalHttpConfig {
    pub metrics_registry: MetricsRegistry,
    pub adapter_client_rx: oneshot::Receiver<mz_adapter::Client>,
    pub active_connection_counter: ConnectionCounter,
    pub helm_chart_version: Option<String>,
    pub deployment_state_handle: DeploymentStateHandle,
    pub internal_console_redirect_url: Option<String>,
}

pub struct InternalHttpServer {
    router: Router,
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

impl InternalHttpServer {
    pub fn new(
        InternalHttpConfig {
            metrics_registry,
            adapter_client_rx,
            active_connection_counter,
            helm_chart_version,
            deployment_state_handle,
            internal_console_redirect_url,
        }: InternalHttpConfig,
    ) -> InternalHttpServer {
        let metrics = Metrics::register_into(&metrics_registry, "mz_internal_http");
        let console_config = Arc::new(console::ConsoleProxyConfig::new(
            internal_console_redirect_url,
            "/internal-console".to_string(),
        ));

        let adapter_client_rx = adapter_client_rx.shared();
        let router = base_router(BaseRouterConfig { profiling: true })
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
            .layer(middleware::from_fn(internal_http_auth))
            .layer(Extension(adapter_client_rx.clone()))
            .layer(Extension(console_config))
            .layer(Extension(active_connection_counter.clone()));

        let ws_router = Router::new()
            .route("/api/experimental/sql", routing::get(sql::handle_sql_ws))
            // This middleware extracts the MZ user from the x-materialize-user http header.
            // Normally, browser-initiated websocket requests do not support headers, however for the
            // Internal HTTP Server the browser would be connecting through teleport, which should
            // attach the x-materialize-user header to all requests it proxies to this api.
            .layer(middleware::from_fn(internal_http_auth))
            .with_state(WsState {
                frontegg: Arc::new(None),
                adapter_client_rx,
                active_connection_counter,
                helm_chart_version: helm_chart_version.clone(),
            });

        let leader_router = Router::new()
            .route("/api/leader/status", routing::get(handle_leader_status))
            .route("/api/leader/promote", routing::post(handle_leader_promote))
            .route(
                "/api/leader/skip-catchup",
                routing::post(handle_leader_skip_catchup),
            )
            .with_state(deployment_state_handle);

        let router = router
            .merge(ws_router)
            .merge(leader_router)
            .apply_default_layers("internal", metrics);

        InternalHttpServer { router }
    }
}

async fn internal_http_auth(mut req: Request, next: Next) -> impl IntoResponse {
    let user_name = req
        .headers()
        .get("x-materialize-user")
        .map(|h| h.to_str())
        .unwrap_or_else(|| Ok(SYSTEM_USER_NAME));
    let user_name = match user_name {
        Ok(name @ (SUPPORT_USER_NAME | SYSTEM_USER_NAME)) => name.to_string(),
        _ => {
            return Err(AuthError::MismatchedUser(format!(
            "user specified in x-materialize-user must be {SUPPORT_USER_NAME} or {SYSTEM_USER_NAME}"
        )));
        }
    };
    req.extensions_mut().insert(AuthedUser {
        name: user_name,
        external_metadata_rx: None,
    });
    Ok(next.run(req).await)
}

#[async_trait]
impl Server for InternalHttpServer {
    const NAME: &'static str = "internal_http";

    fn handle_connection(&self, mut conn: Connection) -> ConnectionHandler {
        let router = self.router.clone();

        Box::pin(async {
            let mut make_tower_svc = router.into_make_service_with_connect_info::<SocketAddr>();
            let direct_peer_addr = conn.peer_addr().context("fetching peer addr")?;
            let peer_addr = conn
                .take_proxy_header_address()
                .await
                .map(|a| a.source)
                .unwrap_or(direct_peer_addr);
            let tower_svc = make_tower_svc.call(peer_addr).await?;
            let service = hyper::service::service_fn(|req| tower_svc.clone().call(req));
            let http = hyper::server::conn::http1::Builder::new();
            http.serve_connection(TokioIo::new(conn), service)
                .with_upgrades()
                .err_into()
                .await
        })
    }
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
    ) -> Result<Self, AdapterError>
    where
        F: FnOnce(&mut Session),
    {
        let conn_id = adapter_client.new_conn_id()?;
        let mut session = adapter_client.new_session(SessionConfig {
            conn_id,
            uuid: Uuid::new_v4(),
            user: user.name,
            client_ip: Some(peer_addr),
            external_metadata_rx: user.external_metadata_rx,
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
    #[error("unexpected credentials")]
    UnexpectedCredentials,
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
    frontegg: Option<&FronteggAuthentication>,
) -> impl IntoResponse {
    // First, extract the username from the certificate, validating that the
    // connection matches the TLS configuration along the way.
    let conn_protocol = req.extensions().get::<ConnProtocol>().unwrap();
    match (tls_mode, &conn_protocol) {
        (TlsMode::Disable, ConnProtocol::Http) => {}
        (TlsMode::Disable, ConnProtocol::Https { .. }) => unreachable!(),
        (TlsMode::Require, ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (TlsMode::Require, ConnProtocol::Https { .. }) => {}
    }
    let creds = match frontegg {
        None => {
            // If no Frontegg authentication, use whatever is in the HTTP auth
            // header (without checking the password), or fall back to the
            // default user.
            if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
                Credentials::User(basic.username().to_string())
            } else {
                Credentials::DefaultUser
            }
        }
        Some(_) => {
            if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
                Credentials::Password {
                    username: basic.username().to_string(),
                    password: basic.password().to_string(),
                }
            } else if let Some(bearer) = req.headers().typed_get::<Authorization<Bearer>>() {
                Credentials::Token {
                    token: bearer.token().to_string(),
                }
            } else {
                return Err(AuthError::MissingHttpAuthentication);
            }
        }
    };

    let user = auth(frontegg, creds).await?;

    // Add the authenticated user as an extension so downstream handlers can
    // inspect it if necessary.
    req.extensions_mut().insert(user);

    // Run the request.
    Ok(next.run(req).await)
}

async fn init_ws(
    WsState {
        frontegg,
        adapter_client_rx,
        active_connection_counter,
        helm_chart_version,
    }: &WsState,
    existing_user: Option<AuthedUser>,
    peer_addr: IpAddr,
    ws: &mut WebSocket,
) -> Result<AuthedClient, anyhow::Error> {
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
    let (user, options) = match (frontegg.as_ref(), existing_user, ws_auth) {
        (Some(frontegg), None, ws_auth) => {
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
                WebSocketAuth::OptionsOnly { options: _ } => {
                    anyhow::bail!("expected auth information");
                }
            };
            (auth(Some(frontegg), creds).await?, options)
        }
        (
            None,
            None,
            WebSocketAuth::Basic {
                user,
                password: _,
                options,
            },
        ) => (auth(None, Credentials::User(user)).await?, options),
        // No frontegg, specified existing user, we only accept options only.
        (None, Some(existing_user), WebSocketAuth::OptionsOnly { options }) => {
            (existing_user, options)
        }
        // No frontegg, specified existing user, we do not expect basic or bearer auth.
        (None, Some(_), WebSocketAuth::Basic { .. } | WebSocketAuth::Bearer { .. }) => {
            warn!("Unexpected bearer or basic auth provided when using user header");
            anyhow::bail!("unexpected")
        }
        // Specifying both frontegg and an existing user should not be possible.
        (Some(_), Some(_), _) => anyhow::bail!("unexpected"),
        // No frontegg, no existing user, and no passed username.
        (None, None, WebSocketAuth::Bearer { .. } | WebSocketAuth::OptionsOnly { .. }) => {
            warn!("Unexpected auth type when not using frontegg or user header");
            anyhow::bail!("unexpected")
        }
    };

    let client = AuthedClient::new(
        &adapter_client_rx.clone().await?,
        user,
        peer_addr,
        active_connection_counter.clone(),
        helm_chart_version.clone(),
        |_session| (),
        options,
    )
    .await?;

    Ok(client)
}

enum Credentials {
    User(String),
    DefaultUser,
    Password { username: String, password: String },
    Token { token: String },
}

async fn auth(
    frontegg: Option<&FronteggAuthentication>,
    creds: Credentials,
) -> Result<AuthedUser, AuthError> {
    // There are three places a username may be specified:
    //
    //   - certificate common name
    //   - HTTP Basic authentication
    //   - JWT email address
    //
    // We verify that if any of these are present, they must match any other
    // that is also present.

    // Then, handle Frontegg authentication if required.
    let (name, external_metadata_rx) = match (frontegg, creds) {
        // If no Frontegg authentication, allow the default user.
        (None, Credentials::DefaultUser) => (HTTP_DEFAULT_USER.name.to_string(), None),
        // If no Frontegg authentication, allow a protocol-specified user.
        (None, Credentials::User(name)) => (name, None),
        // With frontegg disabled, specifying credentials is an error.
        (None, _) => return Err(AuthError::UnexpectedCredentials),
        // If we require Frontegg auth, fetch credentials from the HTTP auth
        // header. Basic auth comes with a username/password, where the password
        // is the client+secret pair. Bearer auth is an existing JWT that must
        // be validated. In either case, if a username was specified in the
        // client cert, it must match that of the JWT.
        (Some(frontegg), creds) => match creds {
            Credentials::Password { username, password } => {
                let auth_session = frontegg.authenticate(&username, &password).await?;
                let user = auth_session.user().into();
                let external_metadata_rx = Some(auth_session.external_metadata_rx());
                (user, external_metadata_rx)
            }
            Credentials::Token { token } => {
                let claims = frontegg.validate_access_token(&token, None)?;
                let (_, external_metadata_rx) = watch::channel(ExternalUserMetadata {
                    user_id: claims.user_id,
                    admin: claims.is_admin,
                });
                (claims.user, Some(external_metadata_rx))
            }
            Credentials::DefaultUser | Credentials::User(_) => {
                return Err(AuthError::MissingHttpAuthentication)
            }
        },
    };

    if mz_adapter::catalog::is_reserved_role_name(name.as_str()) {
        return Err(AuthError::InvalidLogin(name));
    }
    Ok(AuthedUser {
        name,
        external_metadata_rx,
    })
}

/// Configuration for [`base_router`].
struct BaseRouterConfig {
    /// Whether to enable the profiling routes.
    profiling: bool,
}

/// Returns the router for routes that are shared between the internal and
/// external HTTP servers.
fn base_router(BaseRouterConfig { profiling }: BaseRouterConfig) -> Router {
    // Adding a layer with in this function will only apply to the routes defined in this function.
    // https://docs.rs/axum/0.6.1/axum/routing/struct.Router.html#method.layer
    let mut router = Router::new()
        .route(
            "/",
            routing::get(move || async move { root::handle_home(profiling).await }),
        )
        .route("/api/sql", routing::post(sql::handle_sql))
        .route("/memory", routing::get(memory::handle_memory))
        .route(
            "/hierarchical-memory",
            routing::get(memory::handle_hierarchical_memory),
        )
        .route("/static/*path", routing::get(root::handle_static));

    if profiling {
        router = router.nest("/prof/", mz_prof_http::router(&BUILD_INFO));
    }

    router
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
