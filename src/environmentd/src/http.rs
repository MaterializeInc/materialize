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
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::error_handling::HandleErrorLayer;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{DefaultBodyLimit, FromRequestParts, Query, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::{routing, Extension, Json, Router};
use futures::future::{FutureExt, Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Method, Request, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use mz_adapter::session::{Session, SessionConfig};
use mz_adapter::{AdapterError, AdapterNotice, Client, SessionClient, WebhookAppenderCache};
use mz_frontegg_auth::{Authenticator as FronteggAuthentication, Error as FronteggError};
use mz_http_util::DynamicFilterTarget;
use mz_ore::cast::u64_to_usize;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::str::StrExt;
use mz_repr::user::ExternalUserMetadata;
use mz_server_core::{ConnectionHandler, Server};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::user::{HTTP_DEFAULT_USER, SUPPORT_USER_NAME, SYSTEM_USER_NAME};
use mz_sql::session::vars::{
    ConnectionCounter, DropConnection, Value, Var, VarInput, WELCOME_MESSAGE,
};
use openssl::ssl::{Ssl, SslContext};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, watch};
use tokio_openssl::SslStream;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use crate::BUILD_INFO;

mod catalog;
mod console;
mod memory;
mod metrics;
mod probe;
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
    pub tls: Option<TlsConfig>,
    pub frontegg: Option<FronteggAuthentication>,
    pub adapter_client: mz_adapter::Client,
    pub allowed_origin: AllowOrigin,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
    pub concurrent_webhook_req: Arc<tokio::sync::Semaphore>,
    pub metrics: Metrics,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub context: SslContext,
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
    active_connection_count: SharedConnectionCounter,
}

#[derive(Clone)]
pub struct WebhookState {
    adapter_client: mz_adapter::Client,
    webhook_cache: WebhookAppenderCache,
}

#[derive(Debug)]
pub struct HttpServer {
    tls: Option<TlsConfig>,
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
            active_connection_count,
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
            .layer(Extension(Arc::clone(&active_connection_count)))
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
                active_connection_count,
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

    fn tls_context(&self) -> Option<&SslContext> {
        self.tls.as_ref().map(|tls| &tls.context)
    }
}

impl Server for HttpServer {
    const NAME: &'static str = "http";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        let router = self.router.clone();
        let tls_context = self.tls_context().cloned();
        Box::pin(async {
            let (conn, conn_protocol) = match tls_context {
                Some(tls_context) => {
                    let mut ssl_stream = SslStream::new(Ssl::new(&tls_context)?, conn)?;
                    if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                        let _ = ssl_stream.get_mut().shutdown().await;
                        return Err(e.into());
                    }
                    (MaybeHttpsStream::Https(ssl_stream), ConnProtocol::Https)
                }
                _ => (MaybeHttpsStream::Http(conn), ConnProtocol::Http),
            };
            let svc = router.layer(Extension(conn_protocol));
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, svc)
                .with_upgrades()
                .err_into()
                .await
        })
    }
}

pub struct InternalHttpConfig {
    pub metrics_registry: MetricsRegistry,
    pub adapter_client_rx: oneshot::Receiver<mz_adapter::Client>,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
    pub promote_leader: oneshot::Sender<()>,
    pub ready_to_promote: oneshot::Receiver<()>,
    pub internal_console_redirect_url: Option<String>,
}

pub struct InternalHttpServer {
    router: Router,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderStatusResponse {
    pub status: LeaderStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LeaderStatus {
    /// The current leader returns this. It shouldn't need to know that another instance is attempting to update it.
    IsLeader,
    /// The new pod should return this status until it is ready to become the leader, or it has determined that it cannot proceed.
    Initializing,
    /// Once we receive this status, we can tell it to become the leader and migrate the EIP.
    ReadyToPromote,
}

#[derive(Debug)]
pub enum LeaderState {
    IsLeader,
    Initializing {
        // Invariant: promote_leader is Some in Initializing and ReadyToPromote: we need
        // to be able to move them from one state to the other and to access it by value
        // without the fiddly work of moving the state in and out of LeaderState mutex.
        promote_leader: Option<oneshot::Sender<()>>,
        ready_to_promote: oneshot::Receiver<()>,
    },
    ReadyToPromote {
        // Same invariant as Initializing
        promote_leader: Option<oneshot::Sender<()>>,
    },
}

fn state_to_status(state: &LeaderState) -> LeaderStatus {
    match state {
        LeaderState::IsLeader => LeaderStatus::IsLeader,
        LeaderState::Initializing { .. } => LeaderStatus::Initializing,
        LeaderState::ReadyToPromote { .. } => LeaderStatus::ReadyToPromote,
    }
}

pub async fn handle_leader_status(
    State(state): State<Arc<Mutex<LeaderState>>>,
) -> impl IntoResponse {
    let mut leader_state = state.lock().expect("lock poisoned");
    match &mut *leader_state {
        LeaderState::IsLeader => (),
        LeaderState::Initializing {
            promote_leader,
            ready_to_promote,
        } => {
            match ready_to_promote.try_recv() {
                Ok(_) => {
                    assert!(promote_leader.is_some(), "invariant");
                    *leader_state = LeaderState::ReadyToPromote {
                        promote_leader: promote_leader.take(),
                    };
                }
                Err(TryRecvError::Empty) => {
                    // Continue waiting.
                }
                Err(TryRecvError::Closed) => {
                    *leader_state = LeaderState::IsLeader; // server has started, it is the leader now
                }
            }
        }
        LeaderState::ReadyToPromote { .. } => (),
    }
    let status = state_to_status(&leader_state);
    drop(leader_state);
    (
        StatusCode::OK,
        Json(serde_json::json!(LeaderStatusResponse { status })),
    )
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BecomeLeaderResponse {
    pub result: BecomeLeaderResult,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum BecomeLeaderResult {
    Success, // 200 http status: also return this if we are already the leader
    Failure {
        // 500 http status if called when not `ReadyToPromote`.
        message: String,
    },
}

pub async fn handle_leader_promote(
    State(state): State<Arc<Mutex<LeaderState>>>,
) -> impl IntoResponse {
    let mut leader_state = state.lock().expect("lock poisoned");

    match &mut *leader_state {
        LeaderState::IsLeader => (),
        LeaderState::Initializing { .. } => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!(BecomeLeaderResult::Failure {
                    message: "Not ready to promote, still initializing".into(),
                })),
            );
        }
        LeaderState::ReadyToPromote { promote_leader } => {
            // even if send fails it means the server has started and we're already the leader
            let _ = promote_leader.take().expect("invariant").send(());
        }
    }
    // We're either already the leader or should be if we reach this.
    *leader_state = LeaderState::IsLeader;
    drop(leader_state);
    (
        StatusCode::OK,
        Json(serde_json::json!(BecomeLeaderResponse {
            result: BecomeLeaderResult::Success,
        })),
    )
}

impl InternalHttpServer {
    pub fn new(
        InternalHttpConfig {
            metrics_registry,
            adapter_client_rx,
            active_connection_count,
            promote_leader,
            ready_to_promote,
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
            .layer(Extension(Arc::clone(&active_connection_count)));

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
                active_connection_count,
            });

        let leader_router = Router::new()
            .route("/api/leader/status", routing::get(handle_leader_status))
            .route("/api/leader/promote", routing::post(handle_leader_promote))
            .with_state(Arc::new(Mutex::new(LeaderState::Initializing {
                promote_leader: Some(promote_leader),
                ready_to_promote,
            })));

        let router = router
            .merge(ws_router)
            .merge(leader_router)
            .apply_default_layers("internal", metrics);

        InternalHttpServer { router }
    }
}

async fn internal_http_auth<B>(mut req: Request<B>, next: Next<B>) -> impl IntoResponse {
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

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        let router = self.router.clone();
        Box::pin(async {
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, router)
                .with_upgrades()
                .err_into()
                .await
        })
    }
}

type Delayed<T> = Shared<oneshot::Receiver<T>>;

type SharedConnectionCounter = Arc<Mutex<ConnectionCounter>>;

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
    pub drop_connection: Option<DropConnection>,
}

impl AuthedClient {
    async fn new<F>(
        adapter_client: &Client,
        user: AuthedUser,
        active_connection_count: SharedConnectionCounter,
        session_config: F,
        options: BTreeMap<String, String>,
    ) -> Result<Self, AdapterError>
    where
        F: FnOnce(&mut Session),
    {
        let conn_id = adapter_client.new_conn_id()?;
        let mut session = adapter_client.new_session(SessionConfig {
            conn_id,
            user: user.name,
            external_metadata_rx: user.external_metadata_rx,
        });
        let drop_connection =
            DropConnection::new_connection(session.user(), active_connection_count)?;
        session_config(&mut session);
        for (key, val) in options {
            const LOCAL: bool = false;
            if let Err(err) = session
                .vars_mut()
                .set(None, &key, VarInput::Flat(&val), LOCAL)
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
            drop_connection,
        })
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthedClient
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

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

        let user = req.extensions.get::<AuthedUser>().unwrap();
        let adapter_client = req
            .extensions
            .get::<Delayed<mz_adapter::Client>>()
            .unwrap()
            .clone();
        let adapter_client = adapter_client.await.map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "adapter client missing".into(),
            )
        })?;
        let active_connection_count = req.extensions.get::<SharedConnectionCounter>().unwrap();

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
                    return Err((code, msg));
                }
            }
        };

        let client = AuthedClient::new(
            &adapter_client,
            user.clone(),
            Arc::clone(active_connection_count),
            |session| {
                session
                    .vars_mut()
                    .set_default(WELCOME_MESSAGE.name(), VarInput::Flat(&false.format()))
                    .expect("known to exist")
            },
            options,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

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

async fn http_auth<B>(
    mut req: Request<B>,
    next: Next<B>,
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
        // If no Frontegg authentication, use the default HTTP user.
        None => Credentials::DefaultUser,
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
        active_connection_count,
    }: &WsState,
    existing_user: Option<AuthedUser>,
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
        Arc::clone(active_connection_count),
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
                (claims.email, Some(external_metadata_rx))
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
