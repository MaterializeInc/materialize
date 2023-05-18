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

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{DefaultBodyLimit, FromRequestParts, Query};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing, Extension, Router};
use futures::future::{FutureExt, Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Request, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use mz_adapter::{AdapterError, AdapterNotice, Client, SessionClient};
use mz_frontegg_auth::{Authentication as FronteggAuthentication, Error as FronteggError};
use mz_ore::cast::u64_to_usize;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::str::StrExt;
use mz_ore::tracing::TracingHandle;
use mz_sql::session::user::{ExternalUserMetadata, User, HTTP_DEFAULT_USER, SYSTEM_USER};
use mz_sql::session::vars::{ConnectionCounter, VarInput};
use openssl::ssl::{Ssl, SslContext};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_openssl::SslStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use crate::server::{ConnectionHandler, Server};
use crate::BUILD_INFO;

mod catalog;
mod memory;
mod probe;
mod root;
mod sql;

pub use sql::{SqlResponse, WebSocketAuth, WebSocketResponse};

/// Maximum allowed size for a request.
pub const MAX_REQUEST_SIZE: usize = u64_to_usize(2 * bytesize::MB);

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub tls: Option<TlsConfig>,
    pub frontegg: Option<FronteggAuthentication>,
    pub adapter_client: mz_adapter::Client,
    pub allowed_origin: AllowOrigin,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
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
    adapter_client: mz_adapter::Client,
    active_connection_count: SharedConnectionCounter,
}

#[derive(Debug)]
pub struct HttpServer {
    tls: Option<TlsConfig>,
    router: Router,
}

impl HttpServer {
    pub fn new(
        HttpConfig {
            tls,
            frontegg,
            adapter_client,
            allowed_origin,
            active_connection_count,
        }: HttpConfig,
    ) -> HttpServer {
        let tls_mode = tls.as_ref().map(|tls| tls.mode).unwrap_or(TlsMode::Disable);
        let frontegg = Arc::new(frontegg);
        let base_frontegg = Arc::clone(&frontegg);
        let (adapter_client_tx, adapter_client_rx) = oneshot::channel();
        adapter_client_tx
            .send(adapter_client.clone())
            .expect("rx known to be live");

        let base_router = base_router(BaseRouterConfig { profiling: false })
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(middleware::from_fn(move |req, next| {
                let base_frontegg = Arc::clone(&base_frontegg);
                async move { http_auth(req, next, tls_mode, &base_frontegg).await }
            }))
            .layer(Extension(adapter_client_rx.shared()))
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
                adapter_client,
                active_connection_count,
            });
        let router = Router::new().merge(base_router).merge(ws_router);
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
    pub tracing_handle: TracingHandle,
    pub adapter_client_rx: oneshot::Receiver<mz_adapter::Client>,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
}

pub struct InternalHttpServer {
    router: Router,
}

impl InternalHttpServer {
    pub fn new(
        InternalHttpConfig {
            metrics_registry,
            tracing_handle,
            adapter_client_rx,
            active_connection_count,
        }: InternalHttpConfig,
    ) -> InternalHttpServer {
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
                    let tracing_handle = tracing_handle.clone();
                    move |payload| async move {
                        mz_http_util::handle_reload_tracing_filter(
                            &tracing_handle,
                            TracingHandle::reload_opentelemetry_filter,
                            payload,
                        )
                        .await
                    }
                }),
            )
            .route(
                "/api/stderr/config",
                routing::put({
                    move |payload| async move {
                        mz_http_util::handle_reload_tracing_filter(
                            &tracing_handle,
                            TracingHandle::reload_stderr_log_filter,
                            payload,
                        )
                        .await
                    }
                }),
            )
            .route("/api/tracing", routing::get(mz_http_util::handle_tracing))
            .route(
                "/api/catalog",
                routing::get(catalog::handle_internal_catalog),
            )
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(Extension(AuthedUser(SYSTEM_USER.clone())))
            .layer(Extension(adapter_client_rx.shared()))
            .layer(Extension(active_connection_count));

        InternalHttpServer { router }
    }
}

#[async_trait]
impl Server for InternalHttpServer {
    const NAME: &'static str = "internal_http";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        let router = self.router.clone();
        Box::pin(async {
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, router).err_into().await
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
struct AuthedUser(User);

pub struct AuthedClient {
    pub client: SessionClient,
    pub drop_connection: Option<DropConnection>,
}

pub struct DropConnection {
    pub active_connection_count: SharedConnectionCounter,
}

impl Drop for DropConnection {
    fn drop(&mut self) {
        let mut connections = self.active_connection_count.lock().expect("lock poisoned");
        assert_ne!(connections.current, 0);
        tracing::info!("dec connection count in http");
        connections.current -= 1;
    }
}

impl AuthedClient {
    async fn new(
        adapter_client: &Client,
        user: AuthedUser,
        active_connection_count: SharedConnectionCounter,
    ) -> Result<Self, AdapterError> {
        let AuthedUser(user) = user;
        let drop_connection = if !user.is_internal() && !user.is_external_admin() {
            let connections = {
                let mut connections = active_connection_count.lock().expect("lock poisoned");
                connections.current += 1;
                tracing::info!("inc connection count in http");
                *connections
            };
            let guard = DropConnection {
                active_connection_count,
            };
            if connections.current > connections.limit {
                return Err(AdapterError::ResourceExhaustion {
                    limit_name: "max_connections".into(),
                    resource_type: "connection".into(),
                    desired: connections.current.to_string(),
                    limit: connections.limit.to_string(),
                    current: (connections.current - 1).to_string(),
                });
            }
            Some(guard)
        } else {
            None
        };
        let adapter_client = adapter_client.new_conn()?;
        let session = adapter_client.new_session(user);
        let (adapter_client, _) = adapter_client.startup(session).await?;
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
        let mut client = AuthedClient::new(
            &adapter_client,
            user.clone(),
            Arc::clone(active_connection_count),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Apply options that were provided in query params.
        let session = client.client.session();
        let maybe_options = if params.options.is_empty() {
            // It's possible 'options' simply wasn't provided, we don't want that to
            // count as a failure to deserialize
            Ok(BTreeMap::<String, String>::default())
        } else {
            serde_json::from_str(&params.options)
        };

        if let Ok(options) = maybe_options {
            for (key, val) in options {
                const LOCAL: bool = false;
                if let Err(err) = session.vars_mut().set(&key, VarInput::Flat(&val), LOCAL) {
                    session.add_notice(AdapterNotice::BadStartupSetting {
                        name: key.to_string(),
                        reason: err.to_string(),
                    })
                }
            }
        } else {
            // If we fail to deserialize options, fail the request.
            let code = StatusCode::BAD_REQUEST;
            let msg = format!("Failed to deserialize {} map", "options".quoted());
            return Err((code, msg));
        }

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
    MismatchedUser(&'static str),
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
    frontegg: &Option<FronteggAuthentication>,
) -> impl IntoResponse {
    // First, extract the username from the certificate, validating that the
    // connection matches the TLS configuration along the way.
    let conn_protocol = req.extensions().get::<ConnProtocol>().unwrap();
    let cert_user = match (tls_mode, &conn_protocol) {
        (TlsMode::Disable, ConnProtocol::Http) => None,
        (TlsMode::Disable, ConnProtocol::Https { .. }) => unreachable!(),
        (TlsMode::Require, ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (TlsMode::Require, ConnProtocol::Https { .. }) => None,
    };
    let creds = match frontegg {
        // If no Frontegg authentication, we can use the cert's username if
        // present, otherwise the default HTTP user.
        None => Credentials::User(cert_user),
        Some(_) => {
            if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
                if let Some(user) = cert_user {
                    if basic.username() != user {
                        return Err(AuthError::MismatchedUser(
                        "user in client certificate did not match user specified in authorization header",
                    ));
                    }
                }
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
        adapter_client,
        active_connection_count,
    }: &WsState,
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
    let (creds, options) = if frontegg.is_some() {
        match ws_auth {
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
        }
    } else if let WebSocketAuth::Basic { user, options, .. } = ws_auth {
        (Credentials::User(Some(user)), options)
    } else {
        anyhow::bail!("unexpected")
    };
    let user = auth(frontegg, creds).await?;

    let mut client =
        AuthedClient::new(adapter_client, user, Arc::clone(active_connection_count)).await?;

    // Assign any options we got from our WebSocket startup.
    let session = client.client.session();
    for (key, val) in options {
        const LOCAL: bool = false;
        if let Err(err) = session.vars_mut().set(&key, VarInput::Flat(&val), LOCAL) {
            session.add_notice(AdapterNotice::BadStartupSetting {
                name: key,
                reason: err.to_string(),
            })
        }
    }

    Ok(client)
}

enum Credentials {
    User(Option<String>),
    Password { username: String, password: String },
    Token { token: String },
}

async fn auth(
    frontegg: &Option<FronteggAuthentication>,
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
    let user = match (frontegg, creds) {
        // If no Frontegg authentication, use the requested user or the default
        // HTTP user.
        (None, Credentials::User(user)) => User {
            name: user.unwrap_or_else(|| HTTP_DEFAULT_USER.name.to_string()),
            external_metadata: None,
        },
        // With frontegg disabled, specifying credentials is an error.
        (None, _) => return Err(AuthError::UnexpectedCredentials),
        // If we require Frontegg auth, fetch credentials from the HTTP auth
        // header. Basic auth comes with a username/password, where the password
        // is the client+secret pair. Bearer auth is an existing JWT that must
        // be validated. In either case, if a username was specified in the
        // client cert, it must match that of the JWT.
        (Some(frontegg), creds) => {
            let (user, token) = match creds {
                Credentials::Password { username, password } => (
                    Some(username),
                    frontegg
                        .exchange_password_for_token(&password)
                        .await?
                        .access_token,
                ),
                Credentials::Token { token } => (None, token),
                Credentials::User(_) => return Err(AuthError::MissingHttpAuthentication),
            };
            let claims = frontegg.validate_access_token(&token, user.as_deref())?;
            User {
                external_metadata: Some(ExternalUserMetadata {
                    user_id: claims.best_user_id(),
                    group_id: claims.tenant_id,
                    admin: claims.admin(frontegg.admin_role()),
                }),
                name: claims.email,
            }
        }
    };

    if mz_adapter::catalog::is_reserved_role_name(user.name.as_str()) {
        return Err(AuthError::InvalidLogin(user.name));
    }
    Ok(AuthedUser(user))
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
        router = router.nest("/prof/", mz_prof::http::router(&BUILD_INFO));
    }
    router
}
