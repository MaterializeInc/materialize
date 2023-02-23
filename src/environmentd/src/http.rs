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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{DefaultBodyLimit, FromRequestParts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing, Extension, Router};
use futures::future::{FutureExt, Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Request, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use openssl::ssl::{Ssl, SslContext};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_openssl::SslStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use mz_adapter::catalog::{HTTP_DEFAULT_USER, SYSTEM_USER};
use mz_adapter::session::{ExternalUserMetadata, User};
use mz_adapter::{AdapterError, Client, SessionClient};
use mz_frontegg_auth::{FronteggAuthentication, FronteggError};
use mz_ore::cast::u64_to_usize;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::result::ResultExt;
use mz_ore::tracing::TracingHandle;

use crate::server::{ConnectionHandler, Server};
use crate::BUILD_INFO;

pub use sql::{SqlResponse, WebSocketAuth, WebSocketResponse};

mod catalog;
mod memory;
mod probe;
mod root;
mod sql;

/// Maximum allowed size for a request.
pub const MAX_REQUEST_SIZE: usize = u64_to_usize(2 * bytesize::MB);

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub tls: Option<TlsConfig>,
    pub frontegg: Option<FronteggAuthentication>,
    pub adapter_client: mz_adapter::Client,
    pub allowed_origin: AllowOrigin,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub context: SslContext,
    pub mode: TlsMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    Disable,
    Enable,
}

#[derive(Clone)]
pub struct WsState {
    frontegg: Arc<Option<FronteggAuthentication>>,
    adapter_client: mz_adapter::Client,
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
            .layer(Extension(AuthedUser {
                user: SYSTEM_USER.clone(),
                create_if_not_exists: false,
            }))
            .layer(Extension(adapter_client_rx.shared()));
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

#[derive(Clone)]
enum ConnProtocol {
    Http,
    Https,
}

#[derive(Clone, Debug)]
struct AuthedUser {
    user: User,
    create_if_not_exists: bool,
}

pub struct AuthedClient(pub SessionClient);

impl AuthedClient {
    async fn new(adapter_client: &Client, user: AuthedUser) -> Result<Self, AdapterError> {
        let AuthedUser {
            user,
            create_if_not_exists,
        } = user;
        let adapter_client = adapter_client.new_conn()?;
        let session = adapter_client.new_session(user);
        let (adapter_client, _) = adapter_client
            .startup(session, create_if_not_exists)
            .await?;
        Ok(AuthedClient(adapter_client))
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
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
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
        AuthedClient::new(&adapter_client, user.clone())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
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
        (TlsMode::Enable, ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (TlsMode::Enable, ConnProtocol::Https { .. }) => None,
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
    let creds = if frontegg.is_some() {
        match ws_auth {
            WebSocketAuth::Basic { user, password } => Credentials::Password {
                username: user,
                password,
            },
            WebSocketAuth::Bearer { token } => Credentials::Token { token },
        }
    } else if let WebSocketAuth::Basic { user, .. } = ws_auth {
        Credentials::User(Some(user))
    } else {
        anyhow::bail!("unexpected")
    };
    let user = auth(frontegg, creds).await?;
    AuthedClient::new(adapter_client, user).await.err_into()
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
        // If no Frontegg authentication, user the requested user or the default
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

    if mz_adapter::catalog::is_reserved_name(user.name.as_str()) {
        return Err(AuthError::InvalidLogin(user.name));
    }
    Ok(AuthedUser {
        user,
        // The internal server adds this as false, but here the external server
        // is either in local dev or in production, so we always want to auto
        // create users.
        create_if_not_exists: true,
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
        router = router.nest("/prof/", mz_prof::http::router(&BUILD_INFO));
    }
    router
}
