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
//! materialized embeds an HTTP server for introspection into the running
//! process. At the moment, its primary exports are Prometheus metrics, heap
//! profiles, and catalog dumps.

// Axum handlers must use async, but often don't actually use `await`.
#![allow(clippy::unused_async)]

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{FromRequest, RequestParts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::IntoMakeService;
use axum::{routing, Extension, Router};
use futures::future::TryFutureExt;
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Request, StatusCode};
use hyper::server::conn::AddrIncoming;
use hyper_openssl::MaybeHttpsStream;
use mz_adapter::SessionClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::OpenTelemetryEnableCallback;
use openssl::nid::Nid;
use openssl::ssl::{Ssl, SslContext};
use openssl::x509::X509;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use mz_adapter::session::Session;
use mz_frontegg_auth::{FronteggAuthentication, FronteggError};

use crate::BUILD_INFO;

mod catalog;
mod memory;
mod root;
mod sql;

const SYSTEM_USER: &str = "mz_system";

#[derive(Debug, Clone)]
pub struct Config {
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
    Require,
    AssumeUser,
}

#[derive(Debug)]
pub struct Server {
    tls: Option<TlsConfig>,
    // NOTE(benesch): this `Mutex` is silly, but necessary because using this
    // server requires `Sync` and `Router` is not `Sync` by default. It is
    // unlikely to be a performance problem in practice.
    router: Mutex<Router>,
}

impl Server {
    pub fn new(
        Config {
            tls,
            frontegg,
            adapter_client,
            allowed_origin,
        }: Config,
    ) -> Server {
        let tls_mode = tls.as_ref().map(|tls| tls.mode);
        let frontegg = Arc::new(frontegg);
        let router = Router::new()
            .route("/", routing::get(root::handle_home))
            .route(
                "/api/internal/catalog",
                routing::get(catalog::handle_internal_catalog),
            )
            .route("/api/sql", routing::post(sql::handle_sql))
            .route("/memory", routing::get(memory::handle_memory))
            .route(
                "/hierarchical-memory",
                routing::get(memory::handle_hierarchical_memory),
            )
            .nest("/prof/", mz_prof::http::router(&BUILD_INFO))
            .route("/static/*path", routing::get(root::handle_static))
            .layer(Extension(adapter_client))
            .layer(middleware::from_fn(move |req, next| {
                let frontegg = Arc::clone(&frontegg);
                async move { auth(req, next, tls_mode, &frontegg).await }
            }))
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
        Server {
            tls,
            router: Mutex::new(router),
        }
    }

    fn tls_context(&self) -> Option<&SslContext> {
        self.tls.as_ref().map(|tls| &tls.context)
    }

    pub async fn handle_connection(&self, conn: TcpStream) -> Result<(), anyhow::Error> {
        let (conn, conn_protocol) = match &self.tls_context() {
            Some(tls_context) => {
                let mut ssl_stream = SslStream::new(Ssl::new(tls_context)?, conn)?;
                if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                    let _ = ssl_stream.get_mut().shutdown().await;
                    return Err(e.into());
                }
                let client_cert = ssl_stream.ssl().peer_certificate();
                (
                    MaybeHttpsStream::Https(ssl_stream),
                    ConnProtocol::Https { client_cert },
                )
            }
            _ => (MaybeHttpsStream::Http(conn), ConnProtocol::Http),
        };
        let router = self.router.lock().expect("lock poisoned").clone();
        let svc = router.layer(Extension(conn_protocol));
        let http = hyper::server::conn::Http::new();
        http.serve_connection(conn, svc).err_into().await
    }

    // Handler functions are attached by various submodules. They all have a
    // signature of the following form:
    //
    //     fn handle_foo(req) -> impl Future<Output = anyhow::Result<Result<Body>>>
    //
    // If you add a new handler, please add it to the most appropriate
    // submodule, or create a new submodule if necessary. Don't add it here!
}

#[derive(Clone)]
enum ConnProtocol {
    Http,
    Https { client_cert: Option<X509> },
}

struct AuthedUser {
    user: String,
    create_if_not_exists: bool,
}

pub struct AuthedClient(pub SessionClient);

#[async_trait]
impl<B> FromRequest<B> for AuthedClient
where
    B: Send,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let AuthedUser {
            user,
            create_if_not_exists,
        } = req.extensions().get::<AuthedUser>().unwrap();
        let adapter_client = req.extensions().get::<mz_adapter::Client>().unwrap();

        let adapter_client = adapter_client
            .new_conn()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let session = Session::new(adapter_client.conn_id(), user.clone());
        let (adapter_client, _) = match adapter_client.startup(session, *create_if_not_exists).await
        {
            Ok(adapter_client) => adapter_client,
            Err(e) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
            }
        };

        Ok(AuthedClient(adapter_client))
    }
}

#[derive(Debug, Error)]
enum AuthError {
    #[error("HTTPS is required")]
    HttpsRequired,
    #[error("invalid username in client certificate")]
    InvalidCertUserName,
    #[error("{0}")]
    Frontegg(#[from] FronteggError),
    #[error("missing authorization header")]
    MissingHttpAuthentication,
    #[error("{0}")]
    MismatchedUser(&'static str),
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
        (StatusCode::UNAUTHORIZED, message).into_response()
    }
}

async fn auth<B>(
    mut req: Request<B>,
    next: Next<B>,
    tls_mode: Option<TlsMode>,
    frontegg: &Option<FronteggAuthentication>,
) -> impl IntoResponse {
    // There are three places a username may be specified:
    //
    //   - certificate common name
    //   - HTTP Basic authentication
    //   - JWT email address
    //
    // We verify that if any of these are present, they must match any other
    // that is also present.

    // First, extract the username from the certificate, validating that the
    // connection matches the TLS configuration along the way.
    let conn_protocol = req.extensions().get::<ConnProtocol>().unwrap();
    let mut user = match (tls_mode, &conn_protocol) {
        (None, ConnProtocol::Http) => None,
        (None, ConnProtocol::Https { .. }) => unreachable!(),
        (Some(TlsMode::Require), ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (Some(TlsMode::Require), ConnProtocol::Https { .. }) => None,
        (Some(TlsMode::AssumeUser), ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (Some(TlsMode::AssumeUser), ConnProtocol::Https { client_cert }) => client_cert
            .as_ref()
            .and_then(|cert| cert.subject_name().entries_by_nid(Nid::COMMONNAME).next())
            .and_then(|cn| cn.data().as_utf8().ok())
            .map(|cn| Some(cn.to_string()))
            .ok_or(AuthError::InvalidCertUserName)?,
    };

    // Then, handle Frontegg authentication if required.
    let user = match frontegg {
        // If no Frontegg authentication, we can use the cert's username if
        // present, otherwise the system user.
        None => user.unwrap_or_else(|| SYSTEM_USER.to_string()),
        // If we require Frontegg auth, fetch credentials from the HTTP auth
        // header. Basic auth comes with a username/password, where the password
        // is the client+secret pair. Bearer auth is an existing JWT that must
        // be validated. In either case, if a username was specified in the
        // client cert, it must match that of the JWT.
        Some(frontegg) => {
            let token = if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
                if let Some(user) = user {
                    if basic.username() != user {
                        return Err(AuthError::MismatchedUser(
                            "user in client certificate did not match user specified in authorization header",
                        ));
                    }
                }
                user = Some(basic.username().to_string());
                frontegg
                    .exchange_password_for_token(basic.0.password())
                    .await?
                    .access_token
            } else if let Some(bearer) = req.headers().typed_get::<Authorization<Bearer>>() {
                bearer.token().to_string()
            } else {
                return Err(AuthError::MissingHttpAuthentication);
            };
            let claims = frontegg.validate_access_token(&token, user.as_deref())?;
            claims.email
        }
    };

    // Add the authenticated user as an extension so downstream handlers can
    // inspect it if necessary.
    req.extensions_mut().insert(AuthedUser {
        user,
        create_if_not_exists: frontegg.is_some(),
    });

    // Run the request.
    Ok(next.run(req).await)
}

#[derive(Clone)]
pub struct InternalServer {
    metrics_registry: MetricsRegistry,
    otel_collector_enabler: OpenTelemetryEnableCallback,
}

impl InternalServer {
    pub fn new(
        metrics_registry: MetricsRegistry,
        otel_collector_enabler: OpenTelemetryEnableCallback,
    ) -> Self {
        Self {
            metrics_registry,
            otel_collector_enabler,
        }
    }

    pub fn bind(self, addr: SocketAddr) -> axum::Server<AddrIncoming, IntoMakeService<Router>> {
        let metrics_registry = self.metrics_registry;
        let router = Router::new()
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
            .route(
                "/api/opentelemetry/config",
                routing::put(move |payload| async move {
                    mz_http_util::handle_enable_otel(self.otel_collector_enabler, payload).await
                }),
            );
        axum::Server::bind(&addr).serve(router.into_make_service())
    }
}
