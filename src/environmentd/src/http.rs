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

use std::convert::From;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::anyhow;
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
use itertools::izip;
use mz_sql::plan::Plan;
use openssl::nid::Nid;
use openssl::ssl::{Ssl, SslContext};
use openssl::x509::X509;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use mz_adapter::catalog::HTTP_DEFAULT_USER;
use mz_adapter::session::{EndTransactionAction, Session, TransactionStatus, User};
use mz_adapter::{
    ExecuteResponse, ExecuteResponseKind, ExecuteResponsePartialError, PeekResponseUnary,
    SessionClient,
};
use mz_frontegg_auth::{FronteggAuthentication, FronteggError};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::{OpenTelemetryEnableCallback, StderrFilterCallback};
use mz_repr::{Datum, RowArena};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{Raw, Statement};

use crate::BUILD_INFO;

mod catalog;
mod memory;
mod root;
pub(crate) mod sql;

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
            .layer(middleware::from_fn(move |req, next| {
                let frontegg = Arc::clone(&frontegg);
                async move { auth(req, next, tls_mode, &frontegg).await }
            }))
            .layer(Extension(adapter_client))
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
    user: User,
    create_if_not_exists: bool,
}

/// The result of a single query executed with `simple_execute`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SimpleResult {
    /// The query returned rows.
    Rows {
        /// The result rows.
        rows: Vec<Vec<serde_json::Value>>,
        /// The name of the columns in the row.
        col_names: Vec<String>,
    },
    /// The query executed successfully but did not return rows.
    Ok {
        ok: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partial_err: Option<ExecuteResponsePartialError>,
    },
    /// The query returned an error.
    Err { error: String },
}

impl SimpleResult {
    pub(crate) fn err(msg: impl std::fmt::Display) -> SimpleResult {
        SimpleResult::Err {
            error: msg.to_string(),
        }
    }

    /// Generates a `SimpleResult::Ok` based on an `ExecuteResponse`.
    ///
    /// # Panics
    /// - If `ExecuteResponse::partial_err(&res)` panics.
    pub(crate) fn ok(res: ExecuteResponse) -> SimpleResult {
        let ok = res.tag();
        let partial_err = res.partial_err();
        SimpleResult::Ok { ok, partial_err }
    }
}

/// The response to [`AuthedClient::execute_sql_http_request`].
#[derive(Debug, Serialize)]
pub struct SqlHttpExecuteResponse {
    pub results: Vec<SimpleResult>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtendedRequest {
    // TODO: we can remove this alias once platforms issues these requests
    // using `query`.
    #[serde(alias = "sql")]
    /// 0 or 1 queries
    pub query: String,
    /// Optional parameters for the query
    #[serde(default)]
    pub params: Vec<Option<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum HttpSqlRequest {
    /// 0 or more queries delimited by semicolons in the same strings. Do not
    /// support parameters.
    Simple {
        #[serde(alias = "sql")]
        query: String,
    },
    /// 0 or more [`ExtendedRequest`]s.
    Extended { queries: Vec<ExtendedRequest> },
}

pub struct AuthedClient(pub SessionClient);

impl AuthedClient {
    /// Executes an [`HttpSqlRequest`].
    ///
    /// The provided `stmts` are executed directly, and their results
    /// are returned as a vector of rows, where each row is a vector of JSON
    /// objects.
    pub async fn execute_sql_http_request(
        &mut self,
        request: HttpSqlRequest,
    ) -> Result<SqlHttpExecuteResponse, anyhow::Error> {
        // This API prohibits executing statements with responses whose
        // semantics are at odds with an HTTP response.
        fn check_prohibited_stmts(stmt: &Statement<Raw>) -> Result<(), anyhow::Error> {
            let execute_responses = Plan::generated_from(stmt.into())
                .into_iter()
                .map(ExecuteResponse::generated_from)
                .flatten()
                .collect::<Vec<_>>();

            if execute_responses.iter().any(|execute_response| {
                matches!(
                    execute_response,
                    ExecuteResponseKind::Fetch
                        | ExecuteResponseKind::SetVariable
                        | ExecuteResponseKind::Tailing
                        | ExecuteResponseKind::CopyTo
                        | ExecuteResponseKind::CopyFrom
                        | ExecuteResponseKind::Raise
                        | ExecuteResponseKind::DeclaredCursor
                        | ExecuteResponseKind::ClosedCursor
                )
            }) && !matches!(
                stmt,
                // Both `SelectStatement` and `CopyStatement` generate
                // `PeekPlan`, but `SELECT` should be permitted and `COPY` not.
                Statement::Select(mz_sql::ast::SelectStatement { query: _, as_of: _ })
            ) {
                anyhow::bail!("unsupported via this API: {}", stmt.to_ast_string());
            }

            Ok(())
        }

        let mut stmt_groups = vec![];
        let mut results = vec![];

        match request {
            HttpSqlRequest::Simple { query } => {
                let stmts = mz_sql::parse::parse(&query).map_err(|e| anyhow!(e))?;
                let mut stmt_group = Vec::with_capacity(stmts.len());
                for stmt in stmts {
                    check_prohibited_stmts(&stmt)?;
                    stmt_group.push((stmt, vec![]));
                }
                stmt_groups.push(stmt_group);
            }
            HttpSqlRequest::Extended { queries } => {
                for ExtendedRequest { query, params } in queries {
                    let mut stmts = mz_sql::parse::parse(&query).map_err(|e| anyhow!(e))?;
                    if stmts.len() != 1 {
                        anyhow::bail!(
                            "each query must contain exactly 1 statement, but \"{}\" contains {}",
                            query,
                            stmts.len()
                        );
                    }

                    let stmt = stmts.pop().unwrap();
                    check_prohibited_stmts(&stmt)?;

                    stmt_groups.push(vec![(stmt, params)]);
                }
            }
        }

        for stmt_group in stmt_groups {
            let num_stmts = stmt_group.len();
            for (stmt, params) in stmt_group {
                assert!(num_stmts <= 1 || params.is_empty(),
                    "statement groups contain more than 1 statement iff Simple request, which does not support parameters"
                );

                if matches!(self.0.session().transaction(), TransactionStatus::Failed(_)) {
                    break;
                }
                // Mirror the behavior of the PostgreSQL simple query protocol.
                // See the pgwire::protocol::StateMachine::query method for details.
                if let Err(e) = self.0.start_transaction(Some(num_stmts)).await {
                    results.push(SimpleResult::err(e));
                    break;
                }
                let res = self.execute_stmt(stmt, params).await;
                if matches!(res, SimpleResult::Err { .. }) {
                    self.0.fail_transaction();
                }
                results.push(res);
            }
        }

        if self.0.session().transaction().is_implicit() {
            self.0.end_transaction(EndTransactionAction::Commit).await?;
        }
        Ok(SqlHttpExecuteResponse { results })
    }

    async fn execute_stmt(
        &mut self,
        stmt: Statement<Raw>,
        raw_params: Vec<Option<String>>,
    ) -> SimpleResult {
        let client = &mut self.0;

        const EMPTY_PORTAL: &str = "";
        if let Err(e) = client
            .describe(EMPTY_PORTAL.into(), Some(stmt.clone()), vec![])
            .await
        {
            return SimpleResult::err(e);
        }

        let prep_stmt = match client.get_prepared_statement(&EMPTY_PORTAL).await {
            Ok(stmt) => stmt,
            Err(err) => {
                return SimpleResult::err(err);
            }
        };

        let param_types = &prep_stmt.desc().param_types;
        if param_types.len() != raw_params.len() {
            let message = format!(
                "request supplied {actual} parameters, \
                         but {statement} requires {expected}",
                statement = (&stmt).to_ast_string(),
                actual = raw_params.len(),
                expected = param_types.len()
            );
            return SimpleResult::err(message);
        }

        let buf = RowArena::new();
        let mut params = vec![];
        for (raw_param, mz_typ) in izip!(raw_params, param_types) {
            let pg_typ = mz_pgrepr::Type::from(mz_typ);
            let datum = match raw_param {
                None => Datum::Null,
                Some(raw_param) => {
                    match mz_pgrepr::Value::decode(
                        mz_pgrepr::Format::Text,
                        &pg_typ,
                        &raw_param.as_bytes(),
                    ) {
                        Ok(param) => param.into_datum(&buf, &pg_typ),
                        Err(err) => {
                            let msg = format!("unable to decode parameter: {}", err);
                            return SimpleResult::err(msg);
                        }
                    }
                }
            };
            params.push((datum, mz_typ.clone()))
        }

        let result_formats = vec![
            mz_pgrepr::Format::Text;
            prep_stmt
                .desc()
                .relation_desc
                .clone()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0)
        ];

        let desc = prep_stmt.desc().clone();
        let revision = prep_stmt.catalog_revision;
        let stmt = prep_stmt.sql().cloned();
        if let Err(err) = client.session().set_portal(
            EMPTY_PORTAL.into(),
            desc,
            stmt,
            params,
            result_formats,
            revision,
        ) {
            return SimpleResult::err(err.to_string());
        }

        let desc = client
            .session()
            // We do not need to verify here because `self.execute` verifies below.
            .get_portal_unverified(EMPTY_PORTAL)
            .map(|portal| portal.desc.clone())
            .expect("unnamed portal should be present");

        let res = match client.execute(EMPTY_PORTAL.into()).await {
            Ok(res) => res,
            Err(e) => {
                return SimpleResult::err(e);
            }
        };

        match res {
            ExecuteResponse::Canceled => {
                SimpleResult::err("statement canceled due to user request")
            }
            res @ (ExecuteResponse::CreatedConnection { existed: _ }
            | ExecuteResponse::CreatedDatabase { existed: _ }
            | ExecuteResponse::CreatedSchema { existed: _ }
            | ExecuteResponse::CreatedRole
            | ExecuteResponse::CreatedComputeInstance { existed: _ }
            | ExecuteResponse::CreatedComputeInstanceReplica { existed: _ }
            | ExecuteResponse::CreatedTable { existed: _ }
            | ExecuteResponse::CreatedIndex { existed: _ }
            | ExecuteResponse::CreatedSecret { existed: _ }
            | ExecuteResponse::CreatedSource { existed: _ }
            | ExecuteResponse::CreatedSources
            | ExecuteResponse::CreatedSink { existed: _ }
            | ExecuteResponse::CreatedView { existed: _ }
            | ExecuteResponse::CreatedViews { existed: _ }
            | ExecuteResponse::CreatedMaterializedView { existed: _ }
            | ExecuteResponse::CreatedType
            | ExecuteResponse::Deleted(_)
            | ExecuteResponse::DiscardedTemp
            | ExecuteResponse::DiscardedAll
            | ExecuteResponse::DroppedDatabase
            | ExecuteResponse::DroppedSchema
            | ExecuteResponse::DroppedRole
            | ExecuteResponse::DroppedComputeInstance
            | ExecuteResponse::DroppedComputeInstanceReplicas
            | ExecuteResponse::DroppedSource
            | ExecuteResponse::DroppedIndex
            | ExecuteResponse::DroppedSink
            | ExecuteResponse::DroppedTable
            | ExecuteResponse::DroppedView
            | ExecuteResponse::DroppedMaterializedView
            | ExecuteResponse::DroppedType
            | ExecuteResponse::DroppedSecret
            | ExecuteResponse::DroppedConnection
            | ExecuteResponse::EmptyQuery
            | ExecuteResponse::Inserted(_)
            | ExecuteResponse::StartedTransaction { duplicated: _ }
            | ExecuteResponse::TransactionExited {
                tag: _,
                was_implicit: _,
            }
            | ExecuteResponse::Updated(_)
            | ExecuteResponse::AlteredObject(_)
            | ExecuteResponse::AlteredIndexLogicalCompaction
            | ExecuteResponse::AlteredSystemConfiguraion
            | ExecuteResponse::Deallocate { all: _ }
            | ExecuteResponse::Prepare) => SimpleResult::ok(res),
            ExecuteResponse::SendingRows {
                future: rows,
                span: _,
            } => {
                let rows = match rows.await {
                    PeekResponseUnary::Rows(rows) => rows,
                    PeekResponseUnary::Error(e) => {
                        return SimpleResult::err(e);
                    }
                    PeekResponseUnary::Canceled => {
                        return SimpleResult::err("statement canceled due to user request");
                    }
                };
                let mut sql_rows: Vec<Vec<serde_json::Value>> = vec![];
                let col_names = match desc.relation_desc {
                    Some(desc) => desc.iter_names().map(|name| name.to_string()).collect(),
                    None => vec![],
                };
                let mut datum_vec = mz_repr::DatumVec::new();
                for row in rows {
                    let datums = datum_vec.borrow_with(&row);
                    sql_rows.push(datums.iter().map(From::from).collect());
                }
                SimpleResult::Rows {
                    rows: sql_rows,
                    col_names,
                }
            }
            res @ (ExecuteResponse::Fetch { .. }
            | ExecuteResponse::SetVariable { .. }
            | ExecuteResponse::Tailing { .. }
            | ExecuteResponse::CopyTo { .. }
            | ExecuteResponse::CopyFrom { .. }
            | ExecuteResponse::Raise { .. }
            | ExecuteResponse::DeclaredCursor
            | ExecuteResponse::ClosedCursor) => {
                SimpleResult::err(
                    format!("internal error: encountered prohibited ExecuteResponse {:?}.\n\n
This is a bug. Can you please file an issue letting us know?\n
https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=C-bug%2CC-triage&template=01-bug.yml",
                ExecuteResponseKind::from(res)))
            }
        }
    }
}

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
    #[error("unauthorized login to user '{0}'")]
    InvalidLogin(String),
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
        // present, otherwise the default HTTP user.
        None => User {
            name: user.unwrap_or_else(|| HTTP_DEFAULT_USER.name.to_string()),
            external_id: None,
        },
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
            User {
                name: claims.email,
                external_id: Some(claims.user_id),
            }
        }
    };

    if mz_adapter::catalog::is_reserved_name(user.name.as_str()) {
        return Err(AuthError::InvalidLogin(user.name));
    }

    // Add the authenticated user as an extension so downstream handlers can
    // inspect it if necessary.
    req.extensions_mut().insert(AuthedUser {
        user,
        create_if_not_exists: frontegg.is_some() || !matches!(tls_mode, Some(TlsMode::AssumeUser)),
    });

    // Run the request.
    Ok(next.run(req).await)
}

#[derive(Clone)]
pub struct InternalServer {
    metrics_registry: MetricsRegistry,
    otel_enable_callback: OpenTelemetryEnableCallback,
    stderr_filter_callback: StderrFilterCallback,
}

impl InternalServer {
    pub fn new(
        metrics_registry: MetricsRegistry,
        otel_enable_callback: OpenTelemetryEnableCallback,
        stderr_filter_callback: StderrFilterCallback,
    ) -> Self {
        Self {
            metrics_registry,
            otel_enable_callback,
            stderr_filter_callback,
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
                    mz_http_util::handle_enable_otel(self.otel_enable_callback, payload).await
                }),
            )
            .route(
                "/api/stderr/config",
                routing::put(move |payload| async move {
                    mz_http_util::handle_modify_stderr_filter(self.stderr_filter_callback, payload)
                        .await
                }),
            );
        axum::Server::bind(&addr).serve(router.into_make_service())
    }
}
