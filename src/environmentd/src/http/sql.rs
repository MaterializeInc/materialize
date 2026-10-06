// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use futures::Future;
use futures::future::BoxFuture;

use http::StatusCode;
use itertools::Itertools;
use mz_adapter::client::{RecordFirstRowStream, redact_sql_for_logging};
use mz_adapter::session::{EndTransactionAction, TransactionStatus};
use mz_adapter::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use mz_adapter::{
    AdapterError, AdapterNotice, EXECUTION_TIME_NOTICE_CODE, ExecuteContextGuard, ExecuteResponse,
    ExecuteResponseKind, ExecutionTime, ExecutionTimeKind, PeekResponseUnary, SessionClient,
    verify_datum_desc,
};
use mz_auth::password::Password;
use mz_catalog::memory::objects::{Cluster, ClusterReplica};
use mz_interchange::encode::TypedDatum;
use mz_interchange::json::{JsonNumberPolicy, ToJson};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{MakeCollectorOpts, MetricsRegistry};
use mz_ore::result::ResultExt;
use mz_ore::sql::Sql;
use mz_repr::{Datum, RelationDesc, RowArena, RowIterator};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{CopyDirection, CopyStatement, CopyTarget, Raw, Statement, StatementKind};
use mz_sql::parse::StatementParseResult;
use mz_sql::plan::Plan;
use mz_sql::session::metadata::SessionMetadata;
use prometheus::Opts;
use prometheus::core::{AtomicF64, GenericGaugeVec};
use serde::{Deserialize, Serialize};
use tokio::{select, time};
use tokio_postgres::error::SqlState;
use tower_sessions::Session as TowerSession;
use tracing::{debug, info};
use tungstenite::protocol::frame::coding::CloseCode;

use crate::http::prometheus::PrometheusSqlQuery;
use crate::http::{
    AuthError, AuthedClient, AuthedUser, MAX_REQUEST_SIZE, WsState, ensure_session_unexpired,
    init_ws, maybe_get_authenticated_session,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Axum(#[from] axum::Error),
    #[error("SUBSCRIBE only supported over websocket")]
    SubscribeOnlyOverWs,
    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    AbortedTransaction,
    #[error("unsupported via this API: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Unstructured(anyhow::Error),
}

impl Error {
    pub fn detail(&self) -> Option<String> {
        match self {
            Error::Adapter(err) => err.detail(),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Error::Adapter(err) => err.hint(),
            _ => None,
        }
    }

    pub fn position(&self) -> Option<usize> {
        match self {
            Error::Adapter(err) => err.position(),
            _ => None,
        }
    }

    pub fn code(&self) -> SqlState {
        match self {
            Error::Adapter(err) => err.code(),
            Error::AbortedTransaction => SqlState::IN_FAILED_SQL_TRANSACTION,
            _ => SqlState::INTERNAL_ERROR,
        }
    }
}

static PER_REPLICA_LABELS: &[&str] = &["replica_full_name", "instance_id", "replica_id"];

async fn execute_promsql_query(
    client: &mut AuthedClient,
    query: &PrometheusSqlQuery<'_>,
    metrics_registry: &MetricsRegistry,
    metrics_by_name: &mut BTreeMap<String, GenericGaugeVec<AtomicF64>>,
    cluster: Option<(&Cluster, &ClusterReplica)>,
) {
    assert_eq!(query.per_replica, cluster.is_some());

    let mut res = SqlResponse {
        results: Vec::new(),
    };

    execute_request(client, query.to_sql_request(cluster), &mut res)
        .await
        .expect("valid SQL query");

    let result = match res.results.as_slice() {
        // Each query issued is preceded by several SET commands
        // to make sure it is routed to the right cluster replica.
        [
            SqlResult::Ok { .. },
            SqlResult::Ok { .. },
            SqlResult::Ok { .. },
            result,
        ] => result,
        // Transient errors are fine, like if the cluster or replica
        // was dropped before the promsql query was executed. We
        // should not see errors in the steady state.
        _ => {
            info!(
                "error executing prometheus query {}: {:?}",
                query.metric_name, res
            );
            return;
        }
    };

    let SqlResult::Rows { desc, rows, .. } = result else {
        info!(
            "did not receive rows for SQL query for prometheus metric {}: {:?}, {:?}",
            query.metric_name, result, cluster
        );
        return;
    };

    let gauge_vec = metrics_by_name
        .entry(query.metric_name.to_string())
        .or_insert_with(|| {
            let mut label_names: Vec<String> = desc
                .columns
                .iter()
                .filter(|col| col.name != query.value_column_name)
                .map(|col| col.name.clone())
                .collect();

            if query.per_replica {
                label_names.extend(PER_REPLICA_LABELS.iter().map(|label| label.to_string()));
            }

            metrics_registry.register::<GenericGaugeVec<AtomicF64>>(MakeCollectorOpts {
                opts: Opts::new(query.metric_name, query.help).variable_labels(label_names),
                buckets: None,
            })
        });

    for row in rows {
        // Rows are stored as pre-serialized JSON arrays. Parse each one back to
        // `Value`s here. Promsql results are tiny, so the parse cost is
        // negligible.
        let row: Vec<serde_json::Value> =
            serde_json::from_str(row.get()).expect("row is a valid JSON array");

        // Non-value columns become Prometheus label values. A SQL `NULL`
        // arrives as JSON `null` and yields `None` from `as_str()`; fall back
        // to an empty label rather than panicking. The query author is
        // responsible for `COALESCE`ing nullable columns to a meaningful
        // value; this is a defensive backstop.
        let mut label_values = desc
            .columns
            .iter()
            .zip_eq(&row)
            .filter(|(col, _)| col.name != query.value_column_name)
            .map(|(_, val)| val.as_str().unwrap_or(""))
            .collect::<Vec<_>>();

        let value = desc
            .columns
            .iter()
            .zip_eq(&row)
            .find(|(col, _)| col.name == query.value_column_name)
            .map(|(_, val)| val.as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0))
            .unwrap_or(0.0);

        match cluster {
            Some((cluster, replica)) => {
                let replica_full_name = format!("{}.{}", cluster.name, replica.name);
                let cluster_id = cluster.id.to_string();
                let replica_id = replica.replica_id.to_string();

                label_values.push(&replica_full_name);
                label_values.push(&cluster_id);
                label_values.push(&replica_id);

                gauge_vec
                    .get_metric_with_label_values(&label_values)
                    .expect("valid labels")
                    .set(value);
            }
            None => {
                gauge_vec
                    .get_metric_with_label_values(&label_values)
                    .expect("valid labels")
                    .set(value);
            }
        }
    }
}

async fn handle_promsql_query(
    client: &mut AuthedClient,
    query: &PrometheusSqlQuery<'_>,
    metrics_registry: &MetricsRegistry,
    metrics_by_name: &mut BTreeMap<String, GenericGaugeVec<AtomicF64>>,
) {
    if !query.per_replica {
        execute_promsql_query(client, query, metrics_registry, metrics_by_name, None).await;
        return;
    }

    let catalog = client.client.catalog_snapshot("handle_promsql_query").await;
    let clusters: Vec<&Cluster> = catalog.clusters().collect();

    for cluster in clusters {
        for replica in cluster.replicas() {
            execute_promsql_query(
                client,
                query,
                metrics_registry,
                metrics_by_name,
                Some((cluster, replica)),
            )
            .await;
        }
    }
}

pub async fn handle_promsql(
    mut client: AuthedClient,
    queries: &[PrometheusSqlQuery<'_>],
) -> MetricsRegistry {
    let metrics_registry = MetricsRegistry::new();
    let mut metrics_by_name = BTreeMap::new();

    for query in queries {
        handle_promsql_query(&mut client, query, &metrics_registry, &mut metrics_by_name).await;
    }

    metrics_registry
}

pub async fn handle_sql(
    mut client: AuthedClient,
    Json(request): Json<SqlRequest>,
) -> impl IntoResponse {
    let mut res = SqlResponse {
        results: Vec::new(),
    };
    // Don't need to worry about timeouts or resetting cancel here because there is always exactly 1
    // request.
    match execute_request(&mut client, request, &mut res).await {
        Ok(()) => Ok(Json(res)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e.to_string())),
    }
}

#[derive(Debug)]
pub enum ExistingUser {
    /// An AuthedUser provided by the
    /// `x_materialize_user_header_auth` middleware
    XMaterializeUserHeader(AuthedUser),
    /// An AuthedUser provided by an authenticated session
    /// established via [`crate::http::handle_login`].
    Session(AuthedUser),
}

pub(crate) async fn handle_sql_ws(
    State(state): State<WsState>,
    existing_user: Option<Extension<AuthedUser>>,
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    tower_session: Option<Extension<TowerSession>>,
) -> Result<impl IntoResponse, AuthError> {
    let session = tower_session.map(|Extension(session)| session);
    // The `x_materialize_user_header_auth` middleware may have already provided the user for us
    let user = match existing_user {
        Some(Extension(user)) => Some(ExistingUser::XMaterializeUserHeader(user)),
        None => {
            let session = maybe_get_authenticated_session(session.as_ref()).await;
            if let Some((session, session_data)) = session {
                let user = ensure_session_unexpired(session, session_data).await?;
                Some(ExistingUser::Session(user))
            } else {
                None
            }
        }
    };

    let addr = Box::new(addr.ip());
    Ok(ws
        .max_message_size(MAX_REQUEST_SIZE)
        .on_upgrade(|ws| async move { run_ws(state, user, *addr, ws).await }))
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum WebSocketAuth {
    Basic {
        user: String,
        password: Password,
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
    Bearer {
        token: String,
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
    OptionsOnly {
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
}

async fn run_ws(state: WsState, user: Option<ExistingUser>, peer_addr: IpAddr, mut ws: WebSocket) {
    let mut client = match init_ws(state, user, peer_addr, &mut ws).await {
        Ok(client) => client,
        Err(e) => {
            // We omit most detail from the error message we send to the client, to
            // avoid giving attackers unnecessary information during auth. AdapterErrors
            // are safe to return because they're generated after authentication.
            debug!("WS request failed init: {}", e);
            let reason: Utf8Bytes = match e.downcast_ref::<AdapterError>() {
                Some(error) => error.to_string().into(),
                None => "unauthorized".to_string().into(),
            };
            let _ = ws
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Protocol.into(),
                    reason,
                })))
                .await;
            return;
        }
    };

    // Successful auth, send startup messages.
    let mut msgs = Vec::new();
    let session = client.client.session();
    for var in session.vars().notify_set() {
        msgs.push(WebSocketResponse::ParameterStatus(ParameterStatus {
            name: var.name().to_string(),
            value: var.value(),
        }));
    }
    msgs.push(WebSocketResponse::BackendKeyData(BackendKeyData {
        conn_id: session.conn_id().unhandled(),
        secret_key: session.secret_key(),
    }));
    msgs.push(WebSocketResponse::ReadyForQuery(
        session.transaction_code().into(),
    ));
    for msg in msgs {
        let _ = ws
            .send(Message::Text(
                serde_json::to_string(&msg).expect("must serialize").into(),
            ))
            .await;
    }

    // Send any notices that might have been generated on startup.
    let notices = session.drain_notices();
    if let Err(err) = forward_notices(&mut ws, notices).await {
        debug!("failed to forward notices to WebSocket, {err:?}");
        return;
    }

    loop {
        // Handle timeouts first so we don't execute any statements when there's a pending timeout.
        let msg = select! {
            biased;

            // `recv_timeout()` is cancel-safe as per it's docs.
            Some(timeout) = client.client.recv_timeout() => {
                client.client.terminate().await;
                // We must wait for the client to send a request before we can send the error
                // response. Although this isn't the PG wire protocol, we choose to mirror it by
                // only sending errors as responses to requests.
                let _ = ws.recv().await;
                let err = Error::from(AdapterError::from(timeout));
                let _ = send_ws_response(&mut ws, WebSocketResponse::Error(err.into())).await;
                return;
            },
            message = ws.recv() => message,
        };

        client.client.remove_idle_in_transaction_session_timeout();

        let msg = match msg {
            Some(Ok(msg)) => msg,
            _ => {
                // client disconnected
                return;
            }
        };

        let req: Result<SqlRequest, Error> = match msg {
            Message::Text(data) => serde_json::from_str(&data).err_into(),
            Message::Binary(data) => serde_json::from_slice(&data).err_into(),
            // Handled automatically by the server.
            Message::Ping(_) => {
                continue;
            }
            Message::Pong(_) => {
                continue;
            }
            Message::Close(_) => {
                return;
            }
        };

        // Figure out if we need to send an error, any notices, but always the ready message.
        let err = match run_ws_request(req, &mut client, &mut ws).await {
            Ok(()) => None,
            Err(err) => Some(WebSocketResponse::Error(err.into())),
        };

        // After running our request, there are several messages we need to send in a
        // specific order.
        //
        // Note: we nest these into a closure so we can centralize our error handling
        // for when sending over the WebSocket fails. We could also use a try {} block
        // here, but those aren't stabilized yet.
        let ws_response = || async {
            // First respond with any error that might have occurred.
            if let Some(e_resp) = err {
                send_ws_response(&mut ws, e_resp).await?;
            }

            // Then forward along any notices we generated.
            let notices = client.client.session().drain_notices();
            forward_notices(&mut ws, notices).await?;

            // Finally, respond that we're ready for the next query.
            let ready =
                WebSocketResponse::ReadyForQuery(client.client.session().transaction_code().into());
            send_ws_response(&mut ws, ready).await?;

            Ok::<_, Error>(())
        };

        if let Err(err) = ws_response().await {
            debug!("failed to send response over WebSocket, {err:?}");
            return;
        }
    }
}

async fn run_ws_request(
    req: Result<SqlRequest, Error>,
    client: &mut AuthedClient,
    ws: &mut WebSocket,
) -> Result<(), Error> {
    let req = req?;
    execute_request(client, req, ws).await
}

/// Sends a single [`WebSocketResponse`] over the provided [`WebSocket`].
async fn send_ws_response(ws: &mut WebSocket, resp: WebSocketResponse) -> Result<(), Error> {
    let msg = serde_json::to_string(&resp).unwrap();
    let msg = Message::Text(msg.into());
    ws.send(msg).await?;

    Ok(())
}

/// Forwards a collection of Notices to the provided [`WebSocket`].
async fn forward_notices(
    ws: &mut WebSocket,
    notices: impl IntoIterator<Item = AdapterNotice>,
) -> Result<(), Error> {
    let ws_notices = notices
        .into_iter()
        .map(|notice| WebSocketResponse::Notice(Notice::from(notice)));

    for notice in ws_notices {
        send_ws_response(ws, notice).await?;
    }

    Ok(())
}

/// A request to execute SQL over HTTP.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum SqlRequest {
    /// A simple query request.
    Simple {
        /// A query string containing zero or more queries delimited by
        /// semicolons.
        query: Sql,
    },
    /// An extended query request.
    Extended {
        /// Queries to execute using the extended protocol.
        queries: Vec<ExtendedRequest>,
    },
}

/// An request to execute a SQL query using the extended protocol.
#[derive(Serialize, Deserialize, Debug)]
pub struct ExtendedRequest {
    /// A query string containing zero or one queries.
    query: String,
    /// Optional parameters for the query.
    #[serde(default)]
    params: Vec<Option<String>>,
}

/// The response to a `SqlRequest`.
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlResponse {
    /// The results for each query in the request.
    pub(in crate::http) results: Vec<SqlResult>,
}

impl SqlResponse {
    /// Creates a new empty SqlResponse for collecting results.
    pub(in crate::http) fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }
}

pub(in crate::http) enum StatementResult {
    SqlResult(SqlResult),
    /// A peek (`SELECT`) result whose rows are streamed out of the peek response
    /// stash. The stream is consumed lazily by the sender so a large result is
    /// not buffered whole. Contrast `SqlResult::Rows`, which holds
    /// already-collected rows and exists only for the buffered JSON transport.
    Rows {
        desc: RelationDesc,
        rows_stream: RecordFirstRowStream,
        max_result_size: usize,
        /// Whether the statement ends its statement group, see [`commit_if_ends_group`].
        ends_group: bool,
        /// Whether the statement is a write that returned rows (`... RETURNING`).
        returning_write: bool,
    },
    Subscribe {
        desc: RelationDesc,
        tag: String,
        rx: RecordFirstRowStream,
        ctx_extra: ExecuteContextGuard,
    },
}

impl From<SqlResult> for StatementResult {
    fn from(inner: SqlResult) -> Self {
        Self::SqlResult(inner)
    }
}

/// The result of a single query in a [`SqlResponse`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqlResult {
    /// The query returned rows.
    Rows {
        /// The command complete tag.
        tag: String,
        /// The result rows, each already serialized to a compact JSON array.
        ///
        /// We accumulate pre-serialized rows rather than a `serde_json::Value`
        /// tree so the buffered footprint stays close to the wire size. A
        /// `Value` tree is ~10-15x larger (each cell a heap `Value`, in a
        /// per-row `Vec`, in the outer `Vec`), which is what let a large
        /// `SELECT` over the JSON endpoint OOM the process. `RawValue`
        /// re-serializes verbatim, so the wire format is unchanged.
        rows: Vec<Box<serde_json::value::RawValue>>,
        /// Information about each column.
        desc: Description,
        // Any notices generated during execution of the query.
        notices: Vec<Notice>,
    },
    /// The query executed successfully but did not return rows.
    Ok {
        /// The command complete tag.
        ok: String,
        /// Any notices generated during execution of the query.
        notices: Vec<Notice>,
        /// Any parameters that may have changed.
        ///
        /// Note: skip serializing this field in a response if the list of parameters is empty.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        parameters: Vec<ParameterStatus>,
    },
    /// The query returned an error.
    Err {
        error: SqlError,
        // Any notices generated during execution of the query.
        notices: Vec<Notice>,
    },
}

impl SqlResult {
    /// Convert adapter Row results into the buffered web row result format. Error
    /// if the row format does not match the expected descriptor, if the result
    /// exceeds `max_query_result_size`, or if [`commit_if_ends_group`] fails.
    ///
    /// This buffers the whole result, so it is used only by the JSON transport,
    /// whose response is a single document. The WebSocket transport streams rows
    /// through `StatementResult::Rows` and never calls this. The size guard
    /// counts `Row::byte_len`, matching the WebSocket transport and pgwire, and
    /// rows are stored as pre-serialized compact `RawValue`s to avoid the
    /// amplified `Value`-tree buffering that could OOM.
    async fn rows<S>(
        sender: &mut S,
        client: &mut SessionClient,
        mut rows_stream: RecordFirstRowStream,
        max_query_result_size: usize,
        desc: &RelationDesc,
        ends_group: bool,
        returning_write: bool,
    ) -> Result<SqlResult, Error>
    where
        S: ResultSender,
    {
        let mut rows: Vec<Box<serde_json::value::RawValue>> = vec![];
        let mut datum_vec = mz_repr::DatumVec::new();
        let types = &desc.typ().column_types;

        let mut query_result_size: usize = 0;

        loop {
            let peek_response = tokio::select! {
                notice = client.session().recv_notice(), if S::SUPPORTS_STREAMING_NOTICES => {
                    sender.emit_streaming_notices(vec![notice]).await?;
                    continue;
                }
                e = sender.connection_error() => return Err(e),
                r = rows_stream.recv() => {
                    match r {
                        Some(r) => r,
                        None => break,
                    }
                },
            };

            let mut sql_rows = match peek_response {
                PeekResponseUnary::Rows(rows) => rows,
                PeekResponseUnary::Error(e) => {
                    return Ok(SqlResult::err(client, e));
                }
                PeekResponseUnary::DependencyDropped(dep) => {
                    return Ok(SqlResult::err(client, dep.to_concurrent_dependency_drop()));
                }
                PeekResponseUnary::Canceled => {
                    return Ok(SqlResult::err(client, AdapterError::Canceled));
                }
            };

            if let Err(err) = verify_datum_desc(desc, &mut sql_rows) {
                return Ok(SqlResult::Err {
                    error: err.into(),
                    notices: make_notices(client),
                });
            }

            while let Some(row) = sql_rows.next() {
                // Enforce `max_result_size` on `Row::byte_len`, the same quantity
                // pgwire and the WebSocket transport use, so the cap means one
                // thing across every transport.
                query_result_size = query_result_size.saturating_add(row.byte_len());
                if query_result_size > max_query_result_size {
                    use bytesize::ByteSize;
                    return Ok(SqlResult::err(
                        client,
                        AdapterError::ResultSize(format!(
                            "result exceeds max size of {}",
                            ByteSize::b(u64::cast_from(max_query_result_size))
                        )),
                    ));
                }
                let datums = datum_vec.borrow_with(row);
                let json_row: Vec<serde_json::Value> = datums
                    .iter()
                    .enumerate()
                    .map(|(i, d)| {
                        TypedDatum::new(*d, &types[i])
                            .json(&JsonNumberPolicy::ConvertNumberToString)
                    })
                    .collect();
                // Keep only the compact serialized JSON text. The transient
                // `Value` tree above is dropped per row, so at most one row's tree
                // is resident, avoiding the amplified buffering that could OOM.
                let raw = serde_json::value::to_raw_value(&json_row)
                    .expect("row of JSON values always serializes");
                rows.push(raw);
            }
        }

        // TODO: `SqlResult::Rows` has no `parameters` field, so the parameters the commit
        // reverted (a `SET LOCAL` earlier in the group) are dropped.
        let (notice, _reverted) =
            match finish_rows(client, &mut rows_stream, ends_group, returning_write).await {
                Ok(finished) => finished,
                Err(err) => return Ok(SqlResult::err(client, err)),
            };
        if let Some(notice) = notice {
            client.session().add_notice(notice);
        }
        let tag = format!("SELECT {}", rows.len());
        Ok(SqlResult::Rows {
            tag,
            rows,
            desc: Description::from(desc),
            notices: make_notices(client),
        })
    }

    fn err(client: &mut SessionClient, error: impl Into<SqlError>) -> SqlResult {
        SqlResult::Err {
            error: error.into(),
            notices: make_notices(client),
        }
    }

    /// Builds the result of a statement that returned no rows, after [`commit_if_ends_group`],
    /// with the opted-in execution time notice among its notices. `execution` is how long the
    /// statement executed, to which the time of the commit is added.
    async fn complete(
        client: &mut SessionClient,
        tag: String,
        mut params: Vec<ParameterStatus>,
        execution: Duration,
        ends_group: bool,
        write: bool,
    ) -> SqlResult {
        let commit_started = Instant::now();
        match commit_if_ends_group(client, ends_group).await {
            Ok(reverted) => params.extend(reverted),
            Err(err) => return SqlResult::err(client, err),
        }
        let kind = if write {
            ExecutionTimeKind::for_write(client.session().has_staged_writes())
        } else {
            ExecutionTimeKind::Completed
        };
        let notice = client.session().execution_time_notice(ExecutionTime {
            kind,
            elapsed: execution + commit_started.elapsed(),
            strategy: None,
        });
        if let Some(notice) = notice {
            client.session().add_notice(notice);
        }
        SqlResult::ok(client, tag, params)
    }

    fn ok(client: &mut SessionClient, tag: String, params: Vec<ParameterStatus>) -> SqlResult {
        SqlResult::Ok {
            ok: tag,
            parameters: params,
            notices: make_notices(client),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SqlError {
    pub message: String,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,
}

impl From<Error> for SqlError {
    fn from(err: Error) -> Self {
        SqlError {
            message: err.to_string(),
            code: err.code().code().to_string(),
            detail: err.detail(),
            hint: err.hint(),
            position: err.position(),
        }
    }
}

impl From<AdapterError> for SqlError {
    fn from(value: AdapterError) -> Self {
        Error::from(value).into()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum WebSocketResponse {
    ReadyForQuery(String),
    Notice(Notice),
    Rows(Description),
    Row(Vec<serde_json::Value>),
    CommandStarting(CommandStarting),
    CommandComplete(String),
    Error(SqlError),
    ParameterStatus(ParameterStatus),
    BackendKeyData(BackendKeyData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Notice {
    message: String,
    code: String,
    severity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl Notice {
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Description {
    pub columns: Vec<Column>,
}

impl From<&RelationDesc> for Description {
    fn from(desc: &RelationDesc) -> Self {
        let columns = desc
            .iter()
            .map(|(name, typ)| {
                let pg_type = mz_pgrepr::Type::from(&typ.scalar_type);
                Column {
                    name: name.to_string(),
                    type_oid: pg_type.oid(),
                    type_len: pg_type.typlen(),
                    type_mod: pg_type.typmod(),
                }
            })
            .collect();
        Description { columns }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub type_len: i16,
    pub type_mod: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParameterStatus {
    name: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackendKeyData {
    conn_id: u32,
    secret_key: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandStarting {
    has_rows: bool,
    is_streaming: bool,
}

/// Trait describing how to transmit a response to a client. HTTP clients
/// accumulate into a Vec and send all at once. WebSocket clients send each
/// message as they occur.
#[async_trait]
pub(in crate::http) trait ResultSender: Send {
    const SUPPORTS_STREAMING_NOTICES: bool = false;

    /// Adds a result to the client. The first component of the return value is
    /// Err if sending to the client
    /// produced an error and the server should disconnect. It is Ok(Err) if the statement
    /// produced an error and should error the transaction, but remain connected. It is Ok(Ok(()))
    /// if the statement succeeded.
    /// The second component of the return value is `Some` if execution still
    /// needs to be retired for statement logging purposes.
    async fn add_result(
        &mut self,
        client: &mut SessionClient,
        res: StatementResult,
    ) -> (
        Result<Result<(), ()>, Error>,
        Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
    );

    /// Returns a future that resolves only when the client connection has gone away.
    fn connection_error(&mut self) -> BoxFuture<'_, Error>;
    /// Reports whether the client supports streaming SUBSCRIBE results.
    fn allow_subscribe(&self) -> bool;

    /// Emits a streaming notice if the sender supports it.
    ///
    /// Does nothing if `SUPPORTS_STREAMING_NOTICES` is false.
    async fn emit_streaming_notices(&mut self, _: Vec<AdapterNotice>) -> Result<(), Error> {
        unreachable!("streaming notices marked as unsupported")
    }
}

#[async_trait]
impl ResultSender for SqlResponse {
    // The first component of the return value is
    // Err if sending to the client
    // produced an error and the server should disconnect. It is Ok(Err) if the statement
    // produced an error and should error the transaction, but remain connected. It is Ok(Ok(()))
    // if the statement succeeded.
    // The second component of the return value is `Some` if execution still
    // needs to be retired for statement logging purposes.
    async fn add_result(
        &mut self,
        client: &mut SessionClient,
        res: StatementResult,
    ) -> (
        Result<Result<(), ()>, Error>,
        Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
    ) {
        let (res, stmt_logging) = match res {
            StatementResult::SqlResult(res) => {
                let is_err = matches!(res, SqlResult::Err { .. });
                self.results.push(res);
                let res = if is_err { Err(()) } else { Ok(()) };
                (res, None)
            }
            StatementResult::Rows {
                desc,
                rows_stream,
                max_result_size,
                ends_group,
                returning_write,
            } => {
                // The JSON transport is a single buffered document, so the rows
                // must be collected before the response is serialized.
                // `SqlResult::rows` bounds that buffer against `max_result_size`.
                let res = match SqlResult::rows(
                    self,
                    client,
                    rows_stream,
                    max_result_size,
                    &desc,
                    ends_group,
                    returning_write,
                )
                .await
                {
                    Ok(res) => res,
                    Err(e) => return (Err(e), None),
                };
                let is_err = matches!(res, SqlResult::Err { .. });
                self.results.push(res);
                let res = if is_err { Err(()) } else { Ok(()) };
                (res, None)
            }
            StatementResult::Subscribe { ctx_extra, .. } => {
                let message = "SUBSCRIBE only supported over websocket";
                self.results.push(SqlResult::Err {
                    error: Error::SubscribeOnlyOverWs.into(),
                    notices: Vec::new(),
                });
                (
                    Err(()),
                    Some((
                        StatementEndedExecutionReason::Errored {
                            error: message.into(),
                        },
                        ctx_extra,
                    )),
                )
            }
        };
        (Ok(res), stmt_logging)
    }

    fn connection_error(&mut self) -> BoxFuture<'_, Error> {
        Box::pin(futures::future::pending())
    }

    fn allow_subscribe(&self) -> bool {
        false
    }
}

#[async_trait]
impl ResultSender for WebSocket {
    const SUPPORTS_STREAMING_NOTICES: bool = true;

    // The first component of the return value is Err if sending to the client produced an error and
    // the server should disconnect. It is Ok(Err) if the statement produced an error and should
    // error the transaction, but remain connected. It is Ok(Ok(())) if the statement succeeded. The
    // second component of the return value is `Some` if execution still needs to be retired for
    // statement logging purposes.
    async fn add_result(
        &mut self,
        client: &mut SessionClient,
        res: StatementResult,
    ) -> (
        Result<Result<(), ()>, Error>,
        Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
    ) {
        let (has_rows, is_streaming) = match res {
            StatementResult::SqlResult(SqlResult::Err { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Ok { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Rows { .. }) => (true, false),
            StatementResult::Rows { .. } => (true, false),
            StatementResult::Subscribe { .. } => (true, true),
        };
        if let Err(e) = send_ws_response(
            self,
            WebSocketResponse::CommandStarting(CommandStarting {
                has_rows,
                is_streaming,
            }),
        )
        .await
        {
            return (Err(e), None);
        }

        let (is_err, msgs, stmt_logging) = match res {
            StatementResult::SqlResult(SqlResult::Rows { .. }) => {
                // `SqlResult::Rows` holds already-collected rows for the buffered
                // JSON transport only. The WebSocket transport streams peek
                // results through `StatementResult::Rows` and never materializes
                // a `SqlResult::Rows`.
                unreachable!("WebSocket streams peek rows via StatementResult::Rows")
            }
            StatementResult::Rows {
                ref desc,
                mut rows_stream,
                max_result_size,
                ends_group,
                returning_write,
            } => match stream_ws_peek_rows(
                self,
                client,
                desc,
                &mut rows_stream,
                max_result_size,
                ends_group,
                returning_write,
            )
            .await
            {
                Ok(result) => result,
                // A write failure means the remote broke the connection, which we
                // treat as a cancellation to match pgwire.
                Err(e) => return (Err(e), None),
            },
            StatementResult::SqlResult(SqlResult::Ok {
                ok,
                parameters,
                notices,
            }) => {
                // The execution time precedes `CommandComplete`, so clients attribute it to this
                // statement rather than to the next one.
                let (timing, notices): (Vec<_>, Vec<_>) = notices
                    .into_iter()
                    .partition(|notice| notice.code == EXECUTION_TIME_NOTICE_CODE);
                let mut msgs: Vec<_> = timing.into_iter().map(WebSocketResponse::Notice).collect();
                msgs.push(WebSocketResponse::CommandComplete(ok));
                msgs.extend(notices.into_iter().map(WebSocketResponse::Notice));
                msgs.extend(
                    parameters
                        .into_iter()
                        .map(WebSocketResponse::ParameterStatus),
                );
                (false, msgs, None)
            }
            StatementResult::SqlResult(SqlResult::Err { error, notices }) => {
                let mut msgs = vec![WebSocketResponse::Error(error)];
                msgs.extend(notices.into_iter().map(WebSocketResponse::Notice));
                (true, msgs, None)
            }
            StatementResult::Subscribe {
                ref desc,
                tag,
                mut rx,
                ctx_extra,
            } => {
                if let Err(e) = send_ws_response(self, WebSocketResponse::Rows(desc.into())).await {
                    // We consider the remote breaking the connection to be a cancellation,
                    // matching the behavior for pgwire
                    return (
                        Err(e),
                        Some((StatementEndedExecutionReason::Canceled, ctx_extra)),
                    );
                }

                let mut datum_vec = mz_repr::DatumVec::new();
                let mut result_size: usize = 0;
                let mut rows_returned = 0;
                loop {
                    let res = match await_rows(self, client, rx.recv()).await {
                        Ok(res) => res,
                        Err(e) => {
                            // We consider the remote breaking the connection to be a cancellation,
                            // matching the behavior for pgwire
                            return (
                                Err(e),
                                Some((StatementEndedExecutionReason::Canceled, ctx_extra)),
                            );
                        }
                    };
                    match res {
                        Some(PeekResponseUnary::Rows(mut rows)) => {
                            if let Err(err) = verify_datum_desc(desc, &mut rows) {
                                let error = err.to_string();
                                break (
                                    true,
                                    vec![WebSocketResponse::Error(err.into())],
                                    Some((
                                        StatementEndedExecutionReason::Errored { error },
                                        ctx_extra,
                                    )),
                                );
                            }

                            rows_returned += rows.count();
                            while let Some(row) = rows.next() {
                                result_size = result_size.saturating_add(row.byte_len());
                                let datums = datum_vec.borrow_with(row);
                                let types = &desc.typ().column_types;
                                if let Err(e) = send_ws_response(
                                    self,
                                    WebSocketResponse::Row(
                                        datums
                                            .iter()
                                            .enumerate()
                                            .map(|(i, d)| {
                                                TypedDatum::new(*d, &types[i])
                                                    .json(&JsonNumberPolicy::ConvertNumberToString)
                                            })
                                            .collect(),
                                    ),
                                )
                                .await
                                {
                                    // We consider the remote breaking the connection to be a cancellation,
                                    // matching the behavior for pgwire
                                    return (
                                        Err(e),
                                        Some((StatementEndedExecutionReason::Canceled, ctx_extra)),
                                    );
                                }
                            }
                        }
                        Some(PeekResponseUnary::Error(err)) => {
                            let error = err.to_string();
                            break (
                                true,
                                vec![WebSocketResponse::Error(err.into())],
                                Some((StatementEndedExecutionReason::Errored { error }, ctx_extra)),
                            );
                        }
                        Some(PeekResponseUnary::DependencyDropped(dep)) => {
                            let err = dep.to_concurrent_dependency_drop();
                            let error = err.to_string();
                            break (
                                true,
                                vec![WebSocketResponse::Error(err.into())],
                                Some((StatementEndedExecutionReason::Errored { error }, ctx_extra)),
                            );
                        }
                        Some(PeekResponseUnary::Canceled) => {
                            break (
                                true,
                                vec![WebSocketResponse::Error(AdapterError::Canceled.into())],
                                Some((StatementEndedExecutionReason::Canceled, ctx_extra)),
                            );
                        }
                        None => {
                            break (
                                false,
                                vec![WebSocketResponse::CommandComplete(tag)],
                                Some((
                                    StatementEndedExecutionReason::Success {
                                        result_size: Some(u64::cast_from(result_size)),
                                        rows_returned: Some(u64::cast_from(rows_returned)),
                                        execution_strategy: Some(
                                            StatementExecutionStrategy::Standard,
                                        ),
                                    },
                                    ctx_extra,
                                )),
                            );
                        }
                    }
                }
            }
        };
        for msg in msgs {
            if let Err(e) = send_ws_response(self, msg).await {
                return (
                    Err(e),
                    stmt_logging.map(|(_old_reason, ctx_extra)| {
                        (StatementEndedExecutionReason::Canceled, ctx_extra)
                    }),
                );
            }
        }
        (Ok(if is_err { Err(()) } else { Ok(()) }), stmt_logging)
    }

    // Send a websocket Ping every second to verify the client is still
    // connected.
    fn connection_error(&mut self) -> BoxFuture<'_, Error> {
        Box::pin(async {
            let mut tick = time::interval(Duration::from_secs(1));
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(err) = self.send(Message::Ping(Vec::new().into())).await {
                    return err.into();
                }
            }
        })
    }

    fn allow_subscribe(&self) -> bool {
        true
    }

    async fn emit_streaming_notices(&mut self, notices: Vec<AdapterNotice>) -> Result<(), Error> {
        forward_notices(self, notices).await
    }
}

async fn await_rows<S, F, R>(sender: &mut S, client: &mut SessionClient, f: F) -> Result<R, Error>
where
    S: ResultSender,
    F: Future<Output = R> + Send,
{
    let mut f = pin!(f);
    loop {
        tokio::select! {
            notice = client.session().recv_notice(), if S::SUPPORTS_STREAMING_NOTICES => {
                sender.emit_streaming_notices(vec![notice]).await?;
            }
            e = sender.connection_error() => return Err(e),
            r = &mut f => return Ok(r),
        }
    }
}

/// Streams a peek (`SELECT`) result to a WebSocket client one stash batch at a
/// time, flushing between batches so a slow client applies real backpressure
/// and only one batch is resident. This gives a large `SELECT` over WebSocket
/// the same bounded memory profile as pgwire `SELECT`, and mirrors the
/// `Subscribe` arm of `WebSocket::add_result`.
///
/// On success returns the `(is_err, msgs, stmt_logging)` triple that
/// `add_result` folds into its response. An `Err` means a write to the socket
/// failed, so the server should disconnect. `add_result` turns that into a
/// cancellation to match pgwire.
///
/// The `Rows` descriptor is sent lazily, right before the first batch of rows
/// or an empty successful result, but never before an error. So a query that
/// fails before producing any rows emits only an `Error`.
async fn stream_ws_peek_rows(
    ws: &mut WebSocket,
    client: &mut SessionClient,
    desc: &RelationDesc,
    rows_stream: &mut RecordFirstRowStream,
    max_result_size: usize,
    ends_group: bool,
    returning_write: bool,
) -> Result<
    (
        bool,
        Vec<WebSocketResponse>,
        Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
    ),
    Error,
> {
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut result_size: usize = 0;
    let mut rows_returned: usize = 0;
    let mut sent_rows_desc = false;
    loop {
        // Bind before matching so the `Option<PeekResponseUnary>` (which has a
        // significant `Drop`) is not a temporary living for the whole match.
        let res = await_rows(ws, client, rows_stream.recv()).await?;
        match res {
            Some(PeekResponseUnary::Rows(mut rows)) => {
                if let Err(err) = verify_datum_desc(desc, &mut rows) {
                    return Ok(ws_peek_result(
                        client,
                        true,
                        vec![WebSocketResponse::Error(err.into())],
                    ));
                }
                // The header waits until a batch has passed `verify_datum_desc`,
                // so a query that fails before producing any rows emits only an
                // `Error`. Sending it before the loop, as the `Subscribe` arm
                // does, would put a `Rows` header in front of that `Error`.
                if !sent_rows_desc {
                    send_ws_response(ws, WebSocketResponse::Rows(desc.into())).await?;
                    sent_rows_desc = true;
                }
                let types = &desc.typ().column_types;
                while let Some(row) = rows.next() {
                    result_size = result_size.saturating_add(row.byte_len());
                    if result_size > max_result_size {
                        use bytesize::ByteSize;
                        return Ok(ws_peek_result(
                            client,
                            true,
                            vec![WebSocketResponse::Error(
                                AdapterError::ResultSize(format!(
                                    "result exceeds max size of {}",
                                    ByteSize::b(u64::cast_from(max_result_size))
                                ))
                                .into(),
                            )],
                        ));
                    }
                    let datums = datum_vec.borrow_with(row);
                    send_ws_response(
                        ws,
                        WebSocketResponse::Row(
                            datums
                                .iter()
                                .enumerate()
                                .map(|(i, d)| {
                                    TypedDatum::new(*d, &types[i])
                                        .json(&JsonNumberPolicy::ConvertNumberToString)
                                })
                                .collect(),
                        ),
                    )
                    .await?;
                    rows_returned += 1;
                }
            }
            Some(PeekResponseUnary::Error(error)) => {
                return Ok(ws_peek_result(
                    client,
                    true,
                    vec![WebSocketResponse::Error(error.into())],
                ));
            }
            Some(PeekResponseUnary::DependencyDropped(dep)) => {
                return Ok(ws_peek_result(
                    client,
                    true,
                    vec![WebSocketResponse::Error(
                        dep.to_concurrent_dependency_drop().into(),
                    )],
                ));
            }
            Some(PeekResponseUnary::Canceled) => {
                return Ok(ws_peek_result(
                    client,
                    true,
                    vec![WebSocketResponse::Error(AdapterError::Canceled.into())],
                ));
            }
            None => {
                let (notice, reverted) =
                    match finish_rows(client, rows_stream, ends_group, returning_write).await {
                        Ok(finished) => finished,
                        Err(err) => {
                            return Ok(ws_peek_result(
                                client,
                                true,
                                vec![WebSocketResponse::Error(err.into())],
                            ));
                        }
                    };
                // An empty successful result still owes the client a `Rows`
                // descriptor before `CommandComplete`.
                if !sent_rows_desc {
                    send_ws_response(ws, WebSocketResponse::Rows(desc.into())).await?;
                }
                let command_complete =
                    WebSocketResponse::CommandComplete(format!("SELECT {rows_returned}"));
                let msgs = notice
                    .map(|notice| WebSocketResponse::Notice(Notice::from(notice)))
                    .into_iter()
                    .chain([command_complete])
                    .collect();
                let (is_err, mut msgs, stmt_logging) = ws_peek_result(client, false, msgs);
                msgs.extend(reverted.into_iter().map(WebSocketResponse::ParameterStatus));
                return Ok((is_err, msgs, stmt_logging));
            }
        }
    }
}

/// Packages a terminal result of `stream_ws_peek_rows`, appending any notices
/// still buffered on the session after `msgs`.
///
/// The streaming loop forwards notices live through `await_rows`' `recv_notice`
/// select, but the terminal `recv() -> None` can race a still-buffered notice
/// and end the loop first. Draining here restores the buffered path's guarantee
/// that all notices reach the client, after `CommandComplete` or `Error`.
fn ws_peek_result(
    client: &mut SessionClient,
    is_err: bool,
    mut msgs: Vec<WebSocketResponse>,
) -> (
    bool,
    Vec<WebSocketResponse>,
    Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
) {
    msgs.extend(
        make_notices(client)
            .into_iter()
            .map(WebSocketResponse::Notice),
    );
    (is_err, msgs, None)
}

async fn send_and_retire<S: ResultSender>(
    res: StatementResult,
    client: &mut SessionClient,
    sender: &mut S,
) -> Result<Result<(), ()>, Error> {
    let (res, stmt_logging) = sender.add_result(client, res).await;
    if let Some((reason, ctx_extra)) = stmt_logging {
        client.retire_execute(ctx_extra, reason);
    }
    res
}

/// Returns Ok(Err) if any statement error'd during execution.
async fn execute_stmt_group<S: ResultSender>(
    client: &mut SessionClient,
    sender: &mut S,
    stmt_group: Vec<(Statement<Raw>, String, Vec<Option<String>>)>,
) -> Result<Result<(), ()>, Error> {
    let num_stmts = stmt_group.len();
    for (idx, (stmt, sql, params)) in stmt_group.into_iter().enumerate() {
        assert!(
            num_stmts <= 1 || params.is_empty(),
            "statement groups contain more than 1 statement iff Simple request, which does not support parameters"
        );

        let is_aborted_txn = matches!(client.session().transaction(), TransactionStatus::Failed(_));
        if is_aborted_txn && !is_txn_exit_stmt(&stmt) {
            let err = SqlResult::err(client, Error::AbortedTransaction);
            let _ = send_and_retire(err.into(), client, sender).await?;
            return Ok(Err(()));
        }

        // Mirror the behavior of the PostgreSQL simple query protocol.
        // See the pgwire::protocol::StateMachine::query method for details.
        if let Err(e) = client.start_transaction(Some(num_stmts)) {
            let err = SqlResult::err(client, e);
            let _ = send_and_retire(err.into(), client, sender).await?;
            return Ok(Err(()));
        }
        let ends_group = idx + 1 == num_stmts;
        let res = execute_stmt(client, sender, stmt, sql, params, ends_group).await?;
        let is_err = send_and_retire(res, client, sender).await?;

        if is_err.is_err() {
            // Mirror StateMachine::error, which sometimes will clean up the
            // transaction state instead of always leaving it in Failed.
            let txn = client.session().transaction();
            match txn {
                // Error can be called from describe and parse and so might not be in an active
                // transaction.
                TransactionStatus::Default | TransactionStatus::Failed(_) => {}
                // In Started (i.e., a single statement) and implicit transactions cleanup themselves.
                TransactionStatus::Started(_) | TransactionStatus::InTransactionImplicit(_) => {
                    if let Err(err) = client.end_transaction(EndTransactionAction::Rollback).await {
                        let err = SqlResult::err(client, err);
                        let _ = send_and_retire(err.into(), client, sender).await?;
                    }
                }
                // Explicit transactions move to failed.
                TransactionStatus::InTransaction(_) => {
                    client.fail_transaction();
                }
            }
            return Ok(Err(()));
        }
    }
    Ok(Ok(()))
}

/// Executes an entire [`SqlRequest`].
///
/// See the user-facing documentation about the HTTP API for a description of
/// the semantics of this function.
/// Executes a SQL request and sends results to the provided sender.
///
/// Made visible to http submodules (like mcp) via `pub(in crate::http)` to allow
/// reuse of SQL execution logic.
pub(in crate::http) async fn execute_request<S: ResultSender>(
    client: &mut AuthedClient,
    request: SqlRequest,
    sender: &mut S,
) -> Result<(), Error> {
    let client = &mut client.client;

    if client.statement_arrival_logging_enabled().await {
        let session = client.session();
        let conn_id = session.conn_id();
        let session_uuid = session.uuid();
        match &request {
            SqlRequest::Simple { query } => {
                info!(
                    %conn_id, %session_uuid, kind = "http_simple",
                    sql = %redact_sql_for_logging(query.as_str()),
                    "statement arrival"
                );
            }
            SqlRequest::Extended { queries } => {
                for ExtendedRequest { query, params } in queries {
                    // Parameter values are data that redaction cannot reach,
                    // so only their count is logged.
                    info!(
                        %conn_id, %session_uuid, kind = "http_extended",
                        sql = %redact_sql_for_logging(query), num_params = params.len(),
                        "statement arrival"
                    );
                }
            }
        }
    }

    // This API prohibits executing statements with responses whose
    // semantics are at odds with an HTTP response.
    fn check_prohibited_stmts<S: ResultSender>(
        sender: &S,
        stmt: &Statement<Raw>,
    ) -> Result<(), Error> {
        let kind: StatementKind = stmt.into();
        let execute_responses = Plan::generated_from(&kind)
            .into_iter()
            .map(ExecuteResponse::generated_from)
            .flatten()
            .collect::<Vec<_>>();

        // Special-case `COPY TO` statements that are not `COPY ... TO STDOUT`, since
        // StatementKind::Copy links to several `ExecuteResponseKind`s that are not supported,
        // but this specific statement should be allowed.
        let is_valid_copy = matches!(
            stmt,
            Statement::Copy(CopyStatement {
                direction: CopyDirection::To,
                target: CopyTarget::Expr(_),
                ..
            }) | Statement::Copy(CopyStatement {
                direction: CopyDirection::From,
                target: CopyTarget::Expr(_),
                ..
            })
        );

        if !is_valid_copy
            && execute_responses.iter().any(|execute_response| {
                // Returns true if a statement or execute response are unsupported.
                match execute_response {
                    ExecuteResponseKind::Subscribing if sender.allow_subscribe() => false,
                    ExecuteResponseKind::Fetch
                    | ExecuteResponseKind::Subscribing
                    | ExecuteResponseKind::CopyFrom
                    | ExecuteResponseKind::DeclaredCursor
                    | ExecuteResponseKind::ClosedCursor => true,
                    // Various statements generate `PeekPlan` (`SELECT`, `COPY`,
                    // `EXPLAIN`, `SHOW`) which has both `SendRows` and `CopyTo` as its
                    // possible response types. but `COPY` needs be picked out because
                    // http don't support its response type
                    ExecuteResponseKind::CopyTo if matches!(kind, StatementKind::Copy) => true,
                    _ => false,
                }
            })
        {
            return Err(Error::Unsupported(stmt.to_ast_string_simple()));
        }
        Ok(())
    }

    fn parse<'a>(
        client: &SessionClient,
        query: &'a str,
    ) -> Result<Vec<StatementParseResult<'a>>, Error> {
        let result = client
            .parse(query)
            .map_err(|e| Error::Unstructured(anyhow!(e)))?;
        result.map_err(|e| AdapterError::from(e).into())
    }

    let mut stmt_groups = vec![];

    match request {
        SqlRequest::Simple { query } => match parse(client, query.as_str()) {
            Ok(stmts) => {
                let mut stmt_group = Vec::with_capacity(stmts.len());
                let mut stmt_err = None;
                for StatementParseResult { ast: stmt, sql } in stmts {
                    if let Err(err) = check_prohibited_stmts(sender, &stmt) {
                        stmt_err = Some(err);
                        break;
                    }
                    stmt_group.push((stmt, sql.to_string(), vec![]));
                }
                stmt_groups.push(stmt_err.map(Err).unwrap_or_else(|| Ok(stmt_group)));
            }
            Err(e) => stmt_groups.push(Err(e)),
        },
        SqlRequest::Extended { queries } => {
            for ExtendedRequest { query, params } in queries {
                match parse(client, &query) {
                    Ok(mut stmts) => {
                        if stmts.len() != 1 {
                            return Err(Error::Unstructured(anyhow!(
                                "each query must contain exactly 1 statement, but \"{}\" contains {}",
                                query,
                                stmts.len()
                            )));
                        }

                        let StatementParseResult { ast: stmt, sql } = stmts.pop().unwrap();
                        stmt_groups.push(
                            check_prohibited_stmts(sender, &stmt)
                                .map(|_| vec![(stmt, sql.to_string(), params)]),
                        );
                    }
                    Err(e) => stmt_groups.push(Err(e)),
                };
            }
        }
    }

    for stmt_group_res in stmt_groups {
        let executed = match stmt_group_res {
            Ok(stmt_group) => execute_stmt_group(client, sender, stmt_group).await,
            Err(e) => {
                let err = SqlResult::err(client, e);
                let _ = send_and_retire(err.into(), client, sender).await?;
                Ok(Err(()))
            }
        };
        // The statement that ends a group commits its implicit transaction before its result
        // (`commit_if_ends_group`). This commits an implicit transaction that a group left open:
        // a group that ends in a SUBSCRIBE, or that returned early through `?`.
        if client.session().transaction().is_implicit() {
            let ended = client.end_transaction(EndTransactionAction::Commit).await;
            if let Err(err) = ended {
                let err = SqlResult::err(client, err);
                let _ = send_and_retire(StatementResult::SqlResult(err), client, sender).await?;
            }
        }
        if executed?.is_err() {
            break;
        }
    }

    Ok(())
}

/// Executes a single statement in a [`SqlRequest`].
async fn execute_stmt<S: ResultSender>(
    client: &mut SessionClient,
    sender: &mut S,
    stmt: Statement<Raw>,
    sql: String,
    raw_params: Vec<Option<String>>,
    ends_group: bool,
) -> Result<StatementResult, Error> {
    const EMPTY_PORTAL: &str = "";
    // TODO: this classifies by the outer statement, so an `EXECUTE` of a prepared write with
    // `RETURNING` is timed as a read.
    let returning_write = matches!(
        stmt,
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    );
    if let Err(e) = client
        .prepare(EMPTY_PORTAL.into(), Some(stmt.clone()), sql, vec![])
        .await
    {
        return Ok(SqlResult::err(client, e).into());
    }

    let prep_stmt = match client.get_prepared_statement(EMPTY_PORTAL).await {
        Ok(stmt) => stmt,
        Err(err) => {
            return Ok(SqlResult::err(client, err).into());
        }
    };

    let param_types = &prep_stmt.desc().param_types;
    if param_types.len() != raw_params.len() {
        let message = anyhow!(
            "request supplied {actual} parameters, \
                        but {statement} requires {expected}",
            statement = stmt.to_ast_string_simple(),
            actual = raw_params.len(),
            expected = param_types.len()
        );
        return Ok(SqlResult::err(client, Error::Unstructured(message)).into());
    }

    let buf = RowArena::new();
    let mut params = vec![];
    for (raw_param, mz_typ) in raw_params.into_iter().zip_eq(param_types) {
        let pg_typ = mz_pgrepr::Type::from(mz_typ);
        let datum = match raw_param {
            None => Datum::Null,
            Some(raw_param) => {
                match mz_pgrepr::Value::decode(
                    mz_pgwire_common::Format::Text,
                    &pg_typ,
                    raw_param.as_bytes(),
                ) {
                    Ok(param) => match param.into_datum_decode_error(&buf, &pg_typ, "parameter") {
                        Ok(datum) => datum,
                        Err(msg) => {
                            return Ok(
                                SqlResult::err(client, Error::Unstructured(anyhow!(msg))).into()
                            );
                        }
                    },
                    Err(err) => {
                        let msg = anyhow!("unable to decode parameter: {}", err);
                        return Ok(SqlResult::err(client, Error::Unstructured(msg)).into());
                    }
                }
            }
        };
        params.push((datum, mz_typ.clone()))
    }

    let result_formats = vec![
        mz_pgwire_common::Format::Text;
        prep_stmt
            .desc()
            .relation_desc
            .clone()
            .map(|desc| desc.typ().column_types.len())
            .unwrap_or(0)
    ];

    let desc = prep_stmt.desc().clone();
    let logging = Arc::clone(prep_stmt.logging());
    let stmt_ast = prep_stmt.stmt().cloned();
    let state_revision = prep_stmt.state_revision;
    if let Err(err) = client.session().set_portal(
        EMPTY_PORTAL.into(),
        desc,
        stmt_ast,
        logging,
        params,
        result_formats,
        state_revision,
    ) {
        return Ok(SqlResult::err(client, err).into());
    }

    let desc = client
        .session()
        // We do not need to verify here because `client.execute` verifies below.
        .get_portal_unverified(EMPTY_PORTAL)
        .map(|portal| portal.desc.clone())
        .expect("unnamed portal should be present");

    let res = client
        .execute(EMPTY_PORTAL.into(), futures::future::pending(), None)
        .await;
    // The execution time of a statement without rows ends here, before notices are sent.
    let executed = Instant::now();

    if S::SUPPORTS_STREAMING_NOTICES {
        sender
            .emit_streaming_notices(client.session().drain_notices())
            .await?;
    }

    let (res, execute_started) = match res {
        Ok(res) => res,
        Err(e) => {
            return Ok(SqlResult::err(client, e).into());
        }
    };
    let execution = executed.duration_since(execute_started);
    let tag = res.tag();
    let write = ExecutionTimeKind::is_write(&res, client.session());

    Ok(match res {
        ExecuteResponse::CreatedConnection { .. }
        | ExecuteResponse::CreatedDatabase { .. }
        | ExecuteResponse::CreatedSchema { .. }
        | ExecuteResponse::CreatedRole
        | ExecuteResponse::CreatedCluster { .. }
        | ExecuteResponse::CreatedClusterReplica { .. }
        | ExecuteResponse::CreatedTable { .. }
        | ExecuteResponse::CreatedIndex { .. }
        | ExecuteResponse::CreatedMetricSink { .. }
        | ExecuteResponse::CreatedIntrospectionSubscribe
        | ExecuteResponse::CreatedSecret { .. }
        | ExecuteResponse::CreatedSource { .. }
        | ExecuteResponse::CreatedSink { .. }
        | ExecuteResponse::CreatedView { .. }
        | ExecuteResponse::CreatedViews { .. }
        | ExecuteResponse::CreatedMaterializedView { .. }
        | ExecuteResponse::CreatedType
        | ExecuteResponse::CreatedNetworkPolicy
        | ExecuteResponse::Comment
        | ExecuteResponse::Deleted(_)
        | ExecuteResponse::DiscardedTemp
        | ExecuteResponse::DroppedObject(_)
        | ExecuteResponse::DroppedOwned
        | ExecuteResponse::EmptyQuery
        | ExecuteResponse::GrantedPrivilege
        | ExecuteResponse::GrantedRole
        | ExecuteResponse::Inserted(_)
        | ExecuteResponse::Copied(_)
        | ExecuteResponse::Raised
        | ExecuteResponse::ReassignOwned
        | ExecuteResponse::RevokedPrivilege
        | ExecuteResponse::AlteredDefaultPrivileges
        | ExecuteResponse::RevokedRole
        | ExecuteResponse::StartedTransaction { .. }
        | ExecuteResponse::Updated(_)
        | ExecuteResponse::AlteredObject(_)
        | ExecuteResponse::AlteredRole
        | ExecuteResponse::AlteredSystemConfiguration
        | ExecuteResponse::Deallocate { .. }
        | ExecuteResponse::ValidatedConnection
        | ExecuteResponse::Prepare => SqlResult::complete(
            client,
            tag.expect("ok only called on tag-generating results"),
            Vec::default(),
            execution,
            ends_group,
            write,
        )
        .await
        .into(),
        ExecuteResponse::TransactionCommitted { params }
        | ExecuteResponse::TransactionRolledBack { params }
        | ExecuteResponse::DiscardedAll { params } => {
            let params = notify_params(client, params);
            SqlResult::complete(
                client,
                tag.expect("ok only called on tag-generating results"),
                params,
                execution,
                ends_group,
                write,
            )
            .await
            .into()
        }
        ExecuteResponse::SetVariable { name, .. } => {
            let mut params = Vec::with_capacity(1);
            if let Some(var) = client
                .session()
                .vars()
                .notify_set()
                .find(|v| v.name() == &name)
            {
                params.push(ParameterStatus {
                    name,
                    value: var.value(),
                });
            };
            SqlResult::complete(
                client,
                tag.expect("ok only called on tag-generating results"),
                params,
                execution,
                ends_group,
                write,
            )
            .await
            .into()
        }
        ExecuteResponse::SendingRowsStreaming {
            rows,
            instance_id,
            strategy,
        } => {
            let max_result_size = max_result_size(client).await;

            let rows_stream = RecordFirstRowStream::new(
                Box::new(rows),
                execute_started,
                client,
                Some(instance_id),
                Some(strategy),
            );

            StatementResult::Rows {
                desc: desc.relation_desc.expect("RelationDesc must exist"),
                rows_stream,
                max_result_size,
                ends_group,
                returning_write,
            }
        }
        ExecuteResponse::SendingRowsImmediate { rows } => {
            let max_result_size = max_result_size(client).await;

            let rows = futures::stream::once(futures::future::ready(PeekResponseUnary::Rows(rows)));
            let rows_stream = RecordFirstRowStream::new(
                Box::new(rows),
                execute_started,
                client,
                None,
                Some(StatementExecutionStrategy::Constant),
            );

            StatementResult::Rows {
                desc: desc.relation_desc.expect("RelationDesc must exist"),
                rows_stream,
                max_result_size,
                ends_group,
                returning_write,
            }
        }
        ExecuteResponse::Subscribing {
            rx,
            ctx_extra,
            instance_id,
        } => StatementResult::Subscribe {
            tag: "SUBSCRIBE".into(),
            desc: desc.relation_desc.unwrap(),
            rx: RecordFirstRowStream::new(rx, execute_started, client, Some(instance_id), None),
            ctx_extra,
        },
        res @ (ExecuteResponse::Fetch { .. }
        | ExecuteResponse::CopyTo { .. }
        | ExecuteResponse::CopyFrom { .. }
        | ExecuteResponse::DeclaredCursor
        | ExecuteResponse::ClosedCursor) => SqlResult::err(
            client,
            Error::Unstructured(anyhow!(
                "internal error: encountered prohibited ExecuteResponse {:?}.\n\n
            This is a bug. Can you please file an bug report letting us know?\n
            https://github.com/MaterializeInc/materialize/discussions/new?category=bug-reports",
                ExecuteResponseKind::from(res)
            )),
        )
        .into(),
    })
}

/// Commits the implicit transaction if the statement ends its statement group, and returns
/// the parameters the commit reverted.
///
/// Callers must report the statement's completion only after this returns, and an error as
/// the statement's result: PostgreSQL commits before the last statement's `CommandComplete`,
/// so a client sees the statement's success or the commit's failure, never both.
async fn commit_if_ends_group(
    client: &mut SessionClient,
    ends_group: bool,
) -> Result<Vec<ParameterStatus>, AdapterError> {
    if !ends_group || !client.session().transaction().is_implicit() {
        return Ok(Vec::new());
    }
    let response = client.end_transaction(EndTransactionAction::Commit).await?;
    match response {
        ExecuteResponse::TransactionCommitted { params } => Ok(notify_params(client, params)),
        _ => Ok(Vec::new()),
    }
}

/// Runs [`commit_if_ends_group`] for a statement whose rows are exhausted, and returns the
/// opted-in execution time notice and the parameters the commit reverted.
async fn finish_rows(
    client: &mut SessionClient,
    rows_stream: &mut RecordFirstRowStream,
    ends_group: bool,
    returning_write: bool,
) -> Result<(Option<AdapterNotice>, Vec<ParameterStatus>), AdapterError> {
    let commit_started = Instant::now();
    let reverted = commit_if_ends_group(client, ends_group).await?;
    let commit = commit_started.elapsed();
    let notice = rows_stream.take_execution_time().and_then(|time| {
        let time = if returning_write {
            time.for_returning_write(client.session().has_staged_writes(), commit)
        } else {
            time
        };
        client.session().execution_time_notice(time)
    });
    Ok((notice, reverted))
}

/// The `max_result_size` of a statement's result.
///
/// Reads the session's cached catalog, which needs no coordinator round trip unless the catalog
/// changed: such a round trip lands between execution and the first row, inside the time to
/// first row.
async fn max_result_size(client: &mut SessionClient) -> usize {
    let catalog = client.catalog_snapshot("http_max_result_size").await;
    usize::cast_from(catalog.system_config().max_result_size())
}

/// The session parameters among `params` that clients are notified about.
fn notify_params(
    client: &mut SessionClient,
    params: BTreeMap<&'static str, String>,
) -> Vec<ParameterStatus> {
    let notify_set: mz_ore::collections::HashSet<_> = client
        .session()
        .vars()
        .notify_set()
        .map(|v| v.name().to_string())
        .collect();
    params
        .into_iter()
        .filter(|(name, _value)| notify_set.contains(*name))
        .map(|(name, value)| ParameterStatus {
            name: name.to_string(),
            value,
        })
        .collect()
}

fn make_notices(client: &mut SessionClient) -> Vec<Notice> {
    client
        .session()
        .drain_notices()
        .into_iter()
        .map(Notice::from)
        .collect()
}

impl From<AdapterNotice> for Notice {
    fn from(notice: AdapterNotice) -> Self {
        Notice {
            message: notice.to_string(),
            code: notice.code().code().to_string(),
            severity: notice.severity().as_str().to_lowercase(),
            detail: notice.detail(),
            hint: notice.hint(),
        }
    }
}

// Duplicated from protocol.rs.
// See postgres' backend/tcop/postgres.c IsTransactionExitStmt.
fn is_txn_exit_stmt(stmt: &Statement<Raw>) -> bool {
    matches!(
        stmt,
        Statement::Commit(_) | Statement::Rollback(_) | Statement::Prepare(_)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Password, WebSocketAuth};

    #[mz_ore::test]
    fn smoke_test_websocket_auth_parse() {
        struct TestCase {
            json: &'static str,
            expected: WebSocketAuth,
        }

        let test_cases = vec![
            TestCase {
                json: r#"{ "user": "mz", "password": "1234" }"#,
                expected: WebSocketAuth::Basic {
                    user: "mz".to_string(),
                    password: Password("1234".to_string()),
                    options: BTreeMap::default(),
                },
            },
            TestCase {
                json: r#"{ "user": "mz", "password": "1234", "options": {} }"#,
                expected: WebSocketAuth::Basic {
                    user: "mz".to_string(),
                    password: Password("1234".to_string()),
                    options: BTreeMap::default(),
                },
            },
            TestCase {
                json: r#"{ "token": "i_am_a_token" }"#,
                expected: WebSocketAuth::Bearer {
                    token: "i_am_a_token".to_string(),
                    options: BTreeMap::default(),
                },
            },
            TestCase {
                json: r#"{ "token": "i_am_a_token", "options": { "foo": "bar" } }"#,
                expected: WebSocketAuth::Bearer {
                    token: "i_am_a_token".to_string(),
                    options: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
                },
            },
        ];

        fn assert_parse(json: &'static str, expected: WebSocketAuth) {
            let parsed: WebSocketAuth = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected);
        }

        for TestCase { json, expected } in test_cases {
            assert_parse(json, expected)
        }
    }
}
