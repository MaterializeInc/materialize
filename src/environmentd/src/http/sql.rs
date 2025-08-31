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
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use futures::Future;
use futures::future::BoxFuture;

use http::StatusCode;
use itertools::izip;
use mz_adapter::client::RecordFirstRowStream;
use mz_adapter::session::{EndTransactionAction, TransactionStatus};
use mz_adapter::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};
use mz_adapter::{
    AdapterError, AdapterNotice, ExecuteContextExtra, ExecuteResponse, ExecuteResponseKind,
    PeekResponseUnary, SessionClient, verify_datum_desc,
};
use mz_auth::password::Password;
use mz_catalog::memory::objects::{Cluster, ClusterReplica};
use mz_interchange::encode::TypedDatum;
use mz_interchange::json::{JsonNumberPolicy, ToJson};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{MakeCollectorOpts, MetricsRegistry};
use mz_ore::result::ResultExt;
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
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower_sessions::Session as TowerSession;
use tracing::{debug, error, info};
use tungstenite::protocol::frame::coding::CloseCode;

use crate::http::prometheus::PrometheusSqlQuery;
use crate::http::{
    AuthError, AuthedClient, AuthedUser, MAX_REQUEST_SIZE, SESSION_DURATION, TowerSessionData,
    WsState, init_ws,
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
        let mut label_values = desc
            .columns
            .iter()
            .zip(row)
            .filter(|(col, _)| col.name != query.value_column_name)
            .map(|(_, val)| val.as_str().expect("must be string"))
            .collect::<Vec<_>>();

        let value = desc
            .columns
            .iter()
            .zip(row)
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

pub async fn handle_sql_ws(
    State(state): State<WsState>,
    existing_user: Option<Extension<AuthedUser>>,
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    tower_session: Option<Extension<TowerSession>>,
) -> impl IntoResponse {
    // An upstream middleware may have already provided the user for us
    let user = match (existing_user, tower_session) {
        (Some(Extension(user)), _) => Some(user),
        (None, Some(session)) => {
            if let Ok(Some(session_data)) = session.get::<TowerSessionData>("data").await {
                // Check session expiration
                if session_data
                    .last_activity
                    .elapsed()
                    .unwrap_or(Duration::MAX)
                    > SESSION_DURATION
                {
                    let _ = session.delete().await;
                    return Err(AuthError::SessionExpired);
                }
                // Update last activity
                let mut updated_data = session_data.clone();
                updated_data.last_activity = SystemTime::now();
                session
                    .insert("data", &updated_data)
                    .await
                    .map_err(|_| AuthError::FailedToUpdateSession)?;
                // User is authenticated via session
                Some(AuthedUser {
                    name: session_data.username,
                    external_metadata_rx: None,
                })
            } else {
                None
            }
        }
        _ => None,
    };

    let addr = Box::new(addr.ip());
    Ok(ws
        .max_message_size(MAX_REQUEST_SIZE)
        .on_upgrade(|ws| async move { run_ws(&state, user, *addr, ws).await }))
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

async fn run_ws(state: &WsState, user: Option<AuthedUser>, peer_addr: IpAddr, mut ws: WebSocket) {
    let mut client = match init_ws(state, user, peer_addr, &mut ws).await {
        Ok(client) => client,
        Err(e) => {
            // We omit most detail from the error message we send to the client, to
            // avoid giving attackers unnecessary information during auth. AdapterErrors
            // are safe to return because they're generated after authentication.
            debug!("WS request failed init: {}", e);
            let reason = match e.downcast_ref::<AdapterError>() {
                Some(error) => Cow::Owned(error.to_string()),
                None => "unauthorized".into(),
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
                serde_json::to_string(&msg).expect("must serialize"),
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
    let msg = Message::Text(msg);
    ws.send(msg).await?;

    Ok(())
}

/// Forwards a collection of Notices to the provided [`WebSocket`].
async fn forward_notices(
    ws: &mut WebSocket,
    notices: impl IntoIterator<Item = AdapterNotice>,
) -> Result<(), Error> {
    let ws_notices = notices.into_iter().map(|notice| {
        WebSocketResponse::Notice(Notice {
            message: notice.to_string(),
            code: notice.code().code().to_string(),
            severity: notice.severity().as_str().to_lowercase(),
            detail: notice.detail(),
            hint: notice.hint(),
        })
    });

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
        query: String,
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
    results: Vec<SqlResult>,
}

enum StatementResult {
    SqlResult(SqlResult),
    Subscribe {
        desc: RelationDesc,
        tag: String,
        rx: RecordFirstRowStream,
        ctx_extra: ExecuteContextExtra,
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
        /// The result rows.
        rows: Vec<Vec<serde_json::Value>>,
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
    /// Convert adapter Row results into the web row result format. Error if the row format does not
    /// match the expected descriptor.
    // TODO(aljoscha): Bail when max_result_size is exceeded.
    async fn rows<S>(
        sender: &mut S,
        client: &mut SessionClient,
        mut rows_stream: RecordFirstRowStream,
        max_query_result_size: usize,
        desc: &RelationDesc,
    ) -> Result<SqlResult, Error>
    where
        S: ResultSender,
    {
        let mut rows: Vec<Vec<serde_json::Value>> = vec![];
        let mut datum_vec = mz_repr::DatumVec::new();
        let types = &desc.typ().column_types;

        let mut query_result_size = 0;

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
                    return Ok(SqlResult::err(client, Error::Unstructured(anyhow!(e))));
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
                query_result_size += row.byte_len();
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
                rows.push(
                    datums
                        .iter()
                        .enumerate()
                        .map(|(i, d)| {
                            TypedDatum::new(*d, &types[i])
                                .json(&JsonNumberPolicy::ConvertNumberToString)
                        })
                        .collect(),
                );
            }
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
trait ResultSender: Send {
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
        Option<(StatementEndedExecutionReason, ExecuteContextExtra)>,
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
        _client: &mut SessionClient,
        res: StatementResult,
    ) -> (
        Result<Result<(), ()>, Error>,
        Option<(StatementEndedExecutionReason, ExecuteContextExtra)>,
    ) {
        let (res, stmt_logging) = match res {
            StatementResult::SqlResult(res) => {
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
        Option<(StatementEndedExecutionReason, ExecuteContextExtra)>,
    ) {
        let (has_rows, is_streaming) = match res {
            StatementResult::SqlResult(SqlResult::Err { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Ok { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Rows { .. }) => (true, false),
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
            StatementResult::SqlResult(SqlResult::Rows {
                tag,
                rows,
                desc,
                notices,
            }) => {
                let mut msgs = vec![WebSocketResponse::Rows(desc)];
                msgs.extend(rows.into_iter().map(WebSocketResponse::Row));
                msgs.push(WebSocketResponse::CommandComplete(tag));
                msgs.extend(notices.into_iter().map(WebSocketResponse::Notice));
                (false, msgs, None)
            }
            StatementResult::SqlResult(SqlResult::Ok {
                ok,
                parameters,
                notices,
            }) => {
                let mut msgs = vec![WebSocketResponse::CommandComplete(ok)];
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
                                result_size += row.byte_len();
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
                        Some(PeekResponseUnary::Error(error)) => {
                            break (
                                true,
                                vec![WebSocketResponse::Error(
                                    Error::Unstructured(anyhow!(error.clone())).into(),
                                )],
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
                if let Err(err) = self.send(Message::Ping(Vec::new())).await {
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
    for (stmt, sql, params) in stmt_group {
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
        let res = execute_stmt(client, sender, stmt, sql, params).await?;
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
async fn execute_request<S: ResultSender>(
    client: &mut AuthedClient,
    request: SqlRequest,
    sender: &mut S,
) -> Result<(), Error> {
    let client = &mut client.client;

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
        SqlRequest::Simple { query } => match parse(client, &query) {
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
        // At the end of each group, commit implicit transactions. Do that here so that any `?`
        // early return can still be handled here.
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
) -> Result<StatementResult, Error> {
    const EMPTY_PORTAL: &str = "";
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
    for (raw_param, mz_typ) in izip!(raw_params, param_types) {
        let pg_typ = mz_pgrepr::Type::from(mz_typ);
        let datum = match raw_param {
            None => Datum::Null,
            Some(raw_param) => {
                match mz_pgrepr::Value::decode(
                    mz_pgwire_common::Format::Text,
                    &pg_typ,
                    raw_param.as_bytes(),
                ) {
                    Ok(param) => param.into_datum(&buf, &pg_typ),
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
    let catalog_revision = prep_stmt.catalog_revision;
    let stmt = prep_stmt.stmt().cloned();
    let session_state_revision = client.session().state_revision();
    if let Err(err) = client.session().set_portal(
        EMPTY_PORTAL.into(),
        desc,
        stmt,
        logging,
        params,
        result_formats,
        catalog_revision,
        session_state_revision,
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
    let tag = res.tag();

    Ok(match res {
        ExecuteResponse::CreatedConnection { .. }
        | ExecuteResponse::CreatedDatabase { .. }
        | ExecuteResponse::CreatedSchema { .. }
        | ExecuteResponse::CreatedRole
        | ExecuteResponse::CreatedCluster { .. }
        | ExecuteResponse::CreatedClusterReplica { .. }
        | ExecuteResponse::CreatedTable { .. }
        | ExecuteResponse::CreatedIndex { .. }
        | ExecuteResponse::CreatedIntrospectionSubscribe
        | ExecuteResponse::CreatedSecret { .. }
        | ExecuteResponse::CreatedSource { .. }
        | ExecuteResponse::CreatedSink { .. }
        | ExecuteResponse::CreatedView { .. }
        | ExecuteResponse::CreatedViews { .. }
        | ExecuteResponse::CreatedMaterializedView { .. }
        | ExecuteResponse::CreatedContinualTask { .. }
        | ExecuteResponse::CreatedType
        | ExecuteResponse::CreatedNetworkPolicy
        | ExecuteResponse::Comment
        | ExecuteResponse::Deleted(_)
        | ExecuteResponse::DiscardedTemp
        | ExecuteResponse::DiscardedAll
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
        | ExecuteResponse::Prepare => SqlResult::ok(
            client,
            tag.expect("ok only called on tag-generating results"),
            Vec::default(),
        )
        .into(),
        ExecuteResponse::TransactionCommitted { params }
        | ExecuteResponse::TransactionRolledBack { params } => {
            let notify_set: mz_ore::collections::HashSet<_> = client
                .session()
                .vars()
                .notify_set()
                .map(|v| v.name().to_string())
                .collect();
            let params = params
                .into_iter()
                .filter(|(name, _value)| notify_set.contains(*name))
                .map(|(name, value)| ParameterStatus {
                    name: name.to_string(),
                    value,
                })
                .collect();
            SqlResult::ok(
                client,
                tag.expect("ok only called on tag-generating results"),
                params,
            )
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
            SqlResult::ok(
                client,
                tag.expect("ok only called on tag-generating results"),
                params,
            )
            .into()
        }
        ExecuteResponse::SendingRowsStreaming {
            rows,
            instance_id,
            strategy,
        } => {
            let max_query_result_size =
                usize::cast_from(client.get_system_vars().await.max_result_size());

            let rows_stream = RecordFirstRowStream::new(
                Box::new(rows),
                execute_started,
                client,
                Some(instance_id),
                Some(strategy),
            );

            SqlResult::rows(
                sender,
                client,
                rows_stream,
                max_query_result_size,
                &desc.relation_desc.expect("RelationDesc must exist"),
            )
            .await?
            .into()
        }
        ExecuteResponse::SendingRowsImmediate { rows } => {
            let max_query_result_size =
                usize::cast_from(client.get_system_vars().await.max_result_size());

            let rows = futures::stream::once(futures::future::ready(PeekResponseUnary::Rows(rows)));
            let rows_stream =
                RecordFirstRowStream::new(Box::new(rows), execute_started, client, None, None);

            SqlResult::rows(
                sender,
                client,
                rows_stream,
                max_query_result_size,
                &desc.relation_desc.expect("RelationDesc must exist"),
            )
            .await?
            .into()
        }
        ExecuteResponse::Subscribing {
            rx,
            ctx_extra,
            instance_id,
        } => StatementResult::Subscribe {
            tag: "SUBSCRIBE".into(),
            desc: desc.relation_desc.unwrap(),
            rx: RecordFirstRowStream::new(
                Box::new(UnboundedReceiverStream::new(rx)),
                execute_started,
                client,
                Some(instance_id),
                None,
            ),
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

fn make_notices(client: &mut SessionClient) -> Vec<Notice> {
    client
        .session()
        .drain_notices()
        .into_iter()
        .map(|notice| Notice {
            message: notice.to_string(),
            code: notice.code().code().to_string(),
            severity: notice.severity().as_str().to_lowercase(),
            detail: notice.detail(),
            hint: notice.hint(),
        })
        .collect()
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
