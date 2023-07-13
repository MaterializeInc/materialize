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
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::Json;
use futures::future::BoxFuture;
use futures::Future;
use http::StatusCode;
use itertools::izip;
use mz_adapter::session::{EndTransactionAction, RowBatchStream, TransactionStatus};
use mz_adapter::{
    AdapterError, AdapterNotice, ExecuteResponse, ExecuteResponseKind, PeekResponseUnary,
    SessionClient,
};
use mz_interchange::encode::TypedDatum;
use mz_interchange::json::ToJson;
use mz_ore::result::ResultExt;
use mz_pgwire::Severity;
use mz_repr::{Datum, RelationDesc, RowArena};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{Raw, Statement, StatementKind};
use mz_sql::plan::Plan;
use serde::{Deserialize, Serialize};
use tokio::{select, time};
use tokio_postgres::error::SqlState;
use tracing::debug;
use tungstenite::protocol::frame::coding::CloseCode;

use crate::http::{init_ws, AuthedClient, WsState, MAX_REQUEST_SIZE};

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

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn handle_sql_ws(
    State(state): State<WsState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.max_message_size(MAX_REQUEST_SIZE)
        .on_upgrade(|ws| async move { run_ws(&state, ws).await })
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum WebSocketAuth {
    Basic {
        user: String,
        password: String,
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
    Bearer {
        token: String,
        #[serde(default)]
        options: BTreeMap<String, String>,
    },
}

async fn run_ws(state: &WsState, mut ws: WebSocket) {
    let mut client = match init_ws(state, &mut ws).await {
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
                let err = AdapterError::from(timeout);
                let _ = send_ws_response(&mut ws, WebSocketResponse::Error(err.into())).await;
                return;
            },
            message = ws.recv() => message,
        };

        client.client.remove_idle_in_transaction_session_timeout();
        client.client.reset_canceled();

        let msg = match msg {
            Some(Ok(msg)) => msg,
            _ => {
                // client disconnected
                return;
            }
        };

        let req: Result<SqlRequest, anyhow::Error> = match msg {
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

            Ok::<_, anyhow::Error>(())
        };

        if let Err(err) = ws_response().await {
            debug!("failed to send response over WebSocket, {err:?}");
            return;
        }
    }
}

async fn run_ws_request(
    req: Result<SqlRequest, anyhow::Error>,
    client: &mut AuthedClient,
    ws: &mut WebSocket,
) -> Result<(), anyhow::Error> {
    let req = req?;
    execute_request(client, req, ws).await
}

/// Sends a single [`WebSocketResponse`] over the provided [`WebSocket`].
async fn send_ws_response(
    ws: &mut WebSocket,
    resp: WebSocketResponse,
) -> Result<(), anyhow::Error> {
    let msg = serde_json::to_string(&resp).unwrap();
    let msg = Message::Text(msg);
    ws.send(msg).await?;

    Ok(())
}

/// Forwards a collection of Notices to the provided [`WebSocket`].
async fn forward_notices(
    ws: &mut WebSocket,
    notices: impl IntoIterator<Item = AdapterNotice>,
) -> Result<(), anyhow::Error> {
    let ws_notices = notices.into_iter().map(|notice| {
        WebSocketResponse::Notice(Notice {
            message: notice.to_string(),
            severity: Severity::for_adapter_notice(&notice)
                .as_str()
                .to_lowercase(),
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
        rx: RowBatchStream,
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
    fn rows(
        client: &mut SessionClient,
        tag: String,
        rows: Vec<Vec<serde_json::Value>>,
        desc: RelationDesc,
    ) -> SqlResult {
        SqlResult::Rows {
            tag,
            rows,
            desc: Description::from(&desc),
            notices: make_notices(client),
        }
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
}

impl From<AdapterError> for SqlError {
    fn from(err: AdapterError) -> Self {
        SqlError {
            message: err.to_string(),
            code: err.code().code().to_string(),
            detail: err.detail(),
            hint: err.hint(),
        }
    }
}

impl From<String> for SqlError {
    fn from(message: String) -> Self {
        SqlError {
            message,
            code: SqlState::INTERNAL_ERROR.code().to_string(),
            detail: None,
            hint: None,
        }
    }
}

impl From<&str> for SqlError {
    fn from(value: &str) -> Self {
        SqlError::from(value.to_string())
    }
}

impl From<anyhow::Error> for SqlError {
    fn from(value: anyhow::Error) -> Self {
        SqlError::from(value.to_string())
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
    /// Adds a result to the client. `canceled` is a function that returns a future that resolves if
    /// a cancellation request was issued for this connection. Returns Err if sending to the client
    /// produced an error and the server should disconnect. Returns Ok(Err) if the statement
    /// produced an error and should error the transaction, but remain connected. Returns Ok(Ok(()))
    /// if the statement succeeded.
    async fn add_result<C, F>(
        &mut self,
        canceled: C,
        res: StatementResult,
    ) -> Result<Result<(), ()>, anyhow::Error>
    where
        C: Fn() -> F + Send + Sync,
        F: Future<Output = ()> + Send;
    /// Returns a future that resolves only when the client connection has gone away.
    fn connection_error(&mut self) -> BoxFuture<anyhow::Error>;
    /// Reports whether the client supports streaming SUBSCRIBE results.
    fn allow_subscribe(&self) -> bool;

    async fn await_rows<C, F, R>(&mut self, cancelled: C, f: F) -> Result<R, anyhow::Error>
    where
        C: Future<Output = ()> + Send,
        F: Future<Output = R> + Send,
    {
        tokio::select! {
            _ = cancelled => anyhow::bail!("query canceled"),
            e = self.connection_error() => Err(e),
            r = f => Ok(r),
        }
    }
}

#[async_trait]
impl ResultSender for SqlResponse {
    async fn add_result<C, F>(
        &mut self,
        _canceled: C,
        res: StatementResult,
    ) -> Result<Result<(), ()>, anyhow::Error>
    where
        C: Fn() -> F + Send + Sync,
        F: Future<Output = ()> + Send,
    {
        Ok(match res {
            StatementResult::SqlResult(res) => {
                let is_err = matches!(res, SqlResult::Err { .. });
                self.results.push(res);
                if is_err {
                    Err(())
                } else {
                    Ok(())
                }
            }
            StatementResult::Subscribe { .. } => {
                self.results.push(SqlResult::Err {
                    error: "SUBSCRIBE only supported over websocket".into(),
                    notices: Vec::new(),
                });
                Err(())
            }
        })
    }

    fn connection_error(&mut self) -> BoxFuture<anyhow::Error> {
        Box::pin(futures::future::pending())
    }

    fn allow_subscribe(&self) -> bool {
        false
    }
}

#[async_trait]
impl ResultSender for WebSocket {
    async fn add_result<C, F>(
        &mut self,
        canceled: C,
        res: StatementResult,
    ) -> Result<Result<(), ()>, anyhow::Error>
    where
        C: Fn() -> F + Send + Sync,
        F: Future<Output = ()> + Send,
    {
        async fn send(ws: &mut WebSocket, msg: WebSocketResponse) -> Result<(), anyhow::Error> {
            let msg = serde_json::to_string(&msg).expect("must serialize");
            Ok(ws.send(Message::Text(msg)).await?)
        }

        let (has_rows, is_streaming) = match res {
            StatementResult::SqlResult(SqlResult::Err { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Ok { .. }) => (false, false),
            StatementResult::SqlResult(SqlResult::Rows { .. }) => (true, false),
            StatementResult::Subscribe { .. } => (true, true),
        };
        send(
            self,
            WebSocketResponse::CommandStarting(CommandStarting {
                has_rows,
                is_streaming,
            }),
        )
        .await?;

        let (is_err, msgs) = match res {
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
                (false, msgs)
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
                (false, msgs)
            }
            StatementResult::SqlResult(SqlResult::Err { error, notices }) => {
                let mut msgs = vec![WebSocketResponse::Error(error)];
                msgs.extend(notices.into_iter().map(WebSocketResponse::Notice));
                (true, msgs)
            }
            StatementResult::Subscribe {
                ref desc,
                tag,
                mut rx,
            } => {
                send(self, WebSocketResponse::Rows(desc.into())).await?;

                let mut datum_vec = mz_repr::DatumVec::new();
                loop {
                    match self.await_rows(canceled(), rx.recv()).await? {
                        Some(PeekResponseUnary::Rows(rows)) => {
                            for row in rows {
                                let datums = datum_vec.borrow_with(&row);
                                let types = &desc.typ().column_types;
                                send(
                                    self,
                                    WebSocketResponse::Row(
                                        datums
                                            .iter()
                                            .enumerate()
                                            .map(|(i, d)| TypedDatum::new(*d, &types[i]).json())
                                            .collect(),
                                    ),
                                )
                                .await?;
                            }
                        }
                        Some(PeekResponseUnary::Error(err)) => {
                            break (true, vec![WebSocketResponse::Error(err.into())])
                        }
                        Some(PeekResponseUnary::Canceled) => {
                            break (
                                true,
                                vec![WebSocketResponse::Error("query canceled".into())],
                            )
                        }
                        None => break (false, vec![WebSocketResponse::CommandComplete(tag)]),
                    }
                }
            }
        };
        for msg in msgs {
            send(self, msg).await?;
        }
        Ok(if is_err { Err(()) } else { Ok(()) })
    }

    // Send a websocket Ping every second to verify the client is still
    // connected.
    fn connection_error(&mut self) -> BoxFuture<anyhow::Error> {
        Box::pin(async {
            let mut tick = time::interval(Duration::from_secs(1));
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(err) = self.send(Message::Ping(Vec::new())).await {
                    return anyhow::anyhow!(err);
                }
            }
        })
    }

    fn allow_subscribe(&self) -> bool {
        true
    }
}

/// Returns Ok(Err) if any statement error'd during execution.
async fn execute_stmt_group<S: ResultSender>(
    client: &mut SessionClient,
    sender: &mut S,
    stmt_group: Vec<(Statement<Raw>, Vec<Option<String>>)>,
) -> Result<Result<(), ()>, anyhow::Error> {
    let num_stmts = stmt_group.len();
    for (stmt, params) in stmt_group {
        assert!(num_stmts <= 1 || params.is_empty(),
            "statement groups contain more than 1 statement iff Simple request, which does not support parameters"
        );

        let is_aborted_txn = matches!(client.session().transaction(), TransactionStatus::Failed(_));
        if is_aborted_txn && !is_txn_exit_stmt(&stmt) {
            let err = SqlResult::err(
                client,
                "current transaction is aborted, commands ignored until end of transaction block",
            );
            let _ = sender.add_result(|| client.canceled(), err.into()).await?;
            return Ok(Err(()));
        }

        // Mirror the behavior of the PostgreSQL simple query protocol.
        // See the pgwire::protocol::StateMachine::query method for details.
        if let Err(e) = client.start_transaction(Some(num_stmts)) {
            let err = SqlResult::err(client, e);
            let _ = sender.add_result(|| client.canceled(), err.into()).await?;
            return Ok(Err(()));
        }
        let res = execute_stmt(client, sender, stmt, params).await?;
        let is_err = sender.add_result(|| client.canceled(), res).await?;
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
                        let err = SqlResult::err(client, err.to_string());
                        let _ = sender.add_result(|| client.canceled(), err.into()).await?;
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
) -> Result<(), anyhow::Error> {
    let client = &mut client.client;

    // This API prohibits executing statements with responses whose
    // semantics are at odds with an HTTP response.
    fn check_prohibited_stmts<S: ResultSender>(
        sender: &S,
        stmt: &Statement<Raw>,
    ) -> Result<(), anyhow::Error> {
        let kind: StatementKind = stmt.into();
        let execute_responses = Plan::generated_from(kind)
            .into_iter()
            .map(ExecuteResponse::generated_from)
            .flatten()
            .collect::<Vec<_>>();

        if execute_responses.iter().any(|execute_response| {
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
        }) {
            anyhow::bail!("unsupported via this API: {}", stmt.to_ast_string());
        }
        Ok(())
    }

    fn parse(
        client: &mut SessionClient,
        query: &str,
    ) -> Result<Vec<Statement<Raw>>, anyhow::Error> {
        match client.parse(query) {
            Ok(result) => result.map_err(|e| anyhow!(e.error)),
            Err(e) => Err(anyhow!(e)),
        }
    }

    let mut stmt_groups = vec![];

    match request {
        SqlRequest::Simple { query } => {
            let stmts = parse(client, &query)?;
            let mut stmt_group = Vec::with_capacity(stmts.len());
            for stmt in stmts {
                check_prohibited_stmts(sender, &stmt)?;
                stmt_group.push((stmt, vec![]));
            }
            stmt_groups.push(stmt_group);
        }
        SqlRequest::Extended { queries } => {
            for ExtendedRequest { query, params } in queries {
                let mut stmts = parse(client, &query)?;
                if stmts.len() != 1 {
                    anyhow::bail!(
                        "each query must contain exactly 1 statement, but \"{}\" contains {}",
                        query,
                        stmts.len()
                    );
                }

                let stmt = stmts.pop().unwrap();
                check_prohibited_stmts(sender, &stmt)?;

                stmt_groups.push(vec![(stmt, params)]);
            }
        }
    }

    for stmt_group in stmt_groups {
        let executed = execute_stmt_group(client, sender, stmt_group).await;
        // At the end of each group, commit implicit transactions. Do that here so that any `?`
        // early return can still be handled here.
        if client.session().transaction().is_implicit() {
            let ended = client.end_transaction(EndTransactionAction::Commit).await;
            if let Err(err) = ended {
                let err = SqlResult::err(client, err);
                let _ = sender
                    .add_result(|| client.canceled(), StatementResult::SqlResult(err))
                    .await?;
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
    raw_params: Vec<Option<String>>,
) -> Result<StatementResult, anyhow::Error> {
    const EMPTY_PORTAL: &str = "";
    if let Err(e) = client
        .prepare(EMPTY_PORTAL.into(), Some(stmt.clone()), vec![])
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
        let message = format!(
            "request supplied {actual} parameters, \
                        but {statement} requires {expected}",
            statement = stmt.to_ast_string(),
            actual = raw_params.len(),
            expected = param_types.len()
        );
        return Ok(SqlResult::err(client, message).into());
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
                    raw_param.as_bytes(),
                ) {
                    Ok(param) => param.into_datum(&buf, &pg_typ),
                    Err(err) => {
                        let msg = format!("unable to decode parameter: {}", err);
                        return Ok(SqlResult::err(client, msg).into());
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
    let stmt = prep_stmt.stmt().cloned();
    if let Err(err) = client.session().set_portal(
        EMPTY_PORTAL.into(),
        desc,
        stmt,
        params,
        result_formats,
        revision,
    ) {
        return Ok(SqlResult::err(client, err.to_string()).into());
    }

    let desc = client
        .session()
        // We do not need to verify here because `client.execute` verifies below.
        .get_portal_unverified(EMPTY_PORTAL)
        .map(|portal| portal.desc.clone())
        .expect("unnamed portal should be present");

    let res = match client
        .execute(EMPTY_PORTAL.into(), futures::future::pending())
        .await
    {
        Ok(res) => res,
        Err(e) => {
            return Ok(SqlResult::err(client, e).into());
        }
    };
    let tag = res.tag();

    Ok(match res {
        ExecuteResponse::Canceled => {
            SqlResult::err(client, "statement canceled due to user request").into()
        }
        ExecuteResponse::CreatedConnection { .. }
        | ExecuteResponse::CreatedDatabase { .. }
        | ExecuteResponse::CreatedSchema { .. }
        | ExecuteResponse::CreatedRole
        | ExecuteResponse::CreatedCluster { .. }
        | ExecuteResponse::CreatedClusterReplica { .. }
        | ExecuteResponse::CreatedTable { .. }
        | ExecuteResponse::CreatedIndex { .. }
        | ExecuteResponse::CreatedSecret { .. }
        | ExecuteResponse::CreatedSource { .. }
        | ExecuteResponse::CreatedSources
        | ExecuteResponse::CreatedSink { .. }
        | ExecuteResponse::CreatedView { .. }
        | ExecuteResponse::CreatedViews { .. }
        | ExecuteResponse::CreatedMaterializedView { .. }
        | ExecuteResponse::CreatedType
        | ExecuteResponse::Deleted(_)
        | ExecuteResponse::DiscardedTemp
        | ExecuteResponse::DiscardedAll
        | ExecuteResponse::DroppedObject(_)
        | ExecuteResponse::DroppedOwned
        | ExecuteResponse::EmptyQuery
        | ExecuteResponse::GrantedPrivilege
        | ExecuteResponse::GrantedRole
        | ExecuteResponse::Inserted(_)
        | ExecuteResponse::Raised
        | ExecuteResponse::ReassignOwned
        | ExecuteResponse::RevokedPrivilege
        | ExecuteResponse::AlteredDefaultPrivileges
        | ExecuteResponse::RevokedRole
        | ExecuteResponse::StartedTransaction { .. }
        | ExecuteResponse::Updated(_)
        | ExecuteResponse::AlteredObject(_)
        | ExecuteResponse::AlteredIndexLogicalCompaction
        | ExecuteResponse::AlteredRole
        | ExecuteResponse::AlteredSystemConfiguration
        | ExecuteResponse::Deallocate { .. }
        | ExecuteResponse::ValidatedConnection
        | ExecuteResponse::CreatedWebhookSource
        | ExecuteResponse::Prepare => SqlResult::ok(client, tag.expect("ok only called on tag-generating results"), Vec::default()).into(),
        ExecuteResponse::TransactionCommitted { params } | ExecuteResponse::TransactionRolledBack { params }=> {
            let notify_set: mz_ore::collections::HashSet<String> = client
                .session()
                .vars()
                .notify_set()
                .map(|v| v.name().to_string())
                .collect();
            let params = params
                .into_iter()
                .filter(|(name, _value)| notify_set.contains(*name))
                .map(|(name, value)| ParameterStatus { name: name.to_string(), value })
                .collect();
            SqlResult::ok(client, tag.expect("ok only called on tag-generating results"), params).into()
        },
        ExecuteResponse::SetVariable { name, .. } => {
            let mut params = Vec::with_capacity(1);
            if let Some(var) = client.session().vars().notify_set().find(|v| v.name() == &name) {
                params.push(ParameterStatus { name, value: var.value() });
            };
            SqlResult::ok(client, tag.expect("ok only called on tag-generating results"), params).into()
        }
        ExecuteResponse::SendingRows {
            future: rows,
            span: _,
        } => {
            let rows = match sender.await_rows(client.canceled(), rows).await? {
                PeekResponseUnary::Rows(rows) => rows,
                PeekResponseUnary::Error(e) => {
                    return Ok(SqlResult::err(client, e).into());
                }
                PeekResponseUnary::Canceled => {
                    return Ok(SqlResult::err(client, "statement canceled due to user request").into());
                }
            };
            let mut sql_rows: Vec<Vec<serde_json::Value>> = vec![];
            let mut datum_vec = mz_repr::DatumVec::new();
            let desc = desc.relation_desc.expect("RelationDesc must exist");
            let types = &desc.typ().column_types;
            for row in rows {
                let datums = datum_vec.borrow_with(&row);
                sql_rows.push(datums.iter().enumerate().map(|(i, d)| TypedDatum::new(*d, &types[i]).json()).collect());
            }
            let tag = format!("SELECT {}", sql_rows.len());
            SqlResult::rows(client, tag, sql_rows, desc).into()
        }
        ExecuteResponse::Subscribing { rx }  => {
            StatementResult::Subscribe {
                tag: "SUBSCRIBE".into(),
                desc: desc.relation_desc.unwrap(),
                rx,
            }
        },
        res @ (ExecuteResponse::Fetch { .. }
        | ExecuteResponse::CopyTo { .. }
        | ExecuteResponse::CopyFrom { .. }
        | ExecuteResponse::DeclaredCursor
        | ExecuteResponse::ClosedCursor) => {
            SqlResult::err(
                client,
                format!("internal error: encountered prohibited ExecuteResponse {:?}.\n\n
This is a bug. Can you please file an issue letting us know?\n
https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=C-bug%2CC-triage&template=01-bug.yml",
            ExecuteResponseKind::from(res))).into()
        }
    })
}

fn make_notices(client: &mut SessionClient) -> Vec<Notice> {
    client
        .session()
        .drain_notices()
        .into_iter()
        .map(|notice| Notice {
            message: notice.to_string(),
            severity: Severity::for_adapter_notice(&notice)
                .as_str()
                .to_lowercase(),
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

    use super::WebSocketAuth;

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
                    password: "1234".to_string(),
                    options: BTreeMap::default(),
                },
            },
            TestCase {
                json: r#"{ "user": "mz", "password": "1234", "options": {} }"#,
                expected: WebSocketAuth::Basic {
                    user: "mz".to_string(),
                    password: "1234".to_string(),
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
