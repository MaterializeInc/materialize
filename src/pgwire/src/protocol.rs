// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::iter;
use std::mem;

use byteorder::{ByteOrder, NetworkEndian};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{self, StreamExt};
use itertools::izip;
use lazy_static::lazy_static;
use log::debug;
use openssl::nid::Nid;
use postgres::error::SqlState;
use prometheus::{register_histogram_vec, register_uint_counter};
use tokio::io::{self, AsyncRead, AsyncWrite, Interest};
use tokio::time::{self, Duration, Instant};
use tokio_stream::wrappers::UnboundedReceiverStream;

use coord::session::{
    EndTransactionAction, Portal, PortalState, RowBatchStream, Session, TransactionStatus,
};
use coord::ExecuteResponse;
use dataflow_types::PeekResponse;
use ore::cast::CastFrom;
use ore::netio::AsyncReady;
use ore::str::StrExt;
use repr::{Datum, RelationDesc, RelationType, Row, RowArena};
use sql::ast::display::AstDisplay;
use sql::ast::{FetchDirection, Ident, Raw, Statement};
use sql::plan::{CopyFormat, ExecuteTimeout, StatementDesc};

use crate::codec::FramedConn;
use crate::message::{
    self, BackendMessage, ErrorResponse, FrontendMessage, Severity, VERSIONS, VERSION_3,
};
use crate::server::{Conn, TlsMode};

/// Reports whether the given stream begins with a pgwire handshake.
///
/// To avoid false negatives, there must be at least eight bytes in `buf`.
pub fn match_handshake(buf: &[u8]) -> bool {
    // The pgwire StartupMessage looks like this:
    //
    //     i32 - Length of entire message.
    //     i32 - Protocol version number.
    //     [String] - Arbitrary key-value parameters of any length.
    //
    // Since arbitrary parameters can be included in the StartupMessage, the
    // first Int32 is worthless, since the message could have any length.
    // Instead, we sniff the protocol version number.
    if buf.len() < 8 {
        return false;
    }
    let version = NetworkEndian::read_i32(&buf[4..8]);
    VERSIONS.contains(&version)
}

lazy_static! {
    static ref COMMAND_DURATIONS: prometheus::HistogramVec = register_histogram_vec!(
        "mz_command_durations",
        "how long individual commands took",
        &["command", "status"],
        ore::stats::HISTOGRAM_BUCKETS.to_vec()
    )
    .unwrap();
    static ref ROWS_RETURNED: prometheus::UIntCounter = register_uint_counter!(
        "mz_pg_sent_rows",
        "total number of rows sent to clients from pgwire"
    )
    .unwrap();
}

/// Parameters for the [`run`] function.
pub struct RunParams<'a, A> {
    /// The TLS mode of the pgwire server.
    pub tls_mode: Option<TlsMode>,
    /// A client for the coordinator.
    pub coord_client: coord::ConnClient,
    /// The connection to the client.
    pub conn: &'a mut FramedConn<A>,
    /// The protocol version that the client provided in the startup message.
    pub version: i32,
    /// The parameters that the client provided in the startup message.
    pub params: HashMap<String, String>,
}

/// Runs a pgwire connection to completion.
///
/// This involves responding to `FrontendMessage::StartupMessage` and all future
/// requests until the client terminates the connection or a fatal error occurs.
///
/// Note that this function returns successfully even upon delivering a fatal
/// error to the client. It only returns `Err` if an unexpected I/O error occurs
/// while communicating with the client, e.g., if the connection is severed in
/// the middle of a request.
pub async fn run<'a, A>(
    RunParams {
        tls_mode,
        coord_client,
        conn,
        version,
        mut params,
    }: RunParams<'a, A>,
) -> Result<(), io::Error>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin,
{
    if version != VERSION_3 {
        return conn
            .send(ErrorResponse::fatal(
                SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                "server does not support the client's requested protocol version",
            ))
            .await;
    }

    let user = params.remove("user").unwrap_or_else(String::new);

    // Validate that the connection is compatible with the TLS mode.
    //
    // The match here explicitly spells out all cases to be resilient to
    // future changes to TlsMode.
    match (tls_mode, conn.inner()) {
        (None, Conn::Unencrypted(_)) => (),
        (None, Conn::Ssl(_)) => unreachable!(),
        (Some(TlsMode::Require), Conn::Ssl(_)) => (),
        (Some(TlsMode::Require), Conn::Unencrypted(_))
        | (Some(TlsMode::VerifyUser), Conn::Unencrypted(_)) => {
            return conn
                .send(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "TLS encryption is required",
                ))
                .await;
        }
        (Some(TlsMode::VerifyUser), Conn::Ssl(inner_conn)) => {
            let cn_matches = match inner_conn.ssl().peer_certificate() {
                None => false,
                Some(cert) => cert
                    .subject_name()
                    .entries_by_nid(Nid::COMMONNAME)
                    .any(|n| n.data().as_slice() == user.as_bytes()),
            };
            if !cn_matches {
                let msg = format!(
                    "certificate authentication failed for user {}",
                    user.quoted()
                );
                return conn
                    .send(ErrorResponse::fatal(
                        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
                        msg,
                    ))
                    .await;
            }
        }
    }

    // Construct session.
    let mut session = Session::new(conn.id(), user);
    for (name, value) in params {
        let _ = session.vars_mut().set(&name, &value);
    }

    // Register session with coordinator.
    let (mut coord_client, startup) = match coord_client.startup(session).await {
        Ok(startup) => startup,
        Err(e) => {
            return conn
                .send(ErrorResponse::from_coord(Severity::Fatal, e))
                .await
        }
    };

    // From this point forward we must not fail without calling `coord_client.terminate`!

    let res = async {
        let session = coord_client.session();
        let mut buf = vec![BackendMessage::AuthenticationOk];
        for var in session.vars().notify_set() {
            buf.push(BackendMessage::ParameterStatus(var.name(), var.value()));
        }
        buf.push(BackendMessage::BackendKeyData {
            conn_id: session.conn_id(),
            secret_key: startup.secret_key,
        });
        for startup_message in startup.messages {
            buf.push(ErrorResponse::from_startup_message(startup_message).into());
        }
        buf.push(BackendMessage::ReadyForQuery(session.transaction().into()));
        conn.send_all(buf).await?;
        conn.flush().await?;

        let machine = StateMachine {
            conn,
            coord_client: &mut coord_client,
        };
        machine.run().await
    }
    .await;
    coord_client.terminate().await;
    res
}

#[derive(Debug)]
enum State {
    Ready,
    Drain,
    Done,
}

struct StateMachine<'a, A> {
    conn: &'a mut FramedConn<A>,
    coord_client: &'a mut coord::SessionClient,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + AsyncReady + Send + Sync + Unpin + 'a,
{
    // Manually desugar this (don't use `async fn run`) here because a much better
    // error message is produced if there are problems with Send or other traits
    // somewhere within the Future.
    #[allow(clippy::manual_async_fn)]
    fn run(mut self) -> impl Future<Output = Result<(), io::Error>> + Send + 'a {
        async move {
            let mut state = State::Ready;
            loop {
                state = match state {
                    State::Ready => self.advance_ready().await?,
                    State::Drain => self.advance_drain().await?,
                    State::Done => return Ok(()),
                }
            }
        }
    }

    async fn advance_ready(&mut self) -> Result<State, io::Error> {
        let message = self.conn.recv().await?;
        let timer = Instant::now();
        let name = match &message {
            Some(message) => message.name(),
            None => "eof",
        };

        self.coord_client.reset_canceled();

        let next_state = match message {
            Some(FrontendMessage::Query { sql }) => self.query(sql).await?,
            Some(FrontendMessage::Parse {
                name,
                sql,
                param_types,
            }) => self.parse(name, sql, param_types).await?,
            Some(FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            }) => {
                self.bind(
                    portal_name,
                    statement_name,
                    param_formats,
                    raw_params,
                    result_formats,
                )
                .await?
            }
            Some(FrontendMessage::Execute {
                portal_name,
                max_rows,
            }) => {
                let max_rows = match usize::try_from(max_rows) {
                    Ok(0) | Err(_) => ExecuteCount::All, // If `max_rows < 0`, no limit.
                    Ok(n) => ExecuteCount::Count(n),
                };
                self.execute(
                    portal_name,
                    max_rows,
                    portal_exec_message,
                    None,
                    ExecuteTimeout::None,
                )
                .await?
            }
            Some(FrontendMessage::DescribeStatement { name }) => {
                self.describe_statement(name).await?
            }
            Some(FrontendMessage::DescribePortal { name }) => self.describe_portal(&name).await?,
            Some(FrontendMessage::CloseStatement { name }) => self.close_statement(name).await?,
            Some(FrontendMessage::ClosePortal { name }) => self.close_portal(name).await?,
            Some(FrontendMessage::Flush) => self.flush().await?,
            Some(FrontendMessage::Sync) => self.sync().await?,
            Some(FrontendMessage::Terminate) => State::Done,
            None => State::Done,
        };

        let status = match next_state {
            State::Ready | State::Done => "success",
            State::Drain => "error",
        };
        COMMAND_DURATIONS
            .with_label_values(&[name, status])
            .observe(timer.elapsed().as_secs_f64());

        Ok(next_state)
    }

    async fn advance_drain(&mut self) -> Result<State, io::Error> {
        match self.conn.recv().await? {
            Some(FrontendMessage::Sync) => self.sync().await,
            None => Ok(State::Done),
            _ => Ok(State::Drain),
        }
    }

    async fn one_query(&mut self, stmt: Statement<Raw>) -> Result<State, io::Error> {
        // Bind the portal. Note that this does not set the empty string prepared
        // statement.
        let param_types = vec![];
        const EMPTY_PORTAL: &str = "";
        if let Err(e) = self
            .coord_client
            .declare(EMPTY_PORTAL.to_string(), stmt.clone(), param_types)
            .await
        {
            return self
                .error(ErrorResponse::from_coord(Severity::Error, e))
                .await;
        }

        let stmt_desc = self
            .coord_client
            .session()
            .get_portal(EMPTY_PORTAL)
            .map(|portal| portal.desc.clone())
            .expect("unnamed portal should be present");
        if !stmt_desc.param_types.is_empty() {
            return self
                .error(ErrorResponse::error(
                    SqlState::UNDEFINED_PARAMETER,
                    "there is no parameter $1",
                ))
                .await;
        }

        // Maybe send row description.
        if let Some(relation_desc) = &stmt_desc.relation_desc {
            if !stmt_desc.is_copy {
                let formats = vec![pgrepr::Format::Text; stmt_desc.arity()];
                self.conn
                    .send(BackendMessage::RowDescription(
                        message::encode_row_description(relation_desc, &formats),
                    ))
                    .await?;
            }
        }

        let result = match self.coord_client.execute(EMPTY_PORTAL.to_string()).await {
            Ok(response) => {
                self.send_execute_response(
                    response,
                    stmt_desc.relation_desc,
                    EMPTY_PORTAL.to_string(),
                    ExecuteCount::All,
                    portal_exec_message,
                    None,
                    ExecuteTimeout::None,
                )
                .await
            }
            Err(e) => {
                self.error(ErrorResponse::from_coord(Severity::Error, e))
                    .await
            }
        };

        // Destroy the portal.
        self.coord_client.session().remove_portal(EMPTY_PORTAL);

        result
    }

    // See "Multiple Statements in a Simple Query" which documents how implicit
    // transactions are handled.
    // From https://www.postgresql.org/docs/current/protocol-flow.html
    async fn query(&mut self, sql: String) -> Result<State, io::Error> {
        // Parse first before doing any transaction checking.
        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.error(err).await?;
                return self.ready().await;
            }
        };

        let num_stmts = stmts.len();

        // Compare with postgres' backend/tcop/postgres.c exec_simple_query.
        for stmt in stmts {
            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            if self.is_aborted_txn() && !is_txn_exit_stmt(Some(&stmt)) {
                self.aborted_txn_error().await?;
                break;
            }

            // Start an implicit transaction if we aren't in any transaction and there's
            // more than one statement. This mirrors the `use_implicit_block` variable in
            // postgres.
            //
            // This needs to be done in the loop instead of once at the top because
            // a COMMIT/ROLLBACK statement needs to start a new transaction on next
            // statement.
            self.coord_client
                .session()
                .start_transaction_implicit(num_stmts);

            match self.one_query(stmt).await? {
                State::Ready => (),
                State::Drain => break,
                State::Done => return Ok(State::Done),
            }
        }

        // Implicit transactions are closed at the end of a Query message.
        {
            let implicit = matches!(
                self.coord_client.session().transaction(),
                TransactionStatus::Started(_) | TransactionStatus::InTransactionImplicit(_)
            );
            if implicit {
                self.commit_transaction().await;
            }
        }
        self.ready().await
    }

    async fn parse(
        &mut self,
        name: String,
        sql: String,
        param_oids: Vec<u32>,
    ) -> Result<State, io::Error> {
        let mut param_types = vec![];
        for oid in param_oids {
            match pgrepr::Type::from_oid(oid) {
                Some(ty) => param_types.push(Some(ty)),
                None if oid == 0 => param_types.push(None),
                None => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::PROTOCOL_VIOLATION,
                            format!("unable to decode parameter whose type OID is {}", oid),
                        ))
                        .await;
                }
            }
        }

        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                return self.error(err).await;
            }
        };
        if stmts.len() > 1 {
            return self
                .error(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    "cannot insert multiple commands into a prepared statement",
                ))
                .await;
        }
        let maybe_stmt = stmts.into_iter().next();
        if self.is_aborted_txn() && !is_txn_exit_stmt(maybe_stmt.as_ref()) {
            return self.aborted_txn_error().await;
        }
        match self
            .coord_client
            .describe(name, maybe_stmt, param_types)
            .await
        {
            Ok(()) => {
                self.conn.send(BackendMessage::ParseComplete).await?;
                Ok(State::Ready)
            }
            Err(e) => {
                self.error(ErrorResponse::from_coord(Severity::Error, e))
                    .await
            }
        }
    }

    /// Commits and clears the current transaction.
    async fn commit_transaction(&mut self) {
        // We ignore the ExecuteResponse or error here because there's nothing to tell
        // the user in either of those cases.
        let _ = self
            .coord_client
            .end_transaction(EndTransactionAction::Commit)
            .await;
    }

    async fn bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<pgrepr::Format>,
        raw_params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<State, io::Error> {
        let aborted_txn = self.is_aborted_txn();
        let stmt = self
            .coord_client
            .session()
            .get_prepared_statement(&statement_name);
        let stmt = match stmt {
            Some(stmt) => stmt,
            None => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "prepared statement does not exist",
                    ))
                    .await;
            }
        };

        let param_types = &stmt.desc().param_types;
        if param_types.len() != raw_params.len() {
            let message = format!(
                "bind message supplies {actual} parameters, \
                 but prepared statement \"{name}\" requires {expected}",
                name = statement_name,
                actual = raw_params.len(),
                expected = param_types.len()
            );
            return self
                .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, message))
                .await;
        }
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => {
                return self
                    .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, msg))
                    .await
            }
        };
        if aborted_txn && !is_txn_exit_stmt(stmt.sql()) {
            return self.aborted_txn_error().await;
        }
        let buf = RowArena::new();
        let mut params: Vec<(Datum, repr::ScalarType)> = Vec::new();
        for (raw_param, typ, format) in izip!(raw_params, param_types, param_formats) {
            match raw_param {
                None => params.push(pgrepr::null_datum(typ)),
                Some(bytes) => match pgrepr::Value::decode(format, typ, &bytes) {
                    Ok(param) => params.push(param.into_datum(&buf, typ)),
                    Err(err) => {
                        let msg = format!("unable to decode parameter: {}", err);
                        return self
                            .error(ErrorResponse::error(SqlState::INVALID_PARAMETER_VALUE, msg))
                            .await;
                    }
                },
            }
        }

        let result_formats = match pad_formats(
            result_formats,
            stmt.desc()
                .relation_desc
                .clone()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0),
        ) {
            Ok(result_formats) => result_formats,
            Err(msg) => {
                return self
                    .error(ErrorResponse::error(SqlState::PROTOCOL_VIOLATION, msg))
                    .await
            }
        };

        let desc = stmt.desc().clone();
        let stmt = stmt.sql().cloned();
        if let Err(err) =
            self.coord_client
                .session()
                .set_portal(portal_name, desc, stmt, params, result_formats)
        {
            return self
                .error(ErrorResponse::from_coord(Severity::Error, err))
                .await;
        }

        self.conn.send(BackendMessage::BindComplete).await?;
        Ok(State::Ready)
    }

    fn execute(
        &mut self,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> BoxFuture<'_, Result<State, io::Error>> {
        async move {
            let aborted_txn = self.is_aborted_txn();

            // Check if the portal has been started and can be continued.
            let portal = match self.coord_client.session().get_portal_mut(&portal_name) {
                //  let portal = match session.get_portal_mut(&portal_name) {
                Some(portal) => portal,
                None => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::INVALID_CURSOR_NAME,
                            format!("portal {} does not exist", portal_name.quoted()),
                        ))
                        .await;
                }
            };

            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            let txn_exit_stmt = is_txn_exit_stmt(portal.stmt.as_ref());
            if aborted_txn && !txn_exit_stmt {
                return self.aborted_txn_error().await;
            }

            let row_desc = portal.desc.relation_desc.clone();

            match &mut portal.state {
                PortalState::NotStarted => {
                    // Start a transaction if we aren't in one. Postgres does this both here and
                    // in bind. We don't do it in bind because I'm not sure what purpose it would
                    // serve us (i.e., I'm not aware of a pgtest that would differ between us and
                    // Postgres).
                    self.coord_client.session().start_transaction_implicit(1);

                    match self.coord_client.execute(portal_name.clone()).await {
                        Ok(response) => {
                            self.send_execute_response(
                                response,
                                row_desc,
                                portal_name,
                                max_rows,
                                get_response,
                                fetch_portal_name,
                                timeout,
                            )
                            .await
                        }
                        Err(e) => {
                            self.error(ErrorResponse::from_coord(Severity::Error, e))
                                .await
                        }
                    }
                }
                PortalState::InProgress(rows) => {
                    let rows = rows.take().expect("InProgress rows must be populated");
                    self.send_rows(
                        row_desc.expect("portal missing row desc on resumption"),
                        portal_name,
                        rows,
                        max_rows,
                        get_response,
                        fetch_portal_name,
                        timeout,
                    )
                    .await
                }
                // FETCH is an awkward command for our current architecture. In Postgres it
                // will extract <count> rows from the target portal, cache them, and return
                // them to the user as requested. Its command tag is always FETCH <num rows
                // extracted>. In Materialize, since we have chosen to not fully support FETCH,
                // we must remember the number of rows that were returned. Use this tag to
                // remember that information and return it.
                PortalState::Completed(Some(tag)) => {
                    self.conn
                        .send(BackendMessage::CommandComplete {
                            tag: tag.to_string(),
                        })
                        .await?;
                    Ok(State::Ready)
                }
                PortalState::Completed(None) => {
                    self.error(ErrorResponse::error(
                        SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                        format!(
                            "portal {} cannot be run",
                            Ident::new(portal_name).to_ast_string_stable()
                        ),
                    ))
                    .await
                }
            }
        }
        .boxed()
    }

    async fn describe_statement(&mut self, name: String) -> Result<State, io::Error> {
        let stmt = self.coord_client.session().get_prepared_statement(&name);
        match stmt {
            Some(stmt) => {
                self.conn
                    .send(BackendMessage::ParameterDescription(
                        stmt.desc().param_types.clone(),
                    ))
                    .await?;
                // Claim that all results will be output in text format, even
                // though the true result formats are not yet known. A bit
                // weird, but this is the behavior that PostgreSQL specifies.
                let formats = vec![pgrepr::Format::Text; stmt.desc().arity()];
                self.conn.send(describe_rows(stmt.desc(), &formats)).await?;
                Ok(State::Ready)
            }
            None => {
                self.error(ErrorResponse::error(
                    SqlState::INVALID_SQL_STATEMENT_NAME,
                    "prepared statement does not exist",
                ))
                .await
            }
        }
    }

    async fn describe_portal(&mut self, name: &str) -> Result<State, io::Error> {
        let session = self.coord_client.session();
        let row_desc = session
            .get_portal(name)
            .map(|portal| describe_rows(&portal.desc, &portal.result_formats));
        match row_desc {
            Some(row_desc) => {
                self.conn.send(row_desc).await?;
                Ok(State::Ready)
            }
            None => {
                self.error(ErrorResponse::error(
                    SqlState::INVALID_CURSOR_NAME,
                    format!("portal {} does not exist", name.quoted()),
                ))
                .await
            }
        }
    }

    async fn close_statement(&mut self, name: String) -> Result<State, io::Error> {
        self.coord_client.session().remove_prepared_statement(&name);
        self.conn.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    async fn close_portal(&mut self, name: String) -> Result<State, io::Error> {
        self.coord_client.session().remove_portal(&name);
        self.conn.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    fn complete_portal(&mut self, name: &str) {
        let portal = self
            .coord_client
            .session()
            .get_portal_mut(name)
            .expect("portal should exist");
        portal.state = PortalState::Completed(None);
    }

    async fn fetch(
        &mut self,
        name: String,
        count: Option<FetchDirection>,
        max_rows: ExecuteCount,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        // Unlike Execute, no count specified in FETCH returns 1 row, and 0 means 0
        // instead of All.
        let count = count.unwrap_or(FetchDirection::ForwardCount(1));

        // Figure out how many rows we should send back by looking at the various
        // combinations of the execute and fetch.
        //
        // In Postgres, Fetch will cache <count> rows from the target portal and
        // return those as requested (if, say, an Execute message was sent with a
        // max_rows < the Fetch's count). We expect that case to be incredibly rare and
        // so have chosen to not support it until users request it. This eases
        // implementation difficulty since we don't have to be able to "send" rows to
        // a buffer.
        //
        // TODO(mjibson): Test this somehow? Need to divide up the pgtest files in
        // order to have some that are not Postgres compatible.
        let count = match (max_rows, count) {
            (ExecuteCount::Count(max_rows), FetchDirection::ForwardCount(count)) => {
                let count = usize::cast_from(count);
                if max_rows < count {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::FEATURE_NOT_SUPPORTED,
                            "Execute with max_rows < a FETCH's count is not supported",
                        ))
                        .await;
                }
                ExecuteCount::Count(count)
            }
            (ExecuteCount::Count(_), FetchDirection::ForwardAll) => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        "Execute with max_rows of a FETCH ALL is not supported",
                    ))
                    .await;
            }
            (ExecuteCount::All, FetchDirection::ForwardAll) => ExecuteCount::All,
            (ExecuteCount::All, FetchDirection::ForwardCount(count)) => {
                ExecuteCount::Count(usize::cast_from(count))
            }
        };
        let cursor_name = name.to_string();
        self.execute(
            cursor_name,
            count,
            fetch_message,
            fetch_portal_name,
            timeout,
        )
        .await
    }

    async fn flush(&mut self) -> Result<State, io::Error> {
        self.conn.flush().await?;
        Ok(State::Ready)
    }

    async fn sync(&mut self) -> Result<State, io::Error> {
        // Close the current transaction if we are not in an explicit transaction.
        let started = matches!(
            self.coord_client.session().transaction(),
            TransactionStatus::Started(_)
        );
        if started {
            self.commit_transaction().await;
        }
        return self.ready().await;
    }

    async fn ready(&mut self) -> Result<State, io::Error> {
        let txn_state = self.coord_client.session().transaction().into();
        self.conn
            .send(BackendMessage::ReadyForQuery(txn_state))
            .await?;
        self.flush().await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_execute_response(
        &mut self,
        response: ExecuteResponse,
        row_desc: Option<RelationDesc>,
        portal_name: String,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        macro_rules! command_complete {
            ($($arg:tt)*) => {{
                // N.B.: the output of format! must be stored into a
                // variable, or rustc barfs out a completely inscrutable
                // error: https://github.com/rust-lang/rust/issues/64960.
                let tag = format!($($arg)*);
                self.conn.send(BackendMessage::CommandComplete { tag }).await?;
                Ok(State::Ready)
            }};
        }

        macro_rules! created {
            ($existed:expr, $code:expr, $type:expr) => {{
                if $existed {
                    let msg =
                        ErrorResponse::notice($code, concat!($type, " already exists, skipping"));
                    self.conn.send(msg).await?;
                }
                command_complete!("CREATE {}", $type.to_uppercase())
            }};
        }

        match response {
            ExecuteResponse::ClosedCursor => {
                self.complete_portal(&portal_name);
                command_complete!("CLOSE CURSOR")
            }
            ExecuteResponse::CreatedDatabase { existed } => {
                created!(existed, SqlState::DUPLICATE_DATABASE, "database")
            }
            ExecuteResponse::CreatedSchema { existed } => {
                created!(existed, SqlState::DUPLICATE_SCHEMA, "schema")
            }
            ExecuteResponse::CreatedRole => {
                let existed = false;
                created!(existed, SqlState::DUPLICATE_OBJECT, "role")
            }
            ExecuteResponse::CreatedTable { existed } => {
                created!(existed, SqlState::DUPLICATE_TABLE, "table")
            }
            ExecuteResponse::CreatedIndex { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "index")
            }
            ExecuteResponse::CreatedSource { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "source")
            }
            ExecuteResponse::CreatedSources => command_complete!("CREATE SOURCES"),
            ExecuteResponse::CreatedSink { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "sink")
            }
            ExecuteResponse::CreatedView { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "view")
            }
            ExecuteResponse::CreatedType => command_complete!("CREATE TYPE"),
            ExecuteResponse::DeclaredCursor => {
                self.complete_portal(&portal_name);
                command_complete!("DECLARE CURSOR")
            }
            ExecuteResponse::Deleted(n) => command_complete!("DELETE {}", n),
            ExecuteResponse::DiscardedTemp => command_complete!("DISCARD TEMP"),
            ExecuteResponse::DiscardedAll => command_complete!("DISCARD ALL"),
            ExecuteResponse::DroppedDatabase => command_complete!("DROP DATABASE"),
            ExecuteResponse::DroppedSchema => command_complete!("DROP SCHEMA"),
            ExecuteResponse::DroppedRole => command_complete!("DROP ROLE"),
            ExecuteResponse::DroppedSource => command_complete!("DROP SOURCE"),
            ExecuteResponse::DroppedIndex => command_complete!("DROP INDEX"),
            ExecuteResponse::DroppedSink => command_complete!("DROP SINK"),
            ExecuteResponse::DroppedTable => command_complete!("DROP TABLE"),
            ExecuteResponse::DroppedView => command_complete!("DROP VIEW"),
            ExecuteResponse::DroppedType => command_complete!("DROP TYPE"),
            ExecuteResponse::EmptyQuery => {
                self.conn.send(BackendMessage::EmptyQueryResponse).await?;
                Ok(State::Ready)
            }
            ExecuteResponse::Fetch {
                name,
                count,
                timeout,
            } => {
                self.fetch(
                    name,
                    count,
                    max_rows,
                    Some(portal_name.to_string()),
                    timeout,
                )
                .await
            }
            ExecuteResponse::Inserted(n) => {
                // "On successful completion, an INSERT command returns a
                // command tag of the form `INSERT <oid> <count>`."
                //     -- https://www.postgresql.org/docs/11/sql-insert.html
                //
                // OIDs are a PostgreSQL-specific historical quirk, but we
                // can return a 0 OID to indicate that the table does not
                // have OIDs.
                command_complete!("INSERT 0 {}", n)
            }
            ExecuteResponse::SendingRows(rx) => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::SendingRows");
                match rx.await {
                    PeekResponse::Canceled => {
                        self.error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                        .await
                    }
                    PeekResponse::Error(text) => {
                        self.error(ErrorResponse::error(SqlState::INTERNAL_ERROR, text))
                            .await
                    }
                    PeekResponse::Rows(rows) => {
                        self.send_rows(
                            row_desc,
                            portal_name,
                            Box::new(stream::iter(vec![rows])),
                            max_rows,
                            get_response,
                            fetch_portal_name,
                            timeout,
                        )
                        .await
                    }
                }
            }
            ExecuteResponse::SetVariable { name } => {
                // This code is somewhat awkwardly structured because we
                // can't hold `var` across an await point.
                let qn = name.to_string();
                let msg = if let Some(var) = self
                    .coord_client
                    .session()
                    .vars_mut()
                    .notify_set()
                    .find(|v| v.name() == qn)
                {
                    Some(BackendMessage::ParameterStatus(var.name(), var.value()))
                } else {
                    None
                };
                if let Some(msg) = msg {
                    self.conn.send(msg).await?;
                }
                command_complete!("SET")
            }
            ExecuteResponse::StartedTransaction { duplicated } => {
                if duplicated {
                    let msg = ErrorResponse::warning(
                        SqlState::ACTIVE_SQL_TRANSACTION,
                        "there is already a transaction in progress",
                    );
                    self.conn.send(msg).await?;
                }
                command_complete!("BEGIN")
            }
            ExecuteResponse::TransactionExited { tag, was_implicit } => {
                // In Postgres, if a user sends a COMMIT or ROLLBACK in an implicit
                // transaction, a notice is sent warning them. (The transaction is still closed
                // and a new implicit transaction started, though.)
                if was_implicit {
                    let msg = ErrorResponse::notice(
                        SqlState::NO_ACTIVE_SQL_TRANSACTION,
                        "there is no transaction in progress",
                    );
                    self.conn.send(msg).await?;
                }
                command_complete!("{}", tag)
            }
            ExecuteResponse::Tailing { rx } => {
                if fetch_portal_name.is_none() {
                    let mut msg = ErrorResponse::notice(
                        SqlState::WARNING,
                        "streaming TAIL rows directly requires a client that does not buffer output",
                    );
                    if self.coord_client.session().vars().application_name() == "psql" {
                        msg.hint =
                            Some("Wrap your TAIL statement in `COPY (TAIL ...) TO STDOUT`.".into())
                    }
                    self.conn.send(msg).await?;
                    self.conn.flush().await?;
                }
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::Tailing");
                self.send_rows(
                    row_desc,
                    portal_name,
                    Box::new(UnboundedReceiverStream::new(rx)),
                    max_rows,
                    get_response,
                    fetch_portal_name,
                    timeout,
                )
                .await
            }
            ExecuteResponse::CopyTo { format, resp } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::CopyTo");
                let rows: RowBatchStream = match *resp {
                    ExecuteResponse::Tailing { rx } => Box::new(UnboundedReceiverStream::new(rx)),
                    ExecuteResponse::SendingRows(rx) => match rx.await {
                        // TODO(mjibson): This logic is duplicated from SendingRows. Dedup?
                        PeekResponse::Canceled => {
                            return self
                                .error(ErrorResponse::error(
                                    SqlState::QUERY_CANCELED,
                                    "canceling statement due to user request",
                                ))
                                .await;
                        }
                        PeekResponse::Error(text) => {
                            return self
                                .error(ErrorResponse::error(SqlState::INTERNAL_ERROR, text))
                                .await;
                        }
                        PeekResponse::Rows(rows) => Box::new(stream::iter(vec![rows])),
                    },
                    _ => {
                        return self
                            .error(ErrorResponse::error(
                                SqlState::INTERNAL_ERROR,
                                "unsupported COPY response type".to_string(),
                            ))
                            .await;
                    }
                };
                self.copy_rows(format, row_desc, rows).await
            }
            ExecuteResponse::Updated(n) => command_complete!("UPDATE {}", n),
            ExecuteResponse::AlteredObject(o) => command_complete!("ALTER {}", o),
            ExecuteResponse::AlteredIndexLogicalCompaction => command_complete!("ALTER INDEX"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_rows(
        &mut self,
        row_desc: RelationDesc,
        portal_name: String,
        mut rows: RowBatchStream,
        max_rows: ExecuteCount,
        get_response: GetResponse,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, io::Error> {
        // If this portal is being executed from a FETCH then we need to use the result
        // format type of the outer portal.
        let result_format_portal_name: &str = if let Some(ref name) = fetch_portal_name {
            name
        } else {
            &portal_name
        };
        let result_formats = self
            .coord_client
            .session()
            .get_portal(result_format_portal_name)
            .expect("valid fetch portal name for send rows")
            .result_formats
            .clone();

        let (mut wait_once, mut deadline) = match timeout {
            ExecuteTimeout::None => (false, None),
            ExecuteTimeout::Seconds(t) => {
                (false, Some(Instant::now() + Duration::from_secs_f64(t)))
            }
            ExecuteTimeout::WaitOnce => (true, None),
        };
        // fetch_batch is a helper function that fetches the next row batch and
        // implements timeout deadlines if they were requested.
        async fn fetch_batch(
            deadline: Option<Instant>,
            rows: &mut RowBatchStream,
            canceled: impl Future<Output = ()>,
        ) -> FetchResult {
            tokio::select! {
                _ = time::sleep_until(deadline.unwrap_or_else(time::Instant::now)), if deadline.is_some() => FetchResult::Rows(None),
                _ = canceled => FetchResult::Cancelled,
                batch = rows.next() => FetchResult::Rows(batch),
            }
        }

        let canceled = self.coord_client.canceled();
        let mut batch = fetch_batch(deadline, &mut rows, canceled).await;
        if let Some([row, ..]) = batch.as_rows() {
            let datums = row.unpack();
            let col_types = &row_desc.typ().column_types;
            if datums.len() != col_types.len() {
                return self
                    .error(ErrorResponse::error(
                        SqlState::INTERNAL_ERROR,
                        format!(
                            "internal error: row descriptor has {} columns but row has {} columns",
                            col_types.len(),
                            datums.len(),
                        ),
                    ))
                    .await;
            }
            for (i, (d, t)) in datums.iter().zip(col_types).enumerate() {
                if !d.is_instance_of(&t) {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::INTERNAL_ERROR,
                            format!(
                                "internal error: column {} is not of expected type {:?}: {:?}",
                                i, t, d
                            ),
                        ))
                        .await;
                }
            }
        }

        self.conn.set_encode_state(
            row_desc
                .typ()
                .column_types
                .iter()
                .map(|ty| pgrepr::Type::from(&ty.scalar_type))
                .zip(result_formats)
                .collect(),
        );

        let mut total_sent_rows = 0;
        // want_rows is the maximum number of rows the client wants.
        let mut want_rows = match max_rows {
            ExecuteCount::All => usize::MAX,
            ExecuteCount::Count(count) => count,
        };

        // Send rows while the client still wants them and there are still rows to send.
        loop {
            match batch {
                FetchResult::Rows(None) => break,
                FetchResult::Rows(Some(mut batch_rows)) => {
                    // If wait_once is true: the first time this fn is called it blocks (same as
                    // deadline == None). The second time this fn is called it should behave the
                    // same a 0s timeout.
                    if wait_once && !batch_rows.is_empty() {
                        deadline = Some(Instant::now());
                        wait_once = false;
                    }

                    //  let mut batch_rows = batch_rows;
                    // Drain panics if it's > len, so cap it.
                    let drain_rows = cmp::min(want_rows, batch_rows.len());
                    self.conn
                        .send_all(batch_rows.drain(..drain_rows).map(|row| {
                            BackendMessage::DataRow(pgrepr::values_from_row(row, row_desc.typ()))
                        }))
                        .await?;
                    total_sent_rows += drain_rows;
                    want_rows -= drain_rows;
                    // If we have sent the number of requested rows, put the remainder of the batch
                    // (if any) back and stop sending.
                    if want_rows == 0 {
                        if !batch_rows.is_empty() {
                            rows = Box::new(stream::iter(vec![batch_rows]).chain(rows));
                        }
                        break;
                    }
                    self.conn.flush().await?;
                    let canceled = self.coord_client.canceled();
                    batch = fetch_batch(deadline, &mut rows, canceled).await;
                }
                FetchResult::Cancelled => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                        .await;
                }
            }
        }

        ROWS_RETURNED.inc_by(u64::cast_from(total_sent_rows));

        let portal = self
            .coord_client
            .session()
            .get_portal_mut(&portal_name)
            .expect("valid portal name for send rows");

        // Always return rows back, even if it's empty. This prevents an unclosed
        // portal from re-executing after it has been emptied.
        portal.state = PortalState::InProgress(Some(rows));

        let fetch_portal = fetch_portal_name.map(|name| {
            self.coord_client
                .session()
                .get_portal_mut(&name)
                .expect("valid fetch portal")
        });
        let response_message = get_response(max_rows, total_sent_rows, fetch_portal);
        self.conn.send(response_message).await?;
        Ok(State::Ready)
    }

    async fn copy_rows(
        &mut self,
        format: CopyFormat,
        row_desc: RelationDesc,
        mut stream: RowBatchStream,
    ) -> Result<State, io::Error> {
        let (encode_fn, encode_format): (
            fn(Row, &RelationType, &mut Vec<u8>) -> Result<(), std::io::Error>,
            pgrepr::Format,
        ) = match format {
            CopyFormat::Text => (message::encode_copy_row_text, pgrepr::Format::Text),
            CopyFormat::Binary => (message::encode_copy_row_binary, pgrepr::Format::Binary),
            _ => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        format!("COPY TO format {:?} not supported", format),
                    ))
                    .await
            }
        };

        let typ = row_desc.typ();
        let column_formats = iter::repeat(encode_format)
            .take(typ.column_types.len())
            .collect();
        self.conn
            .send(BackendMessage::CopyOutResponse {
                overall_format: encode_format,
                column_formats,
            })
            .await?;

        // In Postgres, binary copy has a header that is followed (in the same
        // CopyData) by the first row. In order to replicate their behavior, use a
        // common vec that we can extend one time now and then fill up with the encode
        // functions.
        let mut out = Vec::new();

        if let CopyFormat::Binary = format {
            // 11-byte signature.
            out.extend(b"PGCOPY\n\xFF\r\n\0");
            // 32-bit flags field.
            out.extend(&[0, 0, 0, 0]);
            // 32-bit header extension length field.
            out.extend(&[0, 0, 0, 0]);
        }

        let mut count = 0;
        loop {
            tokio::select! {
                _ = time::sleep_until(Instant::now() + Duration::from_secs(1)) => {
                    // It's been a while since we've had any data to send, and
                    // the client may have disconnected. Check whether the
                    // socket is no longer readable and error if so. Otherwise
                    // we might block forever waiting for rows, leaking memory
                    // and a socket.
                    //
                    // In theory we should check for writability rather than
                    // readability—after all, we're writing data to the socket,
                    // not reading from it—but read-closed events are much more
                    // reliable on TCP streams than write-closed events.
                    // See: https://github.com/tokio-rs/mio/pull/1110
                    let ready = self.conn.ready(Interest::READABLE).await?;
                    if ready.is_read_closed() {
                        return self
                            .error(ErrorResponse::fatal(
                                SqlState::CONNECTION_FAILURE,
                                "connection closed",
                            ))
                            .await;
                    }
                },
                _ = self.coord_client.canceled() => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        ))
                    .await;
                },
                batch = stream.next() => match batch {
                    None => break,
                    Some(rows) => {
                        count += rows.len();
                        for row in rows {
                            encode_fn(row, typ, &mut out)?;
                            self.conn
                                .send(BackendMessage::CopyData(mem::take(&mut out)))
                                .await?;
                        }
                    }
                },
            }

            self.conn.flush().await?;
        }
        // Send required trailers.
        if let CopyFormat::Binary = format {
            let trailer: i16 = -1;
            out.extend(&trailer.to_be_bytes());
            self.conn
                .send(BackendMessage::CopyData(mem::take(&mut out)))
                .await?;
        }

        let tag = format!("COPY {}", count);
        self.conn.send(BackendMessage::CopyDone).await?;
        self.conn
            .send(BackendMessage::CommandComplete { tag })
            .await?;
        Ok(State::Ready)
    }

    async fn error(&mut self, err: ErrorResponse) -> Result<State, io::Error> {
        assert!(err.severity.is_error());
        debug!(
            "cid={} error code={} message={}",
            self.coord_client.session().conn_id(),
            err.code.code(),
            err.message
        );
        let is_fatal = err.severity.is_fatal();
        self.conn.send(BackendMessage::ErrorResponse(err)).await?;
        let session = self.coord_client.session();
        match session.transaction() {
            // Error can be called from describe and parse and so might not be in an active
            // transaction.
            TransactionStatus::Default | TransactionStatus::Failed => {}
            // In Started (i.e., a single statement), cleanup ourselves.
            TransactionStatus::Started(_) => {
                session.clear_transaction();
            }
            // Implicit transactions also clear themselves.
            TransactionStatus::InTransactionImplicit(_) => {
                session.clear_transaction();
            }
            // Explicit transactions move to failed.
            TransactionStatus::InTransaction(_) => {
                session.fail_transaction();
            }
        };
        if is_fatal {
            Ok(State::Done)
        } else {
            Ok(State::Drain)
        }
    }

    async fn aborted_txn_error(&mut self) -> Result<State, io::Error> {
        self.conn
            .send(BackendMessage::ErrorResponse(ErrorResponse::error(
                SqlState::IN_FAILED_SQL_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            )))
            .await?;
        Ok(State::Drain)
    }

    fn is_aborted_txn(&mut self) -> bool {
        matches!(
            self.coord_client.session().transaction(),
            TransactionStatus::Failed
        )
    }
}

fn pad_formats(formats: Vec<pgrepr::Format>, n: usize) -> Result<Vec<pgrepr::Format>, String> {
    match (formats.len(), n) {
        (0, e) => Ok(vec![pgrepr::Format::Text; e]),
        (1, e) => Ok(iter::repeat(formats[0]).take(e).collect()),
        (a, e) if a == e => Ok(formats),
        (a, e) => Err(format!(
            "expected {} field format specifiers, but got {}",
            e, a
        )),
    }
}

fn describe_rows(stmt_desc: &StatementDesc, formats: &[pgrepr::Format]) -> BackendMessage {
    match &stmt_desc.relation_desc {
        Some(desc) if !stmt_desc.is_copy => {
            BackendMessage::RowDescription(message::encode_row_description(desc, formats))
        }
        _ => BackendMessage::NoData,
    }
}

fn parse_sql(sql: &str) -> Result<Vec<Statement<Raw>>, ErrorResponse> {
    sql::parse::parse(sql).map_err(|e| {
        // Convert our 0-based byte position to pgwire's 1-based character
        // position.
        let pos = sql[..e.pos].chars().count() + 1;
        ErrorResponse::error(SqlState::SYNTAX_ERROR, e.message).with_position(pos)
    })
}

type GetResponse = fn(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    fetch_portal: Option<&mut Portal>,
) -> BackendMessage;

// A GetResponse used by send_rows during execute messages on portals or for
// simple query messages.
fn portal_exec_message(
    max_rows: ExecuteCount,
    total_sent_rows: usize,
    _fetch_portal: Option<&mut Portal>,
) -> BackendMessage {
    // If max_rows is not specified, we will always send back a CommandComplete. If
    // max_rows is specified, we only send CommandComplete if there were more rows
    // requested than were remaining. That is, if max_rows == number of rows that
    // were remaining before sending (not that are remaining after sending), then
    // we still send a PortalSuspended. The number of remaining rows after the rows
    // have been sent doesn't matter. This matches postgres.
    match max_rows {
        ExecuteCount::Count(max_rows) if max_rows <= total_sent_rows => {
            BackendMessage::PortalSuspended
        }
        _ => BackendMessage::CommandComplete {
            tag: format!("SELECT {}", total_sent_rows),
        },
    }
}

// A GetResponse used by send_rows during FETCH queries.
fn fetch_message(
    _max_rows: ExecuteCount,
    total_sent_rows: usize,
    fetch_portal: Option<&mut Portal>,
) -> BackendMessage {
    let tag = format!("FETCH {}", total_sent_rows);
    if let Some(portal) = fetch_portal {
        portal.state = PortalState::Completed(Some(tag.clone()));
    }
    BackendMessage::CommandComplete { tag }
}

#[derive(Debug, Copy, Clone)]
enum ExecuteCount {
    All,
    Count(usize),
}

// See postgres' backend/tcop/postgres.c IsTransactionExitStmt.
fn is_txn_exit_stmt(stmt: Option<&Statement<Raw>>) -> bool {
    match stmt {
        // Add PREPARE to this if we ever support it.
        Some(stmt) => matches!(stmt, Statement::Commit(_) | Statement::Rollback(_)),
        None => false,
    }
}

#[derive(Debug)]
enum FetchResult {
    Rows(Option<Vec<Row>>),
    Cancelled,
}

impl FetchResult {
    fn as_rows(&self) -> Option<&[Row]> {
        match self {
            FetchResult::Rows(rows) => rows.as_deref(),
            _ => None,
        }
    }
}
