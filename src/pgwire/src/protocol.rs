// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::convert::TryFrom;
use std::future::Future;
use std::iter;
use std::mem;

use byteorder::{ByteOrder, NetworkEndian};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::izip;
use lazy_static::lazy_static;
use log::debug;
use postgres::error::SqlState;
use prometheus::{register_histogram_vec, register_uint_counter};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Duration, Instant};

use coord::session::{Portal, PortalState, RowBatchStream, TransactionStatus};
use coord::{ExecuteResponse, StartupMessage};
use dataflow_types::PeekResponse;
use ore::cast::CastFrom;
use repr::{Datum, RelationDesc, RelationType, Row, RowArena};
use sql::ast::display::AstDisplay;
use sql::ast::{FetchDirection, Ident, Statement};
use sql::plan::FetchOptions;
use sql::plan::{CopyFormat, StatementDesc};

use crate::codec::FramedConn;
use crate::message::{self, BackendMessage, ErrorResponse, FrontendMessage, VERSIONS, VERSION_3};

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

#[derive(Debug)]
enum State {
    Ready,
    Drain,
    Done,
}

pub struct StateMachine<A> {
    pub conn: FramedConn<A>,
    pub conn_id: u32,
    pub secret_key: u32,
    pub coord_client: coord::SessionClient,
}

impl<A> StateMachine<A>
where
    A: AsyncRead + AsyncWrite + Send + Unpin,
{
    // Manually desugar this (don't use `async fn run`) here because a much better
    // error message is produced if there are problems with Send or other traits
    // somewhere within the Future.
    #[allow(clippy::manual_async_fn)]
    pub fn run(
        mut self,
        version: i32,
        params: Vec<(String, String)>,
    ) -> impl Future<Output = Result<(), comm::Error>> + Send {
        async move {
            let mut state = self.startup(version, params).await?;

            loop {
                state = match state {
                    State::Ready => self.advance_ready().await?,
                    State::Drain => self.advance_drain().await?,
                    State::Done => break,
                }
            }

            self.coord_client.terminate().await;
            Ok(())
        }
    }

    async fn advance_ready(&mut self) -> Result<State, comm::Error> {
        let message = self.conn.recv().await?;
        let timer = Instant::now();
        let name = match &message {
            Some(message) => message.name(),
            None => "eof",
        };

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

    async fn advance_drain(&mut self) -> Result<State, comm::Error> {
        match self.conn.recv().await? {
            Some(FrontendMessage::Sync) => self.sync().await,
            None => Ok(State::Done),
            _ => Ok(State::Drain),
        }
    }

    async fn startup(
        &mut self,
        version: i32,
        params: Vec<(String, String)>,
    ) -> Result<State, comm::Error> {
        if version != VERSION_3 {
            return self
                .error(ErrorResponse::fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "server does not support the client's requested protocol version",
                ))
                .await;
        }

        for (name, value) in params {
            let _ = self.coord_client.session().vars_mut().set(&name, &value);
        }

        let notices: Vec<_> = match self.coord_client.startup().await {
            Ok(messages) => messages
                .into_iter()
                .map(|m| match m {
                    StartupMessage::UnknownSessionDatabase => ErrorResponse::notice(
                        SqlState::SUCCESSFUL_COMPLETION,
                        format!(
                            "session database '{}' does not exist",
                            self.coord_client.session().vars().database(),
                        ),
                    )
                    .with_hint(
                        "Create the database with CREATE DATABASE \
                         or pick an extant database with SET DATABASE = <name>. \
                         List available databases with SHOW DATABASES.",
                    )
                    .into_message(),
                })
                .collect(),
            Err(e) => {
                return self
                    .error(ErrorResponse::error(
                        SqlState::INTERNAL_ERROR,
                        format!("{:#}", e),
                    ))
                    .await;
            }
        };

        let mut messages = vec![BackendMessage::AuthenticationOk];
        messages.extend(
            self.coord_client
                .session()
                .vars()
                .notify_set()
                .map(|v| BackendMessage::ParameterStatus(v.name(), v.value())),
        );
        messages.push(BackendMessage::BackendKeyData {
            conn_id: self.conn_id,
            secret_key: self.secret_key,
        });
        messages.extend(notices);
        messages.push(BackendMessage::ReadyForQuery(
            self.coord_client.session().transaction().into(),
        ));
        self.conn.send_all(messages).await?;
        self.flush().await
    }

    async fn one_query(&mut self, stmt: Statement) -> Result<State, comm::Error> {
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
                .error(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    format!("{:#}", e),
                ))
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

        let result = match self.handle_cursors(EMPTY_PORTAL, ExecuteCount::All).await {
            Some(result) => result,
            None => {
                // Not a cursor, Execute.
                match self.coord_client.execute(EMPTY_PORTAL.to_string()).await {
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
                        self.error(ErrorResponse::error(
                            SqlState::INTERNAL_ERROR,
                            format!("{:#}", e),
                        ))
                        .await
                    }
                }
            }
        };

        // Destroy the portal.
        self.coord_client.session().remove_portal(EMPTY_PORTAL);

        result
    }

    async fn handle_cursors(
        &mut self,
        portal_name: &str,
        max_rows: ExecuteCount,
    ) -> Option<Result<State, comm::Error>> {
        let portal = self
            .coord_client
            .session()
            .get_portal_mut(portal_name)
            .expect("portal should exist");

        // Some statements are equivalent to session control messages and should be
        // intercepted instead of being passed on to the coordinator.
        match &portal.stmt {
            Some(Statement::Declare(stmt)) => {
                portal.state = PortalState::Completed(None);
                let name = stmt.name.to_string();
                let stmt = *stmt.stmt.clone();
                Some(self.declare(name, stmt).await)
            }
            Some(Statement::Fetch(stmt)) => {
                let name = stmt.name.clone();
                let count = stmt.count.clone();
                let options = match FetchOptions::try_from(stmt.options.clone()) {
                    Ok(options) => options,
                    Err(e) => {
                        return Some(
                            self.error(ErrorResponse::error(
                                SqlState::INVALID_PARAMETER_VALUE,
                                format!("{}", e),
                            ))
                            .await,
                        )
                    }
                };
                let timeout = match options.timeout {
                    Some(timeout) => {
                        // Limit FETCH timeouts to 1 day. If users have a legitimate need it can be
                        // bumped. If we do bump it, ensure that the new upper limit is within the
                        // bounds of a tokio time future, otherwise it'll panic.
                        const SECS_PER_DAY: f64 = 60f64 * 60f64 * 24f64;
                        let timeout_secs = timeout.as_seconds();
                        if !timeout_secs.is_finite()
                            || timeout_secs < 0f64
                            || timeout_secs > SECS_PER_DAY
                        {
                            return Some(
                                self.error(ErrorResponse::error(
                                    SqlState::INVALID_PARAMETER_VALUE,
                                    format!("timeout out of range: {:#}", timeout),
                                ))
                                .await,
                            );
                        }
                        ExecuteTimeout::Seconds(timeout_secs)
                    }
                    // FETCH defaults to WaitOnce.
                    None => ExecuteTimeout::WaitOnce,
                };
                Some(
                    self.fetch(
                        name,
                        count,
                        max_rows,
                        Some(portal_name.to_string()),
                        timeout,
                    )
                    .await,
                )
            }
            Some(Statement::Close(stmt)) => {
                portal.state = PortalState::Completed(None);
                let name = stmt.name.clone();
                Some(self.close_cursor(name).await)
            }
            _ => None,
        }
    }

    // See "Multiple Statements in a Simple Query" which documents how implicit
    // transactions are handled.
    // From https://www.postgresql.org/docs/current/protocol-flow.html
    async fn query(&mut self, sql: String) -> Result<State, comm::Error> {
        // Parse first before doing any transaction checking.
        let stmts = match parse_sql(&sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.error(err).await?;
                return self.sync().await;
            }
        };

        // Compare with postgres' backend/tcop/postgres.c exec_simple_query.
        for stmt in stmts {
            // In an aborted transaction, reject all commands except COMMIT/ROLLBACK.
            let aborted_txn = matches!(
                self.coord_client.session().transaction(),
                TransactionStatus::Failed
            );
            let txn_exit_stmt = matches!(&stmt, Statement::Commit(_) | Statement::Rollback(_));
            if aborted_txn && !txn_exit_stmt {
                self.conn.send(BackendMessage::ErrorResponse(ErrorResponse::error(
                        SqlState::IN_FAILED_SQL_TRANSACTION,
                        "current transaction is aborted, commands ignored until end of transaction block",
                    ))).await?;
                break;
            }

            // Start an implicit transaction if we aren't in any transaction.
            {
                let session = self.coord_client.session();
                if let TransactionStatus::Idle = session.transaction() {
                    session.start_transaction_implicit();
                }
            }

            match self.one_query(stmt).await? {
                State::Ready => (),
                State::Drain => break,
                State::Done => return Ok(State::Done),
            }
        }

        // Implicit transactions are closed at the end of a Query message.
        {
            let session = self.coord_client.session();
            if let TransactionStatus::InTransactionImplicit = session.transaction() {
                session.end_transaction();
            }
        }
        self.sync().await
    }

    async fn parse(
        &mut self,
        name: String,
        sql: String,
        param_oids: Vec<u32>,
    ) -> Result<State, comm::Error> {
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
                self.error(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    format!("{:#}", e),
                ))
                .await
            }
        }
    }

    async fn bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<pgrepr::Format>,
        raw_params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<State, comm::Error> {
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
        self.coord_client
            .session()
            .set_portal(portal_name, desc, stmt, params, result_formats);

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
    ) -> BoxFuture<'_, Result<State, comm::Error>> {
        async move {
            // Check if the portal has been started and can be continued.
            let portal = match self.coord_client.session().get_portal_mut(&portal_name) {
                //  let portal = match session.get_portal_mut(&portal_name) {
                Some(portal) => portal,
                None => {
                    return self
                        .error(ErrorResponse::error(
                            SqlState::INVALID_CURSOR_NAME,
                            format!("portal \"{}\" does not exist", portal_name),
                        ))
                        .await;
                }
            };
            let row_desc = portal.desc.relation_desc.clone();

            match &mut portal.state {
                PortalState::NotStarted => {
                    if portal.stmt.is_some() {
                        if let Some(result) = self.handle_cursors(&portal_name, max_rows).await {
                            return result;
                        }
                    }

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
                            self.error(ErrorResponse::error(
                                SqlState::INTERNAL_ERROR,
                                format!("{:#}", e),
                            ))
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

    async fn describe_statement(&mut self, name: String) -> Result<State, comm::Error> {
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

    async fn describe_portal(&mut self, name: &str) -> Result<State, comm::Error> {
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
                    format!("portal \"{}\" does not exist", name),
                ))
                .await
            }
        }
    }

    async fn close_statement(&mut self, name: String) -> Result<State, comm::Error> {
        self.coord_client.session().remove_prepared_statement(&name);
        self.conn.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    async fn close_portal(&mut self, name: String) -> Result<State, comm::Error> {
        self.coord_client.session().remove_portal(&name);
        self.conn.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready)
    }

    async fn fetch(
        &mut self,
        name: Ident,
        count: Option<FetchDirection>,
        max_rows: ExecuteCount,
        fetch_portal_name: Option<String>,
        timeout: ExecuteTimeout,
    ) -> Result<State, comm::Error> {
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

    async fn declare(&mut self, name: String, stmt: Statement) -> Result<State, comm::Error> {
        let param_types = vec![];
        if let Err(e) = self.coord_client.declare(name, stmt, param_types).await {
            return self
                .error(ErrorResponse::error(
                    SqlState::INTERNAL_ERROR,
                    format!("{:#}", e),
                ))
                .await;
        }
        self.conn
            .send(BackendMessage::CommandComplete {
                tag: "DECLARE CURSOR".to_string(),
            })
            .await?;
        Ok(State::Ready)
    }

    async fn close_cursor(&mut self, name: Ident) -> Result<State, comm::Error> {
        let cursor_name = name.to_string();
        if !self.coord_client.session().remove_portal(&cursor_name) {
            return self
                .error(ErrorResponse::error(
                    SqlState::INVALID_CURSOR_NAME,
                    format!("cursor {} does not exist", name.to_ast_string_stable()),
                ))
                .await;
        }
        self.conn
            .send(BackendMessage::CommandComplete {
                tag: "CLOSE CURSOR".to_string(),
            })
            .await?;
        Ok(State::Ready)
    }

    async fn flush(&mut self) -> Result<State, comm::Error> {
        self.conn.flush().await?;
        Ok(State::Ready)
    }

    async fn sync(&mut self) -> Result<State, comm::Error> {
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
    ) -> Result<State, comm::Error> {
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
                        ErrorResponse::notice($code, concat!($type, " already exists, skipping"))
                            .into_message();
                    self.conn.send(msg).await?;
                }
                command_complete!("CREATE {}", $type.to_uppercase())
            }};
        }

        match response {
            ExecuteResponse::CreatedDatabase { existed } => {
                created!(existed, SqlState::DUPLICATE_DATABASE, "database")
            }
            ExecuteResponse::CreatedSchema { existed } => {
                created!(existed, SqlState::DUPLICATE_SCHEMA, "schema")
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
            ExecuteResponse::CreatedSink { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "sink")
            }
            ExecuteResponse::CreatedView { existed } => {
                created!(existed, SqlState::DUPLICATE_OBJECT, "view")
            }
            ExecuteResponse::CreatedType => command_complete!("CREATE TYPE"),
            ExecuteResponse::Deleted(n) => command_complete!("DELETE {}", n),
            ExecuteResponse::DiscardedTemp => command_complete!("DISCARD TEMP"),
            ExecuteResponse::DiscardedAll => command_complete!("DISCARD ALL"),
            ExecuteResponse::DroppedDatabase => command_complete!("DROP DATABASE"),
            ExecuteResponse::DroppedSchema => command_complete!("DROP SCHEMA"),
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
                match rx.await? {
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
                            Box::new(stream::iter(vec![Ok(rows)])),
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
            ExecuteResponse::StartedTransaction => command_complete!("BEGIN"),
            ExecuteResponse::TransactionExited { tag, was_implicit } => {
                // In Postgres, if a user sends a COMMIT or ROLLBACK in an implicit
                // transaction, a notice is sent warning them. (The transaction is still closed
                // and a new implicit transaction started, though.)
                if was_implicit {
                    let msg = ErrorResponse::notice(
                        SqlState::NO_ACTIVE_SQL_TRANSACTION,
                        "there is no transaction in progress",
                    )
                    .into_message();
                    self.conn.send(msg).await?;
                }
                command_complete!("{}", tag)
            }
            ExecuteResponse::Tailing { rx } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::Tailing");
                self.send_rows(
                    row_desc,
                    portal_name,
                    Box::new(rx),
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
                    ExecuteResponse::Tailing { rx } => Box::new(rx),
                    ExecuteResponse::SendingRows(rx) => match rx.await? {
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
                        PeekResponse::Rows(rows) => Box::new(stream::iter(vec![Ok(rows)])),
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
    ) -> Result<State, comm::Error> {
        let session = self.coord_client.session();

        // If this portal is being executed from a FETCH then we need to use the result
        // format type of the outer portal.
        let result_format_portal_name: &str = if let Some(ref name) = fetch_portal_name {
            name
        } else {
            &portal_name
        };
        let result_formats = session
            .get_portal(result_format_portal_name)
            .expect("valid fetch portal name for send rows")
            .result_formats
            .clone();

        let portal = session
            .get_portal_mut(&portal_name)
            .expect("valid portal name for send rows");

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
            wait_once: &mut bool,
            deadline: &mut Option<Instant>,
            rows: &mut RowBatchStream,
        ) -> Result<Option<Vec<Row>>, comm::Error> {
            let res = match deadline {
                None => rows.try_next().await,
                Some(deadline) => match time::timeout_at(*deadline, rows.try_next()).await {
                    Ok(batch) => batch,
                    Err(_elapsed) => Ok(None),
                },
            };
            // If wait_once is true: the first time this fn is called it blocks (same as
            // deadline == None). The second time this fn is called it should behave the
            // same a 0s timeout.
            if *wait_once {
                *deadline = Some(Instant::now());
                *wait_once = false;
            }
            res
        };

        let mut batch: Option<Vec<Row>> =
            fetch_batch(&mut wait_once, &mut deadline, &mut rows).await?;
        if let Some([row, ..]) = batch.as_deref() {
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
                                "internal error: column {} is not of expected type {}: {}",
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
        while let Some(batch_rows) = batch {
            let mut batch_rows = batch_rows;
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
            // back and stop sending.
            if want_rows == 0 {
                rows = Box::new(stream::iter(vec![Ok(batch_rows)]).chain(rows));
                break;
            }
            self.conn.flush().await?;
            batch = fetch_batch(&mut wait_once, &mut deadline, &mut rows).await?;
        }

        ROWS_RETURNED.inc_by(u64::cast_from(total_sent_rows));

        // Always return rows back, even if it's empty. This prevents an unclosed
        // portal from re-executing after it has been emptied.
        portal.state = PortalState::InProgress(Some(Box::new(rows)));

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
    ) -> Result<State, comm::Error> {
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
            match time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(None) => break,
                Ok(Some(rows)) => {
                    let rows = rows?;
                    count += rows.len();
                    for row in rows {
                        encode_fn(row, typ, &mut out)?;
                        self.conn
                            .send(BackendMessage::CopyData(mem::take(&mut out)))
                            .await?;
                    }
                }
                Err(time::error::Elapsed { .. }) => {
                    // It's been a while since we've had any data to send, and the client may have
                    // disconnected. Send a notice, which will error if the client has, in fact,
                    // disconnected. Otherwise we might block forever waiting for rows, leaking
                    // memory and a socket.
                    //
                    // TODO: When we are on tokio 0.3, use
                    // https://github.com/tokio-rs/tokio/issues/2228 to detect this.
                    self.conn
                        .send(BackendMessage::ErrorResponse(ErrorResponse::notice(
                            SqlState::NO_DATA,
                            "TAIL waiting for more data",
                        )))
                        .await?;
                }
            }
            self.conn.flush().await?;
        }
        // Send required trailers.
        if let CopyFormat::Binary = format {
            let trailer: i16 = -1;
            self.conn
                .send(BackendMessage::CopyData(trailer.to_be_bytes().to_vec()))
                .await?;
        }

        let tag = format!("COPY {}", count);
        self.conn.send(BackendMessage::CopyDone).await?;
        self.conn
            .send(BackendMessage::CommandComplete { tag })
            .await?;
        Ok(State::Ready)
    }

    async fn error(&mut self, err: ErrorResponse) -> Result<State, comm::Error> {
        assert!(err.severity.is_error());
        debug!(
            "cid={} error code={} message={}",
            self.conn_id,
            err.code.code(),
            err.message
        );
        let is_fatal = err.severity.is_fatal();
        self.conn.send(BackendMessage::ErrorResponse(err)).await?;
        let session = self.coord_client.session();
        // Errors in implicit transactions move it back to idle, not failed.
        match session.transaction() {
            TransactionStatus::InTransactionImplicit => session.end_transaction(),
            _ => session.fail_transaction(),
        };
        if is_fatal {
            Ok(State::Done)
        } else {
            Ok(State::Drain)
        }
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

fn parse_sql(sql: &str) -> Result<Vec<Statement>, ErrorResponse> {
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

#[derive(Debug, Copy, Clone)]
enum ExecuteTimeout {
    None,
    Seconds(f64),
    WaitOnce,
}
