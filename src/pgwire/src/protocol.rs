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
use std::time::Instant;

use byteorder::{ByteOrder, NetworkEndian};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::izip;
use lazy_static::lazy_static;
use log::debug;
use postgres::error::SqlState;
use prometheus::{register_histogram_vec, register_uint_counter};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Duration};

use coord::{session::RowBatchStream, ExecuteResponse, StartupMessage};
use dataflow_types::PeekResponse;
use ore::cast::CastFrom;
use repr::{Datum, RelationDesc, RowArena};
use sql::ast::Statement;
use sql::plan::CopyFormat;

use crate::codec::FramedConn;
use crate::message::{
    self, BackendMessage, ErrorSeverity, FrontendMessage, NoticeSeverity, VERSIONS, VERSION_3,
};

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
                    Ok(0) | Err(_) => usize::MAX, // If `max_rows < 0`, no limit.
                    Ok(n) => n,
                };
                self.execute(portal_name, max_rows).await?
            }
            Some(FrontendMessage::DescribeStatement { name }) => {
                self.describe_statement(name).await?
            }
            Some(FrontendMessage::DescribePortal { name }) => self.describe_portal(name).await?,
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
                .fatal(
                    SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
                    "server does not support the client's requested protocol version",
                )
                .await;
        }

        for (name, value) in params {
            let _ = self.coord_client.session().vars_mut().set(&name, &value);
        }

        let notices: Vec<_> = match self.coord_client.startup().await {
            Ok(messages) => messages
                .into_iter()
                .map(|m| match m {
                    StartupMessage::UnknownSessionDatabase => BackendMessage::NoticeResponse {
                        severity: NoticeSeverity::Notice,
                        code: SqlState::SUCCESSFUL_COMPLETION,
                        message: format!(
                            "session database '{}' does not exist",
                            self.coord_client.session().vars().database()
                        ),
                        detail: None,
                        hint: Some(
                            "Create the database with CREATE DATABASE \
                                 or pick an extant database with SET DATABASE = <name>. \
                                 List available databases with SHOW DATABASES."
                                .into(),
                        ),
                    },
                })
                .collect(),
            Err(e) => {
                return self
                    .error(SqlState::INTERNAL_ERROR, format!("{:#}", e))
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
        let stmt_name = String::from("");
        let param_types = vec![];
        if let Err(e) = self
            .coord_client
            .describe(stmt_name.clone(), Some(stmt), param_types)
            .await
        {
            return self
                .error(SqlState::INTERNAL_ERROR, format!("{:#}", e))
                .await;
        }

        let stmt = self
            .coord_client
            .session()
            .get_prepared_statement(&stmt_name)
            .unwrap();
        if !stmt.param_types().is_empty() {
            return self
                .error(SqlState::UNDEFINED_PARAMETER, "there is no parameter $1")
                .await;
        }

        let row_desc = stmt.desc().relation_desc();

        // Bind.
        let portal_name = String::from("");
        let params = vec![];
        let result_formats = vec![pgrepr::Format::Text; stmt.result_width()];
        self.coord_client
            .session()
            .set_portal(
                portal_name.clone(),
                stmt_name.clone(),
                params,
                result_formats,
            )
            .expect("unnamed statement to be present during simple query flow");

        // Maybe send row description.
        if let Some(ref desc) = row_desc {
            self.conn
                .send(BackendMessage::RowDescription(
                    message::row_description_from_desc(desc),
                ))
                .await?;
        }

        // Execute.
        match self.coord_client.execute(portal_name.clone()).await {
            Ok(response) => {
                let max_rows = usize::MAX;
                self.send_execute_response(response, row_desc, portal_name, max_rows)
                    .await
            }
            Err(e) => {
                self.error(SqlState::INTERNAL_ERROR, format!("{:#}", e))
                    .await
            }
        }
    }

    async fn query(&mut self, sql: String) -> Result<State, comm::Error> {
        let stmts = match sql::parse::parse(sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                self.error(SqlState::SYNTAX_ERROR, format!("{:#}", err))
                    .await?;
                return self.sync().await;
            }
        };
        for stmt in stmts {
            match self.one_query(stmt).await? {
                State::Ready => (),
                State::Drain => break,
                State::Done => return Ok(State::Done),
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
                        .error(
                            SqlState::PROTOCOL_VIOLATION,
                            format!("unable to decode parameter whose type OID is {}", oid),
                        )
                        .await
                }
            }
        }

        let stmts = match sql::parse::parse(sql.clone()) {
            Ok(stmts) => stmts,
            Err(err) => {
                return self
                    .error(SqlState::SYNTAX_ERROR, format!("{:#}", err))
                    .await;
            }
        };
        if stmts.len() > 1 {
            return self
                .error(
                    SqlState::INTERNAL_ERROR,
                    "cannot insert multiple commands into a prepared statement",
                )
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
                self.error(SqlState::INTERNAL_ERROR, format!("{:#}", e))
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
                    .error(
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "prepared statement does not exist",
                    )
                    .await;
            }
        };

        let param_types = stmt.param_types();
        if param_types.len() != raw_params.len() {
            let message = format!(
                "bind message supplies {actual} parameters, \
                 but prepared statement \"{name}\" requires {expected}",
                name = statement_name,
                actual = raw_params.len(),
                expected = param_types.len()
            );
            return self.error(SqlState::PROTOCOL_VIOLATION, message).await;
        }
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => return self.error(SqlState::PROTOCOL_VIOLATION, msg).await,
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
                        return self.error(SqlState::INVALID_PARAMETER_VALUE, msg).await;
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
            Err(msg) => return self.error(SqlState::PROTOCOL_VIOLATION, msg).await,
        };

        self.coord_client
            .session()
            .set_portal(portal_name, statement_name, params, result_formats)
            .unwrap();

        self.conn.send(BackendMessage::BindComplete).await?;
        Ok(State::Ready)
    }

    async fn execute(
        &mut self,
        portal_name: String,
        max_rows: usize,
    ) -> Result<State, comm::Error> {
        let session = self.coord_client.session();
        let row_desc = session
            .get_portal(&portal_name)
            .and_then(|portal| session.get_prepared_statement(&portal.statement_name))
            .and_then(|stmt| stmt.desc().relation_desc.clone());
        let portal = match self.coord_client.session().get_portal_mut(&portal_name) {
            Some(portal) => portal,
            None => {
                return self
                    .error(
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "portal does not exist",
                    )
                    .await;
            }
        };
        if let Some(rows) = portal.remaining_rows.take() {
            return self
                .send_rows(
                    row_desc.expect("portal missing row desc on resumption"),
                    portal_name,
                    rows,
                    max_rows,
                )
                .await;
        }

        match self.coord_client.execute(portal_name.clone()).await {
            Ok(response) => {
                self.send_execute_response(response, row_desc, portal_name, max_rows)
                    .await
            }
            Err(e) => {
                self.error(SqlState::INTERNAL_ERROR, format!("{:#}", e))
                    .await
            }
        }
    }

    async fn describe_statement(&mut self, name: String) -> Result<State, comm::Error> {
        let stmt = self.coord_client.session().get_prepared_statement(&name);
        match stmt {
            Some(stmt) => {
                self.conn
                    .send(BackendMessage::ParameterDescription(
                        stmt.param_types().to_vec(),
                    ))
                    .await?
            }
            None => {
                return self
                    .error(
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "prepared statement does not exist",
                    )
                    .await
            }
        }
        self.send_describe_rows(name).await
    }

    async fn describe_portal(&mut self, name: String) -> Result<State, comm::Error> {
        let stmt_name = self
            .coord_client
            .session()
            .get_portal(&name)
            .map(|portal| portal.statement_name.clone());
        match stmt_name {
            Some(stmt_name) => self.send_describe_rows(stmt_name).await,
            None => {
                self.error(
                    SqlState::INVALID_SQL_STATEMENT_NAME,
                    "portal does not exist",
                )
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

    async fn send_describe_rows(&mut self, stmt_name: String) -> Result<State, comm::Error> {
        let stmt = self
            .coord_client
            .session()
            .get_prepared_statement(&stmt_name)
            .expect("send_describe_statement called incorrectly");
        match stmt.desc().relation_desc() {
            Some(desc) => {
                self.conn
                    .send(BackendMessage::RowDescription(
                        message::row_description_from_desc(&desc),
                    ))
                    .await?
            }
            None => self.conn.send(BackendMessage::NoData).await?,
        }
        Ok(State::Ready)
    }

    async fn send_execute_response(
        &mut self,
        response: ExecuteResponse,
        row_desc: Option<RelationDesc>,
        portal_name: String,
        max_rows: usize,
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
                    self.conn
                        .send(BackendMessage::NoticeResponse {
                            severity: NoticeSeverity::Notice,
                            code: $code,
                            message: concat!($type, " already exists, skipping").into(),
                            detail: None,
                            hint: None,
                        })
                        .await?;
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
            ExecuteResponse::Deleted(n) => command_complete!("DELETE {}", n),
            ExecuteResponse::DroppedDatabase => command_complete!("DROP DATABASE"),
            ExecuteResponse::DroppedSchema => command_complete!("DROP SCHEMA"),
            ExecuteResponse::DroppedSource => command_complete!("DROP SOURCE"),
            ExecuteResponse::DroppedIndex => command_complete!("DROP INDEX"),
            ExecuteResponse::DroppedSink => command_complete!("DROP SINK"),
            ExecuteResponse::DroppedTable => command_complete!("DROP TABLE"),
            ExecuteResponse::DroppedView => command_complete!("DROP VIEW"),
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
                        self.error(
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        )
                        .await
                    }
                    PeekResponse::Error(text) => self.error(SqlState::INTERNAL_ERROR, text).await,
                    PeekResponse::Rows(rows) => {
                        self.send_rows(
                            row_desc,
                            portal_name,
                            Box::new(stream::iter(vec![Ok(rows)])),
                            max_rows,
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
            ExecuteResponse::CommittedTransaction => command_complete!("COMMIT"),
            ExecuteResponse::AbortedTransaction => command_complete!("ROLLBACK"),
            ExecuteResponse::Tailing { rx } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::Tailing");
                self.send_rows(row_desc, portal_name, Box::new(rx), max_rows)
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
                                .error(
                                    SqlState::QUERY_CANCELED,
                                    "canceling statement due to user request",
                                )
                                .await;
                        }
                        PeekResponse::Error(text) => {
                            return self.error(SqlState::INTERNAL_ERROR, text).await;
                        }
                        PeekResponse::Rows(rows) => Box::new(stream::iter(vec![Ok(rows)])),
                    },
                    _ => {
                        return self
                            .error(
                                SqlState::INTERNAL_ERROR,
                                "unsupported COPY response type".to_string(),
                            )
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

    async fn send_rows(
        &mut self,
        row_desc: RelationDesc,
        portal_name: String,
        mut rows: RowBatchStream,
        max_rows: usize,
    ) -> Result<State, comm::Error> {
        let portal = self
            .coord_client
            .session()
            .get_portal_mut(&portal_name)
            .expect("valid portal name for send rows");

        let mut batch = rows.try_next().await?;
        if let Some([row, ..]) = batch.as_deref() {
            let datums = row.unpack();
            let col_types = &row_desc.typ().column_types;
            if datums.len() != col_types.len() {
                return self
                    .error(
                        SqlState::INTERNAL_ERROR,
                        format!(
                            "internal error: row descriptor has {} columns but row has {} columns",
                            col_types.len(),
                            datums.len(),
                        ),
                    )
                    .await;
            }
            for (i, (d, t)) in datums.iter().zip(col_types).enumerate() {
                if !d.is_instance_of(&t) {
                    return self
                        .error(
                            SqlState::INTERNAL_ERROR,
                            format!(
                                "internal error: column {} is not of expected type {}: {}",
                                i, t, d
                            ),
                        )
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
                .zip(portal.result_formats.iter().copied())
                .collect(),
        );

        let mut total_sent_rows = 0;
        // want_rows is the maximum number of rows the client wants.
        let mut want_rows = if max_rows == 0 { usize::MAX } else { max_rows };

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
            batch = rows.try_next().await?;
        }

        ROWS_RETURNED.inc_by(u64::cast_from(total_sent_rows));

        // Always return rows back, even if it's empty. This prevents an unclosed
        // portal from re-executing after it has been emptied. into_inner unwraps the
        // Take.
        portal.set_remaining_rows(Box::new(rows));

        // If max_rows is not specified, we will always send back a CommandComplete. If
        // max_rows is specified, we only send CommandComplete if there were more rows
        // requested than were remaining. That is, if max_rows == number of rows that
        // were remaining before sending (not that are remaining after sending), then
        // we still send a PortalSuspended. The number of remaining rows after the rows
        // have been sent doesn't matter. This matches postgres.
        if max_rows == 0 || max_rows > total_sent_rows {
            self.conn
                .send(BackendMessage::CommandComplete {
                    tag: format!("SELECT {}", total_sent_rows),
                })
                .await?;
        } else {
            self.conn.send(BackendMessage::PortalSuspended).await?;
        }

        Ok(State::Ready)
    }

    async fn copy_rows(
        &mut self,
        format: CopyFormat,
        row_desc: RelationDesc,
        mut stream: RowBatchStream,
    ) -> Result<State, comm::Error> {
        match format {
            CopyFormat::Text => {}
            _ => {
                return self
                    .error(
                        SqlState::FEATURE_NOT_SUPPORTED,
                        format!("COPY TO format {:?} not supported", format),
                    )
                    .await
            }
        };

        let typ = row_desc.typ();
        let column_formats = iter::repeat(pgrepr::Format::Text)
            .take(typ.column_types.len())
            .collect();
        self.conn
            .send(BackendMessage::CopyOutResponse {
                overall_format: pgrepr::Format::Text,
                column_formats,
            })
            .await?;

        let mut count = 0;
        loop {
            match time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(None) => break,
                Ok(Some(rows)) => {
                    let rows = rows?;
                    count += rows.len();
                    for row in rows {
                        self.conn
                            .send(BackendMessage::CopyData(message::encode_copy_row_text(
                                row, typ,
                            )))
                            .await?;
                    }
                }
                Err(time::Elapsed { .. }) => {
                    // It's been a while since we've had any data to send, and
                    // the client may have disconnected. Send a data message
                    // with zero bytes of data, which will error if the client
                    // has, in fact, disconnected. Otherwise we might block
                    // forever waiting for rows, leaking memory and a socket.
                    //
                    // Writing these empty data packets is rather distasteful,
                    // but no better solution for detecting a half-closed socket
                    // presents itself. Tokio/Mio don't provide a cross-platform
                    // means of receiving socket closed notifications, and it's
                    // not clear how to plumb such notifications through a
                    // `Codec` and a `Framed`, anyway.
                    //
                    // If someone does wind up investigating a better solution,
                    // on Linux, the underlying epoll system call supports the
                    // desired notifications via POLLRDHUP [0].
                    //
                    // [0]: https://lkml.org/lkml/2003/7/12/116
                    self.conn.send(BackendMessage::CopyData(vec![])).await?;
                }
            }
            self.conn.flush().await?;
        }

        let tag = format!("COPY {}", count);
        self.conn.send(BackendMessage::CopyDone).await?;
        self.conn
            .send(BackendMessage::CommandComplete { tag })
            .await?;
        Ok(State::Ready)
    }

    async fn error(
        &mut self,
        code: SqlState,
        message: impl Into<String>,
    ) -> Result<State, comm::Error> {
        let message = message.into();
        debug!(
            "cid={} error code={} message={}",
            self.conn_id,
            code.code(),
            message
        );
        self.conn
            .send(BackendMessage::ErrorResponse {
                severity: ErrorSeverity::Error,
                code,
                message,
                detail: None,
            })
            .await?;
        self.coord_client.session().fail_transaction();
        Ok(State::Drain)
    }

    async fn fatal(
        &mut self,
        code: SqlState,
        message: impl Into<String>,
    ) -> Result<State, comm::Error> {
        let message = message.into();
        debug!(
            "cid={} fatal code={} message={}",
            self.conn_id,
            code.code(),
            message
        );
        self.conn
            .send(BackendMessage::ErrorResponse {
                severity: ErrorSeverity::Fatal,
                code,
                message,
                detail: None,
            })
            .await?;
        Ok(State::Done)
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
