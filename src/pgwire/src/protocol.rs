// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;
use std::mem;
use std::time::Instant;

use byteorder::{ByteOrder, NetworkEndian};
use futures::sink::{self, SinkExt};
use futures::stream::{StreamExt, TryStreamExt};
use itertools::izip;
use lazy_static::lazy_static;
use log::{debug, trace};
use postgres::error::SqlState;
use prometheus::{register_histogram_vec, register_int_counter};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Duration};
use tokio_util::codec::Framed;

use coord::session::Session;
use coord::{ExecuteResponse, StartupMessage};
use dataflow_types::{PeekResponse, Update};
use ore::future::OreSinkExt;
use repr::{Datum, RelationDesc, Row, RowArena};
use sql::ast::Statement;

use crate::codec::Codec;
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
    static ref ROWS_RETURNED: prometheus::IntCounter = register_int_counter!(
        "mz_pg_sent_rows",
        "total number of rows sent to clients from pgwire"
    )
    .unwrap();
}

#[derive(Debug)]
enum State {
    Ready(Session),
    Drain(Session),
    Done,
}

impl State {
    fn take(&mut self) -> State {
        mem::replace(self, State::Done)
    }
}

pub struct StateMachine<'a, A> {
    pub conn: &'a mut sink::Buffer<Framed<A, Codec>, BackendMessage>,
    pub conn_id: u32,
    pub secret_key: u32,
    pub cmdq_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn start(
        &mut self,
        session: Session,
        version: i32,
        params: Vec<(String, String)>,
    ) -> Result<(), comm::Error> {
        let mut state = self.startup(session, version, params).await?;

        loop {
            state = match state.take() {
                State::Ready(session) => self.advance_ready(session).await?,
                State::Drain(session) => self.advance_drain(session).await?,
                State::Done => return Ok(()),
            }
        }
    }

    async fn advance_ready(&mut self, session: Session) -> Result<State, comm::Error> {
        let message = self.recv().await?;
        let timer = Instant::now();
        let name = match &message {
            Some(message) => message.name(),
            None => "eof",
        };

        let next_state = match message {
            Some(FrontendMessage::Query { sql }) => self.query(session, sql).await?,
            Some(FrontendMessage::Parse { name, sql, .. }) => {
                self.parse(session, name, sql).await?
            }
            Some(FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            }) => {
                self.bind(
                    session,
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
            }) => self.execute(session, portal_name, max_rows).await?,
            Some(FrontendMessage::DescribeStatement { name }) => {
                self.describe_statement(session, name).await?
            }
            Some(FrontendMessage::DescribePortal { name }) => {
                self.describe_portal(session, name).await?
            }
            Some(FrontendMessage::CloseStatement { name }) => {
                self.close_statement(session, name).await?
            }
            Some(FrontendMessage::ClosePortal { name }) => self.close_portal(session, name).await?,
            Some(FrontendMessage::Flush) => self.flush(session).await?,
            Some(FrontendMessage::Sync) => self.sync(session).await?,
            Some(FrontendMessage::Terminate) => State::Done,
            None => State::Done,
        };

        let status = match next_state {
            State::Ready(_) | State::Done => "success",
            State::Drain(_) => "error",
        };
        COMMAND_DURATIONS
            .with_label_values(&[name, status])
            .observe(timer.elapsed().as_secs_f64());

        Ok(next_state)
    }

    async fn advance_drain(&mut self, session: Session) -> Result<State, comm::Error> {
        match self.recv().await? {
            Some(FrontendMessage::Sync) => self.sync(session).await,
            None => Ok(State::Done),
            _ => Ok(State::Drain(session)),
        }
    }

    async fn startup(
        &mut self,
        mut session: Session,
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
            let _ = session.set(&name, &value);
        }

        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmdq_tx
            .send(coord::Command::Startup { session, tx })
            .await?;
        let (notices, session) = match rx.await? {
            coord::Response {
                result: Ok(messages),
                session,
            } => {
                let notices: Vec<_> = messages
                    .into_iter()
                    .map(|m| match m {
                        StartupMessage::UnknownSessionDatabase => BackendMessage::NoticeResponse {
                            severity: NoticeSeverity::Notice,
                            code: SqlState::SUCCESSFUL_COMPLETION,
                            message: format!(
                                "session database '{}' does not exist",
                                session.database()
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
                    .collect();
                (notices, session)
            }
            coord::Response {
                result: Err(err),
                session,
            } => {
                return self
                    .error(session, SqlState::INTERNAL_ERROR, err.to_string())
                    .await;
            }
        };

        let mut messages = vec![BackendMessage::AuthenticationOk];
        messages.extend(
            session
                .notify_vars()
                .iter()
                .map(|v| BackendMessage::ParameterStatus(v.name(), v.value())),
        );
        messages.push(BackendMessage::BackendKeyData {
            conn_id: self.conn_id,
            secret_key: self.secret_key,
        });
        messages.extend(notices);
        messages.push(BackendMessage::ReadyForQuery(session.transaction().into()));
        self.send_all(messages).await?;
        self.flush(session).await
    }

    async fn one_query(&mut self, session: Session, stmt: Statement) -> Result<State, comm::Error> {
        let stmt_name = String::from("");
        let portal_name = String::from("");
        let (tx, rx) = futures::channel::oneshot::channel();
        let cmd = coord::Command::Describe {
            name: stmt_name.clone(),
            stmt: Some(stmt),
            session,
            tx,
        };

        self.cmdq_tx.send(cmd).await?;
        let mut session = match rx.await? {
            coord::Response {
                result: Ok(()),
                session,
            } => session,
            coord::Response {
                result: Err(err),
                session,
            } => {
                return self
                    .error(session, SqlState::INTERNAL_ERROR, err.to_string())
                    .await;
            }
        };

        let stmt = session.get_prepared_statement("").unwrap();
        if !stmt.param_types().is_empty() {
            return self
                .error(
                    session,
                    SqlState::UNDEFINED_PARAMETER,
                    "there is no parameter $1",
                )
                .await;
        }

        let row_desc = stmt.desc().cloned();

        // Bind.
        let params = vec![];
        let result_formats = vec![pgrepr::Format::Text; stmt.result_width()];
        session
            .set_portal(
                portal_name.clone(),
                stmt_name.clone(),
                params,
                result_formats,
            )
            .expect("unnamed statement to be present during simple query flow");

        // Maybe send row description.
        if let Some(desc) = &row_desc {
            self.send(BackendMessage::RowDescription(
                message::row_description_from_desc(&desc),
            ))
            .await?;
        }

        // Execute.
        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmdq_tx
            .send(coord::Command::Execute {
                portal_name: portal_name.clone(),
                session,
                tx,
            })
            .await?;
        match rx.await? {
            coord::Response {
                result: Ok(response),
                session,
            } => {
                let max_rows = 0;
                self.send_execute_response(session, response, row_desc, portal_name, max_rows)
                    .await
            }
            coord::Response {
                result: Err(err),
                session,
            } => {
                self.error(session, SqlState::INTERNAL_ERROR, err.to_string())
                    .await
            }
        }
    }

    async fn query(&mut self, mut session: Session, sql: String) -> Result<State, comm::Error> {
        let stmts = match sql::parse::parse(sql) {
            Ok(stmts) => stmts,
            Err(err) => {
                let session = match self
                    .error(session, SqlState::SYNTAX_ERROR, err.to_string())
                    .await?
                {
                    State::Drain(s) => s,
                    _ => unreachable!(),
                };
                return self.sync(session).await;
            }
        };
        for stmt in stmts {
            match self.one_query(session, stmt).await? {
                State::Ready(s) => session = s,
                State::Drain(s) => return self.sync(s).await,
                State::Done => return Ok(State::Done),
            }
        }
        self.sync(session).await
    }

    async fn parse(
        &mut self,
        session: Session,
        name: String,
        sql: String,
    ) -> Result<State, comm::Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let stmts = match sql::parse::parse(sql.clone()) {
            Ok(stmts) => stmts,
            Err(err) => {
                return self
                    .error(session, SqlState::SYNTAX_ERROR, err.to_string())
                    .await;
            }
        };
        if stmts.len() > 1 {
            return self
                .error(
                    session,
                    SqlState::INTERNAL_ERROR,
                    "cannot insert multiple commands into a prepared statement",
                )
                .await;
        }
        let maybe_stmt = stmts.into_iter().next();
        let cmd = coord::Command::Describe {
            name,
            stmt: maybe_stmt,
            session,
            tx,
        };
        self.cmdq_tx.send(cmd).await?;

        match rx.await? {
            coord::Response {
                result: Ok(()),
                session,
            } => {
                self.send(BackendMessage::ParseComplete).await?;
                Ok(State::Ready(session))
            }
            coord::Response {
                result: Err(err),
                session,
            } => {
                self.error(session, SqlState::INTERNAL_ERROR, err.to_string())
                    .await
            }
        }
    }

    async fn bind(
        &mut self,
        mut session: Session,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<pgrepr::Format>,
        raw_params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<State, comm::Error> {
        let stmt = match session.get_prepared_statement(&statement_name) {
            Some(stmt) => stmt,
            None => {
                return self
                    .error(
                        session,
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
            return self
                .error(session, SqlState::PROTOCOL_VIOLATION, message)
                .await;
        }
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => return self.error(session, SqlState::PROTOCOL_VIOLATION, msg).await,
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
                            .error(session, SqlState::INVALID_PARAMETER_VALUE, msg)
                            .await;
                    }
                },
            }
        }

        let result_formats = match pad_formats(
            result_formats,
            stmt.desc()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0),
        ) {
            Ok(result_formats) => result_formats,
            Err(msg) => return self.error(session, SqlState::PROTOCOL_VIOLATION, msg).await,
        };

        session
            .set_portal(portal_name, statement_name, params, result_formats)
            .unwrap();

        self.send(BackendMessage::BindComplete).await?;
        Ok(State::Ready(session))
    }

    async fn execute(
        &mut self,
        mut session: Session,
        portal_name: String,
        max_rows: i32,
    ) -> Result<State, comm::Error> {
        let row_desc = session
            .get_prepared_statement_for_portal(&portal_name)
            .and_then(|stmt| stmt.desc().cloned());
        let portal = match session.get_portal_mut(&portal_name) {
            Some(portal) => portal,
            None => {
                return self
                    .error(
                        session,
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "portal does not exist",
                    )
                    .await;
            }
        };
        if portal.remaining_rows.is_some() {
            let rows = portal.remaining_rows.take().unwrap();
            return self
                .send_rows(
                    session,
                    row_desc.expect("portal missing row desc on resumption"),
                    portal_name,
                    rows,
                    max_rows,
                )
                .await;
        }

        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmdq_tx
            .send(coord::Command::Execute {
                portal_name: portal_name.clone(),
                session,
                tx,
            })
            .await?;
        match rx.await? {
            coord::Response {
                result: Ok(response),
                session,
            } => {
                self.send_execute_response(session, response, row_desc, portal_name, max_rows)
                    .await
            }
            coord::Response {
                result: Err(err),
                session,
            } => {
                self.error(session, SqlState::INTERNAL_ERROR, err.to_string())
                    .await
            }
        }
    }

    async fn describe_statement(
        &mut self,
        session: Session,
        name: String,
    ) -> Result<State, comm::Error> {
        match session.get_prepared_statement(&name) {
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
                        session,
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "prepared statement does not exist",
                    )
                    .await
            }
        }
        self.send_describe_rows(session, name).await
    }

    async fn describe_portal(
        &mut self,
        session: Session,
        name: String,
    ) -> Result<State, comm::Error> {
        let portal = match session.get_portal(&name) {
            Some(portal) => portal,
            None => {
                return self
                    .error(
                        session,
                        SqlState::INVALID_SQL_STATEMENT_NAME,
                        "portal does not exist",
                    )
                    .await
            }
        };
        let stmt_name = portal.statement_name.clone();
        self.send_describe_rows(session, stmt_name).await
    }

    async fn close_statement(
        &mut self,
        mut session: Session,
        name: String,
    ) -> Result<State, comm::Error> {
        session.remove_prepared_statement(&name);
        self.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready(session))
    }

    async fn close_portal(
        &mut self,
        mut session: Session,
        name: String,
    ) -> Result<State, comm::Error> {
        session.remove_portal(&name);
        self.send(BackendMessage::CloseComplete).await?;
        Ok(State::Ready(session))
    }

    async fn flush(&mut self, session: Session) -> Result<State, comm::Error> {
        self.conn.flush().await?;
        Ok(State::Ready(session))
    }

    async fn sync(&mut self, session: Session) -> Result<State, comm::Error> {
        self.conn
            .send(BackendMessage::ReadyForQuery(session.transaction().into()))
            .await?;
        self.flush(session).await
    }

    async fn send_describe_rows(
        &mut self,
        session: Session,
        stmt_name: String,
    ) -> Result<State, comm::Error> {
        let stmt = session
            .get_prepared_statement(&stmt_name)
            .expect("send_describe_statement called incorrectly");
        match stmt.desc() {
            Some(desc) => {
                self.conn
                    .send(BackendMessage::RowDescription(
                        message::row_description_from_desc(&desc),
                    ))
                    .await?
            }
            None => self.send(BackendMessage::NoData).await?,
        }
        Ok(State::Ready(session))
    }

    async fn send_execute_response(
        &mut self,
        session: Session,
        response: ExecuteResponse,
        row_desc: Option<RelationDesc>,
        portal_name: String,
        max_rows: i32,
    ) -> Result<State, comm::Error> {
        macro_rules! command_complete {
            ($($arg:tt)*) => {{
                // N.B.: the output of format! must be stored into a
                // variable, or rustc barfs out a completely inscrutable
                // error: https://github.com/rust-lang/rust/issues/64960.
                let tag = format!($($arg)*);
                self.send(BackendMessage::CommandComplete { tag }).await?;
                Ok(State::Ready(session))
            }};
        }

        macro_rules! created {
            ($existed:expr, $code:expr, $type:expr) => {{
                if $existed {
                    self.send(BackendMessage::NoticeResponse {
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
                self.send(BackendMessage::EmptyQueryResponse).await?;
                Ok(State::Ready(session))
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
                            session,
                            SqlState::QUERY_CANCELED,
                            "canceling statement due to user request",
                        )
                        .await
                    }
                    PeekResponse::Error(text) => {
                        self.error(session, SqlState::INTERNAL_ERROR, text).await
                    }
                    PeekResponse::Rows(rows) => {
                        self.send_rows(session, row_desc, portal_name, rows, max_rows)
                            .await
                    }
                }
            }
            ExecuteResponse::SetVariable { name } => {
                // This code is somewhat awkwardly structured because we
                // can't hold `var` across an await point.
                let qn = name.to_string();
                let msg = if let Some(var) = session.notify_vars().iter().find(|v| v.name() == qn) {
                    Some(BackendMessage::ParameterStatus(var.name(), var.value()))
                } else {
                    None
                };
                if let Some(msg) = msg {
                    self.send(msg).await?;
                }
                command_complete!("SET")
            }
            ExecuteResponse::StartedTransaction => command_complete!("BEGIN"),
            ExecuteResponse::CommittedTransaction => command_complete!("COMMIT"),
            ExecuteResponse::AbortedTransaction => command_complete!("ROLLBACK"),
            ExecuteResponse::Tailing { rx } => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::Tailing");
                self.stream_rows(session, row_desc, rx).await
            }
            ExecuteResponse::Updated(n) => command_complete!("UPDATE {}", n),
        }
    }

    async fn send_rows(
        &mut self,
        mut session: Session,
        row_desc: RelationDesc,
        portal_name: String,
        mut rows: Vec<Row>,
        max_rows: i32,
    ) -> Result<State, comm::Error> {
        let portal = session
            .get_portal_mut(&portal_name)
            .expect("valid portal name for send rows");

        if let Some(row) = rows.first() {
            let datums = row.unpack();
            let col_types = &row_desc.typ().column_types;
            if datums.len() != col_types.len() {
                return self
                    .error(
                        session,
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
                            session,
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

        self.conn.get_mut().codec_mut().set_encode_state(
            row_desc
                .typ()
                .column_types
                .iter()
                .map(|ty| pgrepr::Type::from(&ty.scalar_type))
                .zip(portal.result_formats.iter().copied())
                .collect(),
        );

        let mut row_count = 0u32;
        {
            let row_count = &mut row_count;
            self.send_all(
                if max_rows > 0 && (max_rows as usize) < rows.len() {
                    rows.drain(..max_rows as usize)
                } else {
                    rows.drain(..)
                }
                .map(move |row| {
                    *row_count += 1;
                    BackendMessage::DataRow(pgrepr::values_from_row(row, row_desc.typ()))
                }),
            )
            .await?;
        }
        ROWS_RETURNED.inc_by(i64::from(row_count));

        if rows.is_empty() {
            self.send(BackendMessage::CommandComplete {
                tag: format!("SELECT {}", row_count),
            })
            .await?;
        } else {
            portal.set_remaining_rows(rows);
            self.send(BackendMessage::PortalSuspended).await?;
        }

        Ok(State::Ready(session))
    }

    async fn stream_rows(
        &mut self,
        session: Session,
        row_desc: RelationDesc,
        mut rx: comm::mpsc::Receiver<Vec<Update>>,
    ) -> Result<State, comm::Error> {
        let typ = row_desc.typ();
        let column_formats = iter::repeat(pgrepr::Format::Text)
            .take(typ.column_types.len())
            .collect();
        self.send(BackendMessage::CopyOutResponse {
            overall_format: pgrepr::Format::Text,
            column_formats,
        })
        .await?;

        let mut count = 0;
        loop {
            match time::timeout(Duration::from_secs(1), rx.next()).await {
                Ok(None) => break,
                Ok(Some(updates)) => {
                    let updates = updates?;
                    count += updates.len();
                    for update in updates {
                        self.send(BackendMessage::CopyData(message::encode_update(
                            update, typ,
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
                    self.send(BackendMessage::CopyData(vec![])).await?;
                }
            }
            self.conn.flush().await?;
        }

        let tag = format!("COPY {}", count);
        self.send(BackendMessage::CopyDone).await?;
        self.send(BackendMessage::CommandComplete { tag }).await?;
        Ok(State::Ready(session))
    }

    async fn recv(&mut self) -> Result<Option<FrontendMessage>, comm::Error> {
        let message = self.conn.try_next().await?;
        match &message {
            Some(message) => trace!("cid={} recv={:?}", self.conn_id, message),
            None => trace!("cid={} recv=<eof>", self.conn_id),
        }
        Ok(message)
    }

    async fn send(&mut self, message: BackendMessage) -> Result<(), comm::Error> {
        trace!("cid={} send={:?}", self.conn_id, message);
        Ok(self.conn.enqueue(message).await?)
    }

    async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<(), comm::Error> {
        // N.B. we intentionally don't use `self.conn.send_all` here to avoid
        // flushing the sink unnecessarily.
        for m in messages {
            self.send(m).await?;
        }
        Ok(())
    }

    async fn error(
        &mut self,
        mut session: Session,
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
        session.fail_transaction();
        Ok(State::Drain(session))
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
