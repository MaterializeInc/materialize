// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::iter;
use std::mem;
use std::sync::Arc;
use std::time::Instant;

use byteorder::{ByteOrder, NetworkEndian};
use failure::bail;
use futures::sink::SinkExt;
use futures::stream::{self, TryStreamExt};
use lazy_static::lazy_static;
use log::{debug, trace};
use prometheus::register_histogram_vec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use coord::ExecuteResponse;
use dataflow_types::{PeekResponse, Update};
use repr::{RelationDesc, Row};
use sql::Session;

use crate::codec::{Codec, RawParameterBytes};
use crate::id_alloc::{IdAllocator, IdExhaustionError};
use crate::message::{
    self, BackendMessage, EncryptionType, FrontendMessage, ParameterDescription, Severity,
    VERSIONS, VERSION_3,
};
use crate::secrets::SecretManager;

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
        ore::stats::HISTOGRAM_BUCKETS.to_vec(),
        expose_decumulated => true
    )
    .unwrap();
}

/// Handles an incoming pgwire connection.
pub async fn serve<A>(
    conn: A,
    cmdq_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
    gather_metrics: bool,
) -> Result<(), failure::Error>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    lazy_static! {
        static ref CONN_ID_ALLOCATOR: IdAllocator = IdAllocator::new(1, 1 << 16);
        static ref CONN_SECRETS: SecretManager = SecretManager::new();
    }

    let conn_id = match CONN_ID_ALLOCATOR.alloc() {
        Ok(id) => id,
        Err(IdExhaustionError) => {
            bail!("maximum number of connections reached");
        }
    };
    CONN_SECRETS.generate(conn_id);

    let mut machine = StateMachine {
        conn: &mut Framed::new(conn, Codec::new()),
        conn_id,
        conn_secrets: CONN_SECRETS.clone(),
        cmdq_tx,
        gather_metrics,
    };
    let res = machine.start(Session::default()).await;

    CONN_ID_ALLOCATOR.free(conn_id);
    CONN_SECRETS.free(conn_id);
    Ok(res?)
}

#[derive(Debug)]
enum State {
    Startup(Session),
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
    conn: &'a mut Framed<A, Codec>,
    conn_id: u32,
    conn_secrets: SecretManager,
    cmdq_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
    gather_metrics: bool,
}

impl<'a, A> StateMachine<'a, A>
where
    A: AsyncRead + AsyncWrite + Unpin,
{
    async fn start(&mut self, session: Session) -> Result<(), comm::Error> {
        let mut state = State::Startup(session);

        loop {
            state = match state.take() {
                State::Startup(session) => self.advance_startup(session).await?,
                State::Ready(session) => self.advance_ready(session).await?,
                State::Drain(session) => self.advance_drain(session).await?,
                State::Done => return Ok(()),
            };
            if let State::Startup(_) = state {
                // If we haven't left the startup state, we need to tell the
                // decoder to expect another startup message, as startup
                // messages don't have a message type header.
                self.conn.codec_mut().reset_decode_state();
            }
        }
    }

    async fn advance_startup(&mut self, session: Session) -> Result<State, comm::Error> {
        match self.recv().await? {
            Some(FrontendMessage::Startup { version }) => self.startup(session, version).await,
            Some(FrontendMessage::CancelRequest {
                conn_id,
                secret_key,
            }) => self.cancel_request(conn_id, secret_key).await,
            Some(FrontendMessage::GssEncRequest) | Some(FrontendMessage::SslRequest) => {
                self.encryption_request(session).await
            }
            None => Ok(State::Done),
            _ => self.fatal("08P01", "invalid startup message flow").await,
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
                raw_parameter_bytes,
                result_formats,
            }) => {
                self.bind(
                    session,
                    portal_name,
                    statement_name,
                    raw_parameter_bytes,
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
            Some(FrontendMessage::Sync) => self.sync(session).await?,
            Some(FrontendMessage::Terminate) => State::Done,
            None => State::Done,
            _ => self.fatal("08P01", "invalid ready message flow").await?,
        };

        if self.gather_metrics {
            let status = match next_state {
                State::Startup(_) => unreachable!(),
                State::Ready(_) | State::Done => "success",
                State::Drain(_) => "error",
            };
            COMMAND_DURATIONS
                .with_label_values(&[name, status])
                .observe(timer.elapsed().as_secs_f64());
        }

        Ok(next_state)
    }

    async fn advance_drain(&mut self, session: Session) -> Result<State, comm::Error> {
        match self.recv().await? {
            Some(FrontendMessage::Sync) => self.sync(session).await,
            None => Ok(State::Done),
            _ => Ok(State::Drain(session)),
        }
    }

    async fn startup(&mut self, session: Session, version: i32) -> Result<State, comm::Error> {
        if version != VERSION_3 {
            return self
                .fatal(
                    "08004",
                    "server does not support the client's requested protocol version",
                )
                .await;
        }

        let mut messages = vec![BackendMessage::AuthenticationOk];
        messages.extend(
            session
                .notify_vars()
                .iter()
                .map(|v| BackendMessage::ParameterStatus(v.name(), v.value())),
        );
        messages.push(BackendMessage::BackendKeyData {
            conn_id: self.conn_id,
            secret_key: self.conn_secrets.get(self.conn_id).unwrap(),
        });
        messages.push(BackendMessage::ReadyForQuery(session.transaction().into()));
        self.send_all(messages).await?;

        Ok(State::Ready(session))
    }

    async fn cancel_request(
        &mut self,
        conn_id: u32,
        secret_key: u32,
    ) -> Result<State, comm::Error> {
        if self.conn_secrets.verify(conn_id, secret_key) {
            self.cmdq_tx
                .send(coord::Command::CancelRequest { conn_id })
                .await?;
        }
        // For security, the client is not told whether the cancel
        // request succeeds or fails.
        Ok(State::Done)
    }

    async fn encryption_request(&mut self, session: Session) -> Result<State, comm::Error> {
        self.send(BackendMessage::EncryptionResponse(EncryptionType::None))
            .await?;
        Ok(State::Startup(session))
    }

    async fn query(&mut self, session: Session, sql: String) -> Result<State, comm::Error> {
        let run = async {
            let stmt_name = String::from("");
            let portal_name = String::from("");

            // Parse.
            let (tx, rx) = futures::channel::oneshot::channel();
            let cmd = coord::Command::Parse {
                name: stmt_name.clone(),
                sql,
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
                    return self.error(session, "99999", err.to_string()).await;
                }
            };

            let stmt = session.get_prepared_statement(&stmt_name).unwrap();
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
                    conn_id: self.conn_id,
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
                } => self.error(session, "99999", err.to_string()).await,
            }
        };
        match run.await? {
            State::Startup(_) => unreachable!(),
            State::Ready(session) | State::Drain(session) => self.sync(session).await,
            State::Done => Ok(State::Done),
        }
    }

    async fn parse(
        &mut self,
        session: Session,
        name: String,
        sql: String,
    ) -> Result<State, comm::Error> {
        let (tx, rx) = futures::channel::oneshot::channel();

        let cmd = coord::Command::Parse {
            name,
            sql,
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
            } => self.error(session, "99999", err.to_string()).await,
        }
    }

    async fn bind(
        &mut self,
        mut session: Session,
        portal_name: String,
        statement_name: String,
        raw_parameter_bytes: RawParameterBytes,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<State, comm::Error> {
        let stmt = match session.get_prepared_statement(&statement_name) {
            Some(stmt) => stmt,
            None => {
                return self
                    .error(session, "26000", "prepared statement does not exist")
                    .await;
            }
        };
        let param_types = stmt.param_types();
        let params = match raw_parameter_bytes.decode_parameters(param_types) {
            Ok(params) => params,
            Err(err) => return self.error(session, "08P01", err.to_string()).await,
        };
        let fmts = match (
            result_formats.len(),
            stmt.desc()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0),
        ) {
            (0, e) => vec![pgrepr::Format::Text; e],
            (1, e) => iter::repeat(result_formats[0]).take(e).collect(),
            (a, e) if a == e => result_formats,
            (a, e) => {
                let msg = format!("expected {} field format specifiers, but got {}", e, a);
                return self.error(session, "08P01", msg).await;
            }
        };
        session
            .set_portal(portal_name, statement_name, params, fmts)
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
                return self.error(session, "26000", "portal does not exist").await;
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
                conn_id: self.conn_id,
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
            } => self.error(session, "99999", err.to_string()).await,
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
                        stmt.param_types()
                            .iter()
                            .map(ParameterDescription::from)
                            .collect(),
                    ))
                    .await?
            }
            None => {
                return self
                    .error(session, "26000", "prepared statement does not exist")
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
            None => return self.error(session, "26000", "portal does not exist").await,
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

    async fn sync(&mut self, session: Session) -> Result<State, comm::Error> {
        self.conn
            .send(BackendMessage::ReadyForQuery(session.transaction().into()))
            .await?;
        Ok(State::Ready(session))
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

        match response {
            ExecuteResponse::CreatedIndex => command_complete!("CREATE INDEX"),
            ExecuteResponse::CreatedSource => command_complete!("CREATE SOURCE"),
            ExecuteResponse::CreatedSink => command_complete!("CREATE SINK"),
            ExecuteResponse::CreatedTable => command_complete!("CREATE TABLE"),
            ExecuteResponse::CreatedView => command_complete!("CREATE VIEW"),
            ExecuteResponse::Deleted(n) => command_complete!("DELETE {}", n),
            ExecuteResponse::DroppedSource => command_complete!("DROP SOURCE"),
            ExecuteResponse::DroppedIndex => command_complete!("DROP INDEX"),
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
            ExecuteResponse::SendRows(rx) => {
                let row_desc =
                    row_desc.expect("missing row description for ExecuteResponse::SendRows");
                match rx.await? {
                    PeekResponse::Canceled => {
                        self.error(session, "57014", "canceling statement due to user request")
                            .await
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
            ExecuteResponse::StartTransaction => command_complete!("BEGIN"),
            ExecuteResponse::Commit => command_complete!("COMMIT TRANSACTION"),
            ExecuteResponse::Rollback => command_complete!("ROLLBACK TRANSACTION"),
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
        let formats: Arc<Vec<pgrepr::Format>> = Arc::new(portal.result_formats.clone());

        self.send_all(
            if max_rows > 0 && (max_rows as usize) < rows.len() {
                rows.drain(..max_rows as usize)
            } else {
                rows.drain(..)
            }
            .map(move |row| {
                BackendMessage::DataRow(
                    pgrepr::values_from_row(row, row_desc.typ()),
                    formats.clone(),
                )
            }),
        )
        .await?;

        if rows.is_empty() {
            self.send(BackendMessage::CommandComplete {
                tag: "SELECT".into(),
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
        while let Some(updates) = rx.try_next().await? {
            count += updates.len();
            let messages = updates
                .into_iter()
                .map(|update| BackendMessage::CopyData(message::encode_update(update, typ)));
            self.send_all(messages).await?;
        }

        let tag = format!("COPY {}", count);
        self.send(BackendMessage::CopyDone).await?;
        self.send(BackendMessage::CommandComplete { tag }).await?;

        self.sync(session).await
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
        Ok(self.conn.send(message).await?)
    }

    async fn send_all(
        &mut self,
        messages: impl IntoIterator<Item = BackendMessage>,
    ) -> Result<(), comm::Error> {
        Ok(self
            .conn
            .send_all(&mut stream::iter(messages.into_iter().map(Ok)))
            .await?)
    }

    async fn error(
        &mut self,
        mut session: Session,
        code: &'static str,
        message: impl Into<String>,
    ) -> Result<State, comm::Error> {
        let message = message.into();
        debug!(
            "cid={} error code={} message={}",
            self.conn_id, code, message
        );
        self.conn
            .send(BackendMessage::ErrorResponse {
                severity: Severity::Error,
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
        code: &'static str,
        message: impl Into<String>,
    ) -> Result<State, comm::Error> {
        let message = message.into();
        debug!(
            "cid={} fatal code={} message={}",
            self.conn_id, code, message
        );
        self.conn
            .send(BackendMessage::ErrorResponse {
                severity: Severity::Fatal,
                code,
                message,
                detail: None,
            })
            .await?;
        Ok(State::Done)
    }
}
