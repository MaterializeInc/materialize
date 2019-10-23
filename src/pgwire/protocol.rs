// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::io::Write;
use std::iter;
use std::sync::Arc;

use byteorder::{ByteOrder, NetworkEndian};
use failure::format_err;
use futures::sink::Send as SinkSend;
use futures::stream;
use futures::sync::mpsc::UnboundedSender;
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use lazy_static::lazy_static;
use log::{debug, trace};
use state_machine_future::StateMachineFuture as Smf;
use state_machine_future::{transition, RentToOwn};
use std::str;
use tokio::codec::Framed;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::codec::Codec;
use crate::message::{
    self, BackendMessage, FieldFormat, FieldFormatIter, FrontendMessage, ParameterDescription,
    Severity, VERSIONS, VERSION_3,
};
use crate::secrets::SecretManager;
use coord::{self, ExecuteResponse};
use dataflow_types::{PeekResponse, Update};
use ore::future::{Recv, StreamExt};
use repr::{RelationDesc, Row};
use sql::Session;

use prometheus::IntCounterVec;

lazy_static! {
    /// The number of responses that we have ever sent to clients
    ///
    /// TODO: consider using prometheus-static-metric or a variation on its
    /// precompilation pattern to improve perf?
    /// https://github.com/pingcap/rust-prometheus/tree/master/static-metric
    static ref RESPONSES_SENT_COUNTER: IntCounterVec = register_int_counter_vec!(
        "mz_responses_sent_total",
        "Number of times we have have sent rows or errors back to clients",
        &["status", "kind"]
    )
    .unwrap();
}

pub struct Context {
    pub conn_id: u32,
    pub conn_secrets: SecretManager,
    pub cmdq_tx: UnboundedSender<coord::Command>,
    /// If true, we gather prometheus metrics
    pub gather_metrics: bool,
}

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

/// A trait representing a pgwire connection.
///
/// Implementors of this trait provide both a [`futures::Stream`] of pgwire frontend
/// messages and a [`futures::Sink`] of pgwire backend messages.
///
/// This trait exists primarly to ease the pain of writing type declarations
/// for [`StateMachine`]. When trait aliases land ([#41517]), this trait can
/// be replaced with a non-public trait alias.
///
/// [#41517]: https://github.com/rust-lang/rust/issues/41517
pub trait Conn:
    Stream<Item = FrontendMessage, Error = io::Error>
    + Sink<SinkItem = BackendMessage, SinkError = io::Error>
    + Send
{
}

impl<A> Conn for Framed<A, Codec> where A: AsyncWrite + AsyncRead + 'static + Send {}

type MessageStream = Box<dyn Stream<Item = BackendMessage, Error = failure::Error> + Send>;

#[derive(Debug)]
pub enum ErrorKind {
    Standard,
    Extended,
    Fatal,
}

/// A state machine that drives the pgwire backend.
///
/// Much of the state machine boilerplate is generated automatically with the
/// help of the [`state_machine_future`] package. There is a bit too much magic
/// in this approach for my taste, but attempting to write a futures-driven
/// server without it is unbelievably painful. Consider revisiting once
/// async/await support lands in stable.
#[derive(Smf)]
#[state_machine_future(context = "Context")]
pub enum StateMachine<A: Conn + 'static> {
    // Startup flow
    #[state_machine_future(start, transitions(RecvStartup))]
    Start { stream: A, session: Session },

    #[state_machine_future(transitions(SendAuthenticationOk, SendError, Done, Error))]
    RecvStartup { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(SendReadyForQuery, SendError, Error))]
    SendAuthenticationOk {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
    },

    // Regular query flow.
    #[state_machine_future(transitions(RecvQuery, SendError, Error))]
    SendReadyForQuery { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(
        HandleQuery,
        RecvQuery,
        SendError,
        SendParseComplete,
        SendReadyForQuery,
        SendParameterDescription,
        SendDescribeResponse,
        SendBindComplete,
        HandleParse,
        Error,
        Done
    ))]
    RecvQuery { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(
        SendCommandComplete,
        SendRowDescription,
        StartCopyOut,
        SendError,
        SendParameterStatus,
        WaitForRows,
        Error
    ))]
    HandleQuery {
        conn: A,
        rx: futures::sync::oneshot::Receiver<coord::Response<ExecuteResponse>>,
        field_formats: Option<Vec<FieldFormat>>,
        row_desc: Option<RelationDesc>,
        extended: bool,
    },

    #[state_machine_future(transitions(SendParseComplete, HandleQuery, SendError))]
    HandleParse {
        conn: A,
        rx: futures::sync::oneshot::Receiver<coord::Response<()>>,
        extended: bool,
    },

    // Extended query flow.
    #[state_machine_future(transitions(RecvQuery, Error))]
    SendParseComplete { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(RecvQuery, SendError, Error, Done))]
    SendBindComplete { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(SendDescribeResponse, SendError, Error))]
    SendParameterDescription {
        send: SinkSend<A>,
        session: Session,
        name: String,
    },

    #[state_machine_future(transitions(RecvQuery, Error))]
    SendDescribeResponse { send: SinkSend<A>, session: Session },

    // Response flows
    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, Error))]
    SendRowDescription {
        send: SinkSend<A>,
        session: Session,
        row_desc: RelationDesc,
        rows_rx: coord::RowsFuture,
    },

    /// Wait for the dataflow layer to send us rows
    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, SendError, Error))]
    WaitForRows {
        conn: A,
        session: Session,
        row_desc: RelationDesc,
        rows_rx: coord::RowsFuture,
        field_formats: Option<Vec<FieldFormat>>,
        extended: bool,
    },

    #[state_machine_future(transitions(
        WaitForUpdates,
        SendUpdates,
        SendError,
        SendReadyForQuery,
        Error
    ))]
    WaitForUpdates {
        conn: A,
        session: Session,
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },

    #[state_machine_future(transitions(WaitForUpdates, Error))]
    StartCopyOut {
        send: SinkSend<A>,
        session: Session,
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },

    #[state_machine_future(transitions(SendError, Error, WaitForUpdates))]
    SendUpdates {
        send: Box<dyn Future<Item = (MessageStream, A), Error = failure::Error> + Send>,
        session: Session,
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },

    #[state_machine_future(transitions(SendCommandComplete))]
    SendParameterStatus {
        send: SinkSend<A>,
        session: Session,
        extended: bool,
    },

    #[state_machine_future(transitions(SendReadyForQuery, RecvQuery, Error))]
    SendCommandComplete {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
        extended: bool,
        /// Labels for prometheus. These provide the `kind` label value in [`RESPONSES_SENT_COUNTER`]
        label: &'static str,
    },

    #[state_machine_future(transitions(SendReadyForQuery, DrainUntilSync))]
    DrainUntilSync { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(DrainUntilSync, SendReadyForQuery, Done, Error))]
    SendError {
        send: SinkSend<A>,
        session: Session,
        kind: ErrorKind,
    },

    #[state_machine_future(ready)]
    Done(()),

    #[state_machine_future(error)]
    Error(failure::Error),
}

fn format_update(update: Update) -> BackendMessage {
    let mut buf: Vec<u8> = Vec::new();
    let format_result: csv::Result<()> = {
        let mut wtr = csv::WriterBuilder::new()
            .terminator(csv::Terminator::Any(b' '))
            .from_writer(&mut buf);
        wtr.serialize(&update.row)
            .and_then(|_| wtr.flush().map_err(|err| From::from(err)))
    };
    BackendMessage::CopyData(match format_result {
        Ok(()) => match writeln!(&mut buf, " (Diff: {} at {})", update.diff, update.timestamp) {
            Ok(_) => buf,
            Err(e) => e.to_string().into_bytes(),
        },
        Err(e) => e.to_string().into_bytes(),
    })
}

impl<A: Conn> PollStateMachine<A> for StateMachine<A> {
    fn poll_start<'s, 'c>(
        state: &'s mut RentToOwn<'s, Start<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStart<A>, failure::Error> {
        trace!("cid={} start", cx.conn_id);
        let state = state.take();
        transition!(RecvStartup {
            recv: state.stream.recv(),
            session: state.session,
        })
    }

    fn poll_recv_startup<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvStartup<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvStartup<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("cid={} recv startup: {:?}", cx.conn_id, msg);
        let state = state.take();
        let version = match msg {
            FrontendMessage::Startup { version } => version,
            FrontendMessage::CancelRequest {
                conn_id,
                secret_key,
            } => {
                if cx.conn_secrets.verify(conn_id, secret_key) {
                    cx.cmdq_tx
                        .unbounded_send(coord::Command::CancelRequest { conn_id })?;
                }
                // For security, the client is not told whether the cancel
                // request succeeds or fails.
                transition!(Done(()))
            }

            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow at startup".into(),
                    detail: None,
                }),
                session: state.session,
                kind: ErrorKind::Fatal,
            }),
        };

        if version != VERSION_3 {
            transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08004",
                    message: "server does not support SSL".into(),
                    detail: None,
                }),
                session: state.session,
                kind: ErrorKind::Fatal,
            })
        }

        let messages: Vec<_> = iter::once(BackendMessage::AuthenticationOk)
            .chain(
                state
                    .session
                    .notify_vars()
                    .iter()
                    .map(|v| BackendMessage::ParameterStatus(v.name(), v.value())),
            )
            .chain(iter::once(BackendMessage::BackendKeyData {
                conn_id: cx.conn_id,
                secret_key: cx.conn_secrets.get(cx.conn_id).unwrap(),
            }))
            .collect();

        transition!(SendAuthenticationOk {
            send: Box::new(
                stream::iter_ok(messages)
                    .forward(conn)
                    .map(|(_, conn)| conn)
            ),
            session: state.session,
        })
    }

    fn poll_send_authentication_ok<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendAuthenticationOk<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendAuthenticationOk<A>, failure::Error> {
        trace!("cid={} auth ok", cx.conn_id);
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(SendReadyForQuery {
            send: conn.send(BackendMessage::ReadyForQuery(
                state.session.transaction().into()
            )),
            session: state.session,
        })
    }

    fn poll_send_ready_for_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendReadyForQuery<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendReadyForQuery<A>, failure::Error> {
        trace!("cid={} send ready for query", cx.conn_id);
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(RecvQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_start_copy_out<'s, 'c>(
        state: &'s mut RentToOwn<'s, StartCopyOut<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStartCopyOut<A>, failure::Error> {
        trace!("cid={} starting copy out", cx.conn_id);
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(WaitForUpdates {
            conn,
            session: state.session,
            rx: state.rx,
        })
    }

    fn poll_recv_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvQuery<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvQuery<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("cid={} recv query: {:?}", cx.conn_id, msg);
        let state = state.take();
        match msg {
            FrontendMessage::Query { sql } => {
                debug!("query sql: {}", sql);
                let (tx, rx) = futures::sync::oneshot::channel();
                cx.cmdq_tx.unbounded_send(coord::Command::Parse {
                    name: "".into(),
                    sql,
                    session: state.session,
                    tx,
                })?;
                transition!(HandleParse {
                    conn,
                    rx,
                    extended: false,
                })
            }
            FrontendMessage::Parse { name, sql, .. } => {
                debug!("parse sql: {}", sql);
                let (tx, rx) = futures::sync::oneshot::channel();
                cx.cmdq_tx.unbounded_send(coord::Command::Parse {
                    name,
                    sql,
                    session: state.session,
                    tx,
                })?;
                transition!(HandleParse {
                    conn,
                    rx,
                    extended: true
                })
            }
            FrontendMessage::CloseStatement { name } => {
                let mut session = state.session;
                session.remove_prepared_statement(&name);
                transition!(RecvQuery {
                    recv: conn.recv(),
                    session,
                });
            }
            FrontendMessage::ClosePortal { name: _ } => {
                transition!(RecvQuery {
                    recv: conn.recv(),
                    session: state.session,
                });
            }
            FrontendMessage::DescribePortal { name } => Ok(Async::Ready(send_describe_response(
                conn,
                state.session,
                name,
                DescribeKind::Portal,
                cx.conn_id,
            ))),
            FrontendMessage::DescribeStatement { name } => {
                match state.session.get_prepared_statement(&name) {
                    Some(stmt) => {
                        transition!(SendParameterDescription {
                            send: conn.send(BackendMessage::ParameterDescription(
                                stmt.param_types()
                                    .iter()
                                    .map(ParameterDescription::from)
                                    .collect(),
                            )),
                            session: state.session,
                            name,
                        });
                    }
                    None => transition!(SendError {
                        send: conn.send(BackendMessage::ErrorResponse {
                            severity: Severity::Fatal,
                            code: "08P01",
                            message: "prepared statement does not exist".into(),
                            detail: Some(format!("name: {}", name)),
                        }),
                        session: state.session,
                        kind: ErrorKind::Fatal,
                    }),
                }
            }
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                raw_parameter_bytes,
                return_field_formats,
            } => {
                trace!(
                    "cid={} handle bind statement={:?} portal={:?}, raw_parameter_bytes={:?}, return_field_formats={:?}",
                    cx.conn_id,
                    statement_name,
                    portal_name,
                    raw_parameter_bytes,
                    return_field_formats
                );

                let mut session = state.session;
                let fmts = return_field_formats.iter().map(bool::from).collect();
                let stmt = session.get_prepared_statement(&statement_name).unwrap();
                let param_types = stmt.param_types();
                match raw_parameter_bytes.decode_parameters(param_types) {
                    Ok(row) => {
                        session.set_portal(portal_name, statement_name, Some(row), fmts)?;
                        transition!(SendBindComplete {
                            send: conn.send(BackendMessage::BindComplete),
                            session,
                        });
                    }
                    Err(e) => transition!(SendError {
                        send: conn.send(BackendMessage::ErrorResponse {
                            severity: Severity::Fatal,
                            code: "08P01",
                            message: e.to_string(),
                            detail: None,
                        }),
                        session: session,
                        kind: ErrorKind::Fatal,
                    }),
                }
            }
            FrontendMessage::Execute { portal_name } => {
                let (tx, rx) = futures::sync::oneshot::channel();
                let portal = state
                    .session
                    .get_portal(&portal_name)
                    .ok_or_else(|| format_err!("portal {:?} does not exist", portal_name))?;
                let stmt = state
                    .session
                    .get_prepared_statement(&portal.statement_name)
                    .unwrap();
                let row_desc = stmt.desc().cloned();
                let field_formats = portal
                    .return_field_formats
                    .iter()
                    .map(FieldFormat::from)
                    .collect();
                cx.cmdq_tx.unbounded_send(coord::Command::Execute {
                    portal_name,
                    session: state.session,
                    conn_id: cx.conn_id,
                    tx,
                })?;
                transition!(HandleQuery {
                    rx,
                    conn,
                    row_desc,
                    field_formats: Some(field_formats),
                    extended: true,
                })
            }
            FrontendMessage::Sync => transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery(
                    state.session.transaction().into()
                )),
                session: state.session,
            }),
            FrontendMessage::Terminate => transition!(Done(())),
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow".into(),
                    detail: None,
                }),
                session: state.session,
                kind: ErrorKind::Fatal,
            }),
        }
    }

    fn poll_handle_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleQuery<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleQuery<A>, failure::Error> {
        match state.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(coord::Response {
                result: Ok(response),
                session,
            })) => {
                trace!("cid={} poll handle query", cx.conn_id);
                let state = state.take();

                macro_rules! command_complete {
                    ($cmd:expr, $label:tt) => {
                        transition!(SendCommandComplete {
                            send: Box::new(
                                state
                                    .conn
                                    .send(BackendMessage::CommandComplete { tag: $cmd.into() })
                            ),
                            session,
                            extended: state.extended,
                            label: $label
                        })
                    };
                }

                match response {
                    ExecuteResponse::CreatedIndex => {
                        command_complete!("CREATE INDEX", "create_index")
                    }
                    ExecuteResponse::CreatedSource => {
                        command_complete!("CREATE SOURCE", "create_source")
                    }
                    ExecuteResponse::CreatedSink => command_complete!("CREATE SINK", "create_sink"),
                    ExecuteResponse::CreatedTable => {
                        command_complete!("CREATE TABLE", "create_sink")
                    }
                    ExecuteResponse::CreatedView => command_complete!("CREATE VIEW", "create_view"),
                    ExecuteResponse::Deleted(n) => {
                        command_complete!(format!("DELETE {}", n), "delete");
                    }
                    ExecuteResponse::DroppedSource => {
                        command_complete!("DROP SOURCE", "drop_source")
                    }
                    ExecuteResponse::DroppedIndex => command_complete!("DROP INDEX", "drop_index")
                    ExecuteResponse::DroppedTable => command_complete!("DROP TABLE", "drop_table"),
                    ExecuteResponse::DroppedView => command_complete!("DROP VIEW", "drop_view"),
                    ExecuteResponse::EmptyQuery => transition!(SendCommandComplete {
                        send: Box::new(state.conn.send(BackendMessage::EmptyQueryResponse)),
                        session,
                        extended: state.extended,
                        label: "empty",
                    }),
                    ExecuteResponse::Inserted(n) => {
                        // "On successful completion, an INSERT command returns a
                        // command tag of the form `INSERT <oid> <count>`."
                        //     -- https://www.postgresql.org/docs/11/sql-insert.html
                        //
                        // OIDs are a PostgreSQL-specific historical quirk, but we
                        // can return a 0 OID to indicate that the table does not
                        // have OIDs.
                        command_complete!(format!("INSERT 0 {}", n), "insert");
                    }
                    ExecuteResponse::SendRows(rx) => {
                        let row_desc = state
                            .row_desc
                            .expect("missing row description during ExecuteResponse::SendRows");
                        if state.extended {
                            trace!("cid={} handle extended: send rows", cx.conn_id);
                            transition!(WaitForRows {
                                session,
                                conn: state.conn,
                                row_desc,
                                rows_rx: rx,
                                field_formats: state.field_formats,
                                extended: true
                            })
                        } else {
                            transition!(SendRowDescription {
                                send: state.conn.send(BackendMessage::RowDescription(
                                    super::message::row_description_from_desc(&row_desc)
                                )),
                                session,
                                row_desc,
                                rows_rx: rx,
                            })
                        }
                    }
                    ExecuteResponse::SetVariable { name } => {
                        if let Some(var) = session.notify_vars().iter().find(|v| v.name() == name) {
                            trace!("cid={} sending parameter status for {}", cx.conn_id, name);
                            transition!(SendParameterStatus {
                                send: state
                                    .conn
                                    .send(BackendMessage::ParameterStatus(var.name(), var.value())),
                                session,
                                extended: state.extended,
                            })
                        } else {
                            command_complete!("SET", "set")
                        }
                    }
                    ExecuteResponse::StartTransaction => {
                        command_complete!("START TRANSACTION", "transaction_start")
                    }
                    ExecuteResponse::Commit => {
                        command_complete!("COMMIT TRANSACTION", "transaction_commit")
                    }
                    ExecuteResponse::Rollback => {
                        command_complete!("ROLLBACK TRANSACTION", "transaction_rollback")
                    }
                    ExecuteResponse::Tailing { rx } => transition!(StartCopyOut {
                        send: state.conn.send(BackendMessage::CopyOutResponse),
                        session,
                        rx,
                    }),
                    ExecuteResponse::Updated(n) => {
                        command_complete!(format!("UPDATE {}", n), "update");
                    }
                }
            }
            Ok(Async::Ready(coord::Response {
                result: Err(err),
                session,
            })) => {
                let state = state.take();
                transition!(SendError {
                    send: state.conn.send(BackendMessage::ErrorResponse {
                        severity: Severity::Error,
                        code: "99999",
                        message: err.to_string(),
                        detail: None,
                    }),
                    session,
                    kind: if state.extended {
                        ErrorKind::Extended
                    } else {
                        ErrorKind::Standard
                    },
                });
            }
            Err(futures::sync::oneshot::Canceled) => {
                panic!("Connection to sql planner closed unexpectedly")
            }
        }
    }

    fn poll_send_parameter_description<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendParameterDescription<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendParameterDescription<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        Ok(Async::Ready(send_describe_response(
            conn,
            state.session,
            state.name,
            DescribeKind::Statement,
            cx.conn_id,
        )))
    }

    fn poll_send_describe_response<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendDescribeResponse<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendDescribeResponse<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("cid={} sent extended row description", cx.conn_id);
        transition!(RecvQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_send_row_description<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendRowDescription<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendRowDescription<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("cid={} send row description", cx.conn_id);
        transition!(WaitForRows {
            conn,
            session: state.session,
            row_desc: state.row_desc,
            rows_rx: state.rows_rx,
            field_formats: None,
            extended: false
        })
    }

    fn poll_wait_for_updates<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForUpdates<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForUpdates<A>, failure::Error> {
        trace!("cid={} wait for updates", cx.conn_id);
        match state.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(Some(results))) => {
                let state = state.take();
                let stream: MessageStream = Box::new(futures::stream::iter_ok(
                    results.into_iter().map(format_update),
                ));
                transition!(SendUpdates {
                    send: Box::new(stream.forward(state.conn)),
                    session: state.session,
                    rx: state.rx,
                })
            }
            Ok(Async::Ready(None)) => {
                trace!("cid={} update stream finished", cx.conn_id);
                let state = state.take();
                transition!(SendReadyForQuery {
                    send: state.conn.send(BackendMessage::ReadyForQuery(
                        state.session.transaction().into()
                    )),
                    session: state.session,
                })
            }
            Err(err) => panic!("error receiving tail results: {}", err),
        }
    }

    fn poll_wait_for_rows<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForRows<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForRows<A>, failure::Error> {
        match try_ready!(state.rows_rx.poll()) {
            PeekResponse::Canceled => {
                let state = state.take();
                transition!(SendError {
                    send: state.conn.send(BackendMessage::ErrorResponse {
                        severity: Severity::Error,
                        code: "57014",
                        message: "canceling statement due to user request".into(),
                        detail: None,
                    }),
                    session: state.session,
                    kind: ErrorKind::Standard,
                });
            }
            PeekResponse::Rows(rows) => {
                let state = state.take();
                let extended = state.extended;
                trace!(
                    "cid={} wait for rows: count={} extended={}",
                    cx.conn_id,
                    rows.len(),
                    extended
                );
                transition!(send_rows(
                    state.conn,
                    state.session,
                    rows,
                    state.row_desc,
                    state.field_formats.clone(),
                    state.extended,
                    cx.conn_id,
                ));
            }
        }
    }

    fn poll_send_updates<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendUpdates<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendUpdates<A>, failure::Error> {
        let (_, conn) = try_ready!(state.send.poll());
        let state = state.take();
        transition!(WaitForUpdates {
            conn: conn,
            session: state.session,
            rx: state.rx,
        })
    }

    fn poll_send_command_complete<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendCommandComplete<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendCommandComplete<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        let extended = state.extended;
        let in_transaction = state.session.transaction();
        trace!(
            "cid={} send command complete extended={} transaction={:?}",
            cx.conn_id,
            extended,
            in_transaction,
        );
        if cx.gather_metrics {
            RESPONSES_SENT_COUNTER
                .with_label_values(&["success", state.label])
                .inc();
        }
        if extended {
            transition!(RecvQuery {
                recv: conn.recv(),
                session: state.session,
            })
        } else {
            transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery(
                    state.session.transaction().into()
                )),
                session: state.session,
            })
        }
    }

    fn poll_handle_parse<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleParse<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleParse<A>, failure::Error> {
        match try_ready!(state.rx.poll()) {
            coord::Response {
                result: Ok(()),
                mut session,
            } => {
                let state = state.take();
                if state.extended {
                    transition!(SendParseComplete {
                        send: state.conn.send(BackendMessage::ParseComplete),
                        session,
                    })
                } else {
                    let statement_name = String::from("");
                    let stmt = session
                        .get_prepared_statement(&statement_name)
                        .expect("unnamed statement to be present during simple query flow");
                    let row_desc = stmt.desc().cloned();
                    let portal_name = String::from("");
                    let fmts = vec![];
                    session
                        .set_portal(portal_name.clone(), statement_name, None, fmts)
                        .expect("unnamed statement to be present during simple query flow");
                    let (tx, rx) = futures::sync::oneshot::channel();
                    cx.cmdq_tx.unbounded_send(coord::Command::Execute {
                        portal_name,
                        session,
                        conn_id: cx.conn_id,
                        tx,
                    })?;
                    transition!(HandleQuery {
                        conn: state.conn,
                        rx,
                        field_formats: None,
                        row_desc,
                        extended: false,
                    })
                }
            }
            coord::Response {
                result: Err(err),
                session,
            } => {
                let state = state.take();
                transition!(SendError {
                    send: state.conn.send(BackendMessage::ErrorResponse {
                        severity: Severity::Error,
                        code: "99999",
                        message: err.to_string(),
                        detail: None,
                    }),
                    session,
                    kind: if state.extended {
                        ErrorKind::Extended
                    } else {
                        ErrorKind::Standard
                    },
                });
            }
        }
    }

    fn poll_send_parse_complete<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendParseComplete<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendParseComplete<A>, failure::Error> {
        trace!("cid={} send parse complete", cx.conn_id);
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("cid={} transition to recv extended", cx.conn_id);
        transition!(RecvQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_send_bind_complete<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendBindComplete<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendBindComplete<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(RecvQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_drain_until_sync<'s, 'c>(
        state: &'s mut RentToOwn<'s, DrainUntilSync<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterDrainUntilSync<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("cid={} drain until sync msg={:?}", cx.conn_id, msg);
        match msg {
            FrontendMessage::Sync => {
                let state = state.take();
                transition!(SendReadyForQuery {
                    send: conn.send(BackendMessage::ReadyForQuery(
                        state.session.transaction().into()
                    )),
                    session: state.session,
                })
            }
            _ => {
                let state = state.take();
                transition!(DrainUntilSync {
                    recv: conn.recv(),
                    session: state.session,
                })
            }
        }
    }

    fn poll_send_parameter_status<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendParameterStatus<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendParameterStatus<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(SendCommandComplete {
            send: Box::new(conn.send(BackendMessage::CommandComplete { tag: "SET".into() })),
            session: state.session,
            extended: state.extended,
            label: "set",
        })
    }

    fn poll_send_error<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendError<A>>,
        cx: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendError<A>, failure::Error> {
        trace!("cid={} send error kind={:?}", cx.conn_id, state.kind);
        let conn = try_ready!(state.send.poll());
        let mut state = state.take();
        state.session.fail_transaction();
        if cx.gather_metrics {
            RESPONSES_SENT_COUNTER
                .with_label_values(&["error", ""])
                .inc();
        }
        match state.kind {
            ErrorKind::Standard => transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery(
                    state.session.transaction().into()
                )),
                session: state.session,
            }),
            ErrorKind::Extended => transition!(DrainUntilSync {
                recv: conn.recv(),
                session: state.session,
            }),
            ErrorKind::Fatal => transition!(Done(())),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn send_rows<A>(
    conn: A,
    session: Session,
    rows: Vec<Row>,
    row_desc: RelationDesc,
    field_formats: Option<Vec<FieldFormat>>,
    extended: bool,
    conn_id: u32,
) -> SendCommandComplete<A>
where
    A: Conn + 'static,
{
    trace!("cid={} send rows extended={}", conn_id, extended);
    let formats = FieldFormatIter::new(field_formats.map(Arc::new));

    let rows = rows
        .into_iter()
        .map(move |row| {
            BackendMessage::DataRow(
                message::field_values_from_row(row, row_desc.typ()),
                formats.fresh(),
            )
        })
        .chain(iter::once(BackendMessage::CommandComplete {
            tag: "SELECT".into(),
        }));

    SendCommandComplete {
        send: Box::new(stream::iter_ok(rows).forward(conn).map(|(_, conn)| conn)),
        session,
        extended,
        label: "select",
    }
}

#[derive(Clone, Copy, Debug)]
enum DescribeKind {
    Statement,
    Portal,
}

fn send_describe_response<A, R>(
    conn: A,
    session: Session,
    name: String,
    kind: DescribeKind,
    conn_id: u32,
) -> R
where
    A: Conn + 'static,
    R: From<SendError<A>> + From<SendDescribeResponse<A>>,
{
    trace!(
        "cid={} send describe response statement_name={:?}",
        conn_id,
        name
    );
    let stmt = match kind {
        DescribeKind::Statement => session.get_prepared_statement(&name),
        DescribeKind::Portal => session
            .get_portal(&name)
            .and_then(|portal| session.get_prepared_statement(&portal.statement_name)),
    };
    let stmt = match stmt {
        Some(stmt) => stmt,
        None => {
            return SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "portal or prepared statement does not exist".into(),
                    detail: Some(format!("name: {}", name)),
                }),
                session,
                kind: ErrorKind::Fatal,
            }
            .into();
        }
    };
    match stmt.desc() {
        Some(desc) => {
            let desc = super::message::row_description_from_desc(&desc);
            trace!("cid={} sending row description {:?}", conn_id, desc);
            SendDescribeResponse {
                send: conn.send(BackendMessage::RowDescription(desc)),
                session,
            }
            .into()
        }
        None => {
            trace!("cid={} sending no data", conn_id);
            SendDescribeResponse {
                send: conn.send(BackendMessage::NoData),
                session,
            }
            .into()
        }
    }
}
