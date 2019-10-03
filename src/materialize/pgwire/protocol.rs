// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::io::Write;
use std::iter;
use std::sync::Arc;

use byteorder::{ByteOrder, NetworkEndian};
use futures::sink::Send as SinkSend;
use futures::stream;
use futures::sync::mpsc::UnboundedSender;
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use lazy_static::lazy_static;
use log::trace;
use state_machine_future::StateMachineFuture as Smf;
use state_machine_future::{transition, RentToOwn};
use tokio::codec::Framed;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::pgwire::codec::Codec;
use crate::pgwire::message::{
    self, BackendMessage, FieldFormat, FieldFormatIter, FrontendMessage, Severity,
};
use coord::{self, SqlResponse};
use dataflow_types::Update;
use ore::future::{Recv, StreamExt};
use repr::{Datum, RelationType};
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
    pub cmdq_tx: UnboundedSender<coord::Command>,
    /// If true, we gather prometheus metrics
    pub gather_metrics: bool,
}

// Pgwire protocol versions are represented as 32-bit integers, where the
// high 16 bits represent the major version and the low 16 bits represent the
// minor version.
//
// There have only been three released protocol versions, v1.0, v2.0, and v3.0.
// The protocol changes very infrequently: the most recent protocol version,
// v3.0, was released with Postgres v7.4 in 2003.
//
// Somewhat unfortunately, the protocol overloads the version field to indicate
// special types of connections, namely, SSL connections and cancellation
// connections. These pseudo-versions were constructed to avoid ever matching
// a true protocol version.

const VERSION_1: u32 = 0x10000;
const VERSION_2: u32 = 0x20000;
const VERSION_3: u32 = 0x30000;
const VERSION_SSL: u32 = (1234 << 16) + 5678;
const VERSION_CANCEL: u32 = (1234 << 16) + 5679;

const VERSIONS: &[u32] = &[VERSION_1, VERSION_2, VERSION_3, VERSION_SSL, VERSION_CANCEL];

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
    let version = NetworkEndian::read_u32(&buf[4..8]);
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

    #[state_machine_future(transitions(SendAuthenticationOk, SendError, Error))]
    RecvStartup { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(SendReadyForQuery, SendError, Error))]
    SendAuthenticationOk {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
    },

    // Regular query flow
    #[state_machine_future(transitions(RecvQuery, SendError, Error))]
    SendReadyForQuery { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(
        HandleQuery,
        HandleExtendedQuery,
        SendError,
        SendParseComplete,
        SendReadyForQuery,
        SendDescribeResponse,
        HandleBind,
        Error,
        Done
    ))]
    RecvQuery { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(
        SendCommandComplete,
        SendRowDescription,
        StartCopyOut,
        SendError,
        SendParseComplete,
        Error
    ))]
    HandleQuery {
        conn: A,
        rx: futures::sync::oneshot::Receiver<coord::Response>,
    },

    // Extended query flow
    /// We enter the extended query by way of PARSE, then we spin in it until we exit
    #[state_machine_future(transitions(SendParseComplete, WaitForRows, SendError, Error, Done))]
    HandleExtendedQuery {
        conn: A,
        rx: futures::sync::oneshot::Receiver<coord::Response>,
        field_formats: Option<Vec<FieldFormat>>,
    },

    #[state_machine_future(transitions(
        HandleExtendedQuery,
        SendReadyForQuery,
        HandleBind,
        SendDescribeResponse,
        SendError,
        Error,
        Done
    ))]
    RecvExtendedQuery { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(RecvExtendedQuery, Error))]
    SendParseComplete { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(SendBindComplete, SendError, Error, Done))]
    HandleBind {
        send: SinkSend<A>,
        session: Session,
        portal_name: String,
        statement_name: String,
        return_field_formats: Vec<FieldFormat>,
    },

    #[state_machine_future(transitions(RecvQuery, SendError, Error, Done))]
    SendBindComplete { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(SendDescribeResponseRowdesc, Error))]
    SendDescribeResponse {
        send: SinkSend<A>,
        session: Session,
        name: String,
        is_statement: bool, // true if a statement, false if a portal.
    },

    #[state_machine_future(transitions(RecvExtendedQuery, Error))]
    SendDescribeResponseRowdesc { send: SinkSend<A>, session: Session },

    // Response flows
    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, Error))]
    SendRowDescription {
        send: SinkSend<A>,
        session: Session,
        row_type: RelationType,
        rows_rx: coord::RowsFuture,
    },

    /// Wait for the dataflow layer to send us rows
    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, SendError, Error))]
    WaitForRows {
        conn: A,
        session: Session,
        row_type: RelationType,
        rows_rx: coord::RowsFuture,
        field_formats: Option<Vec<FieldFormat>>,
        currently_extended: bool,
    },

    #[state_machine_future(transitions(WaitForUpdates, SendUpdates, SendError, Error))]
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

    #[state_machine_future(transitions(SendReadyForQuery, RecvExtendedQuery, Error))]
    SendCommandComplete {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
        currently_extended: bool,
        /// Labels for prometheus. These provide the `kind` label value in [`RESPONSES_SENT_COUNTER`]
        label: &'static str,
    },

    #[state_machine_future(transitions(SendReadyForQuery, Done, Error))]
    SendError {
        send: SinkSend<A>,
        session: Session,
        fatal: bool,
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
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStart<A>, failure::Error> {
        trace!("start");
        let state = state.take();
        transition!(RecvStartup {
            recv: state.stream.recv(),
            session: state.session,
        })
    }

    fn poll_recv_startup<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvStartup<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvStartup<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("recv startup: {:?}", msg);
        let state = state.take();
        let version = match msg {
            FrontendMessage::Startup { version } => version,
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow at startup".into(),
                    detail: None,
                }),
                session: state.session,
                fatal: true,
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
                fatal: true,
            })
        }

        let messages: Vec<_> = iter::once(BackendMessage::AuthenticationOk)
            .chain(
                state
                    .session
                    .startup_vars()
                    .iter()
                    .map(|v| BackendMessage::ParameterStatus(v.name(), v.value())),
            )
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
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendAuthenticationOk<A>, failure::Error> {
        trace!("auth ok");
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(SendReadyForQuery {
            send: conn.send(BackendMessage::ReadyForQuery),
            session: state.session,
        })
    }

    fn poll_send_ready_for_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendReadyForQuery<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendReadyForQuery<A>, failure::Error> {
        trace!("send ready for query");
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(RecvQuery {
            recv: conn.recv(),
            session: state.session
        })
    }

    fn poll_start_copy_out<'s, 'c>(
        state: &'s mut RentToOwn<'s, StartCopyOut<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStartCopyOut<A>, failure::Error> {
        trace!("starting copy out");
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
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvQuery<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("recv query: {:?}", msg);
        let state = state.take();
        match msg {
            FrontendMessage::Query { sql } => {
                let (tx, rx) = futures::sync::oneshot::channel();
                context.cmdq_tx.unbounded_send(coord::Command {
                    kind: coord::CommandKind::Query { sql },
                    session: state.session,
                    conn_id: context.conn_id,
                    tx,
                })?;
                transition!(HandleQuery { conn, rx })
            }

            // extended flow commands

            // Several flows are identical between extended and regular queries, instead
            // of threading `currently_extended` through almost every state, or
            // duplicating a bunch of functions and SMF enum variants we just handle them
            // here
            FrontendMessage::Parse { name, sql, .. } => {
                let (tx, rx) = futures::sync::oneshot::channel();
                context.cmdq_tx.unbounded_send(coord::Command {
                    kind: coord::CommandKind::Parse { name, sql },
                    session: state.session,
                    conn_id: context.conn_id,
                    tx,
                })?;
                transition!(HandleExtendedQuery {
                    conn,
                    rx,
                    field_formats: None
                })
            }
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                return_field_formats,
            } => transition!(HandleBind {
                send: conn.send(BackendMessage::BindComplete),
                session: state.session,
                portal_name,
                statement_name,
                return_field_formats,
            }),
            FrontendMessage::DescribePortal { name } => transition!(SendDescribeResponse {
                send: conn.send(BackendMessage::ParameterDescription),
                session: state.session,
                name,
                is_statement: false,
            }),
            FrontendMessage::Execute { portal_name } => {
                let (tx, rx) = futures::sync::oneshot::channel();
                let field_formats = state
                    .session
                    .get_portal(&portal_name)
                    .ok_or_else(|| failure::format_err!("portal {:?} does not exist", portal_name))?
                    .return_field_formats
                    .iter()
                    .map(FieldFormat::from)
                    .collect();
                context.cmdq_tx.unbounded_send(coord::Command {
                    kind: coord::CommandKind::Execute { portal_name },
                    session: state.session,
                    conn_id: context.conn_id,
                    tx,
                })?;
                transition!(HandleExtendedQuery {
                    rx,
                    conn,
                    field_formats: Some(field_formats)
                })
            }
            FrontendMessage::Sync => {
                trace!("frontend message flow: sync");
                transition!(SendReadyForQuery {
                    send: conn.send(BackendMessage::ReadyForQuery),
                    session: state.session,
                })
            }

            // End the flow
            FrontendMessage::Terminate => transition!(Done(())),
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow".into(),
                    detail: None,
                }),
                session: state.session,
                fatal: true,
            }),
        };
    }

    fn poll_handle_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleQuery<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleQuery<A>, failure::Error> {
        match state.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(coord::Response {
                sql_result: Ok(response),
                session,
            })) => {
                trace!("poll handle query");
                let state = state.take();

                macro_rules! command_complete {
                    ($cmd:tt, $label:tt) => {
                        transition!(SendCommandComplete {
                            send: Box::new(
                                state
                                    .conn
                                    .send(BackendMessage::CommandComplete { tag: $cmd.into() })
                            ),
                            session,
                            currently_extended: false,
                            label: $label
                        })
                    };
                }

                match response {
                    SqlResponse::CreatedSource => {
                        command_complete!("CREATE SOURCE", "create_source")
                    }
                    SqlResponse::CreatedSink => command_complete!("CREATE SINK", "create_sink"),
                    SqlResponse::CreatedView => command_complete!("CREATE VIEW", "create_view"),
                    SqlResponse::DroppedSource => command_complete!("DROP SOURCE", "drop_source"),
                    SqlResponse::DroppedView => command_complete!("DROP VIEW", "drop_view"),
                    SqlResponse::EmptyQuery => transition!(SendCommandComplete {
                        send: Box::new(state.conn.send(BackendMessage::EmptyQueryResponse)),
                        session,
                        currently_extended: false,
                        label: "empty",
                    }),
                    SqlResponse::SendRows { typ, rx } => transition!(SendRowDescription {
                        send: state.conn.send(BackendMessage::RowDescription(
                            super::message::row_description_from_type(&typ)
                        )),
                        session,
                        row_type: typ,
                        rows_rx: rx,
                    }),
                    SqlResponse::SetVariable => command_complete!("SET", "set"),
                    SqlResponse::Tailing { rx } => transition!(StartCopyOut {
                        send: state.conn.send(BackendMessage::CopyOutResponse),
                        session,
                        rx,
                    }),
                    SqlResponse::Parsed { .. } => panic!("got parse complete in regular query"),
                }
            }
            Ok(Async::Ready(coord::Response {
                sql_result: Err(err),
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
                    fatal: false,
                });
            }
            Err(futures::sync::oneshot::Canceled) => {
                panic!("Connection to sql planner closed unexpectedly")
            }
        }
    }

    fn poll_recv_extended_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvExtendedQuery<A>>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvExtendedQuery<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        trace!("recv query extended: {:?}", msg);
        let state = state.take();
        match msg {
            FrontendMessage::DescribeStatement { name } => transition!(SendDescribeResponse {
                send: conn.send(BackendMessage::ParameterDescription),
                session: state.session,
                name,
                is_statement: true,
            }),
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                return_field_formats,
            } => transition!(HandleBind {
                send: conn.send(BackendMessage::BindComplete),
                session: state.session,
                portal_name,
                statement_name,
                return_field_formats,
            }),
            FrontendMessage::Execute { portal_name } => {
                let (tx, rx) = futures::sync::oneshot::channel();
                let field_formats = state
                    .session
                    .get_portal(&portal_name)
                    .ok_or_else(|| failure::format_err!("portal {:?} does not exist", portal_name))?
                    .return_field_formats
                    .iter()
                    .map(FieldFormat::from)
                    .collect();
                context.cmdq_tx.unbounded_send(coord::Command {
                    kind: coord::CommandKind::Execute { portal_name },
                    session: state.session,
                    conn_id: context.conn_id,
                    tx,
                })?;
                transition!(HandleExtendedQuery {
                    rx,
                    conn,
                    field_formats: Some(field_formats)
                })
            }
            FrontendMessage::Sync => transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery),
                session: state.session,
            }),
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow for extended query".into(),
                    detail: None,
                }),
                session: state.session,
                fatal: true,
            }),
        };
    }

    fn poll_handle_extended_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleExtendedQuery<A>>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleExtendedQuery<A>, failure::Error> {
        match state.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(coord::Response {
                sql_result: Ok(response),
                session,
            })) => {
                let state = state.take();
                match response {
                    SqlResponse::Parsed { .. } => transition!(SendParseComplete {
                        send: state.conn.send(BackendMessage::ParseComplete),
                        session,
                    }),
                    SqlResponse::SendRows { typ, rx } => {
                        trace!("handle extended: send rows");
                        let ff = state.field_formats;
                        debug_assert!(ff.is_some(), "field formats must be set for execute");
                        transition!(WaitForRows {
                            session,
                            conn: state.conn,
                            row_type: typ,
                            rows_rx: rx,
                            field_formats: ff,
                            currently_extended: true
                        })
                    }
                    _ => panic!("extended query: got non extended command={:?}", response),
                }
            }
            Ok(Async::Ready(coord::Response {
                sql_result: Err(err),
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
                    fatal: false,
                });
            }
            Err(futures::sync::oneshot::Canceled) => {
                panic!("Connection to sql planner closed unexpectedly during extended")
            }
        }
    }

    fn poll_send_describe_response<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendDescribeResponse<A>>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendDescribeResponse<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("send describe response for statement={:?}", state.name);
        let statement = if state.is_statement {
            state.session.get_prepared_statement(&state.name)
        } else {
            let name = &state
                .session
                .get_portal(&state.name)
                .ok_or_else(|| failure::format_err!("Portal does not exist: {:?}", state.name))?
                .statement_name;
            state.session.get_prepared_statement(&name)
        };
        match statement {
            Some(ps) => {
                let typ = ps.source().typ();
                let desc = super::message::row_description_from_type(&typ);
                transition!(SendDescribeResponseRowdesc {
                    send: conn.send(BackendMessage::RowDescription(desc)),
                    session: state.session,
                })
            }
            None => failure::bail!("prepared statement does not exist name={:?}", state.name),
        }
    }

    fn poll_send_describe_response_rowdesc<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendDescribeResponseRowdesc<A>>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendDescribeResponseRowdesc<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("sent describe response rowdesc");
        transition!(RecvExtendedQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_send_row_description<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendRowDescription<A>>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendRowDescription<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("send row description");
        transition!(WaitForRows {
            conn,
            session: state.session,
            row_type: state.row_type,
            rows_rx: state.rows_rx,
            field_formats: None,
            currently_extended: false
        })
    }

    fn poll_wait_for_updates<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForUpdates<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForUpdates<A>, failure::Error> {
        trace!("wait for updates");
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
            Ok(Async::Ready(None)) => panic!("Connection to dataflow server closed unexpectedly"),
            Err(err) => panic!("error receiving tail results: {}", err),
        }
    }

    fn poll_wait_for_rows<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForRows<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForRows<A>, failure::Error> {
        let peek_results = try_ready!(state.rows_rx.poll());
        let state = state.take();
        let extended = state.currently_extended;
        trace!(
            "wait for rows: count={} extended={}",
            peek_results.len(),
            extended
        );
        transition!(send_rows(
            state.conn,
            state.session,
            peek_results,
            state.row_type,
            state.field_formats.clone(),
            state.currently_extended,
        ))
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
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendCommandComplete<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        let extended = state.currently_extended;
        trace!("send command complete extended={}", extended);
        if context.gather_metrics {
            RESPONSES_SENT_COUNTER
                .with_label_values(&["success", state.label])
                .inc();
        }
        if extended {
            transition!(RecvExtendedQuery {
                recv: conn.recv(),
                session: state.session,
            })
        } else {
            transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery),
                session: state.session,
            })
        }
    }

    fn poll_send_parse_complete<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendParseComplete<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendParseComplete<A>, failure::Error> {
        trace!("send parse complete");
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        trace!("transition to recv extended");
        transition!(RecvExtendedQuery {
            recv: conn.recv(),
            session: state.session,
        })
    }

    fn poll_handle_bind<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleBind<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleBind<A>, failure::Error> {
        let mut state = state.take();
        let (sn, pn) = (state.statement_name, state.portal_name);
        let fmts = state.return_field_formats.iter().map(bool::from).collect();
        trace!("handle bind statement={:?} portal={:?}", sn, pn);
        state.session.set_portal(pn, sn, fmts)?;

        transition!(SendBindComplete {
            send: state.send,
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

    fn poll_send_error<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendError<A>>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendError<A>, failure::Error> {
        trace!("send error");
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        if context.gather_metrics {
            RESPONSES_SENT_COUNTER
                .with_label_values(&["error", ""])
                .inc();
        }
        if state.fatal {
            transition!(Done(()))
        } else {
            transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery),
                session: state.session,
            })
        }
    }
}

fn send_rows<A, R>(
    conn: A,
    session: Session,
    rows: R,
    row_type: RelationType,
    field_formats: Option<Vec<FieldFormat>>,
    currently_extended: bool,
) -> SendCommandComplete<A>
where
    A: Conn + 'static,
    R: IntoIterator<Item = Vec<Datum>>,
    <R as IntoIterator>::IntoIter: 'static + Send,
{
    trace!("send rows currently_extended={}", currently_extended);
    let formats = FieldFormatIter::new(field_formats.map(Arc::new));

    let rows = rows
        .into_iter()
        .map(move |row| {
            BackendMessage::DataRow(
                message::field_values_from_row(row, &row_type),
                formats.fresh(),
            )
        })
        .chain(iter::once(BackendMessage::CommandComplete {
            tag: "SELECT".into(),
        }));

    SendCommandComplete {
        send: Box::new(stream::iter_ok(rows).forward(conn).map(|(_, conn)| conn)),
        session,
        currently_extended,
        label: "select",
    }
}
