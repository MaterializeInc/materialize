// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use byteorder::{ByteOrder, NetworkEndian};
use futures::sink::Send as SinkSend;
use futures::stream;
use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use state_machine_future::StateMachineFuture as Smf;
use state_machine_future::{transition, RentToOwn};
use std::io::Write;
use std::iter;
use tokio::codec::Framed;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::pgwire::codec::Codec;
use crate::pgwire::message;
use crate::pgwire::message::{BackendMessage, FrontendMessage, Severity};
use crate::queue;
use dataflow_types::{Exfiltration, Update};
use ore::future::{Recv, StreamExt};
use repr::{Datum, RelationType};
use sql::{Session, SqlResponse, WaitFor};

pub struct Context {
    pub conn_id: u32,
    pub cmdq_tx: UnboundedSender<queue::Command>,
    pub dataflow_results_receiver: UnboundedReceiver<Exfiltration>,
    pub num_timely_workers: usize,
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
    #[state_machine_future(start, transitions(RecvStartup))]
    Start { stream: A, session: Session },

    #[state_machine_future(transitions(SendAuthenticationOk, SendError, Error))]
    RecvStartup { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(SendReadyForQuery, SendError, Error))]
    SendAuthenticationOk {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
    },

    #[state_machine_future(transitions(RecvQuery, SendError, Error))]
    SendReadyForQuery { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(HandleQuery, SendError, Error, Done))]
    RecvQuery { recv: Recv<A>, session: Session },

    #[state_machine_future(transitions(
        SendCommandComplete,
        SendRowDescription,
        StartCopyOut,
        SendError,
        Error
    ))]
    HandleQuery {
        conn: A,
        rx: futures::sync::oneshot::Receiver<queue::Response>,
    },

    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, Error))]
    SendRowDescription {
        send: SinkSend<A>,
        session: Session,
        row_type: RelationType,
        /// To be sent immediately
        rows: Vec<Vec<Datum>>,
        wait_for: usize,
    },

    #[state_machine_future(transitions(WaitForRows, SendCommandComplete, SendError, Error))]
    WaitForRows {
        conn: A,
        session: Session,
        row_type: RelationType,
        peek_results: Vec<Vec<Datum>>,
        remaining_results: usize,
    },

    #[state_machine_future(transitions(WaitForUpdates, SendUpdates, SendError, Error))]
    WaitForUpdates { conn: A, session: Session },

    #[state_machine_future(transitions(WaitForUpdates, Error))]
    StartCopyOut { send: SinkSend<A>, session: Session },

    #[state_machine_future(transitions(SendError, Error, WaitForUpdates))]
    SendUpdates {
        send: Box<dyn Future<Item = (MessageStream, A), Error = failure::Error> + Send>,
        session: Session,
    },

    #[state_machine_future(transitions(SendReadyForQuery, Error))]
    SendCommandComplete {
        send: Box<dyn Future<Item = A, Error = io::Error> + Send>,
        session: Session,
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
        let state = state.take();
        let version = match msg {
            FrontendMessage::Startup { version } => version,
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
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(WaitForUpdates {
            conn,
            session: state.session
        })
    }

    fn poll_recv_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvQuery<A>>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRecvQuery<A>, failure::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        let state = state.take();
        match msg {
            FrontendMessage::Query { query } => {
                let sql = String::from(String::from_utf8_lossy(&query));
                let (tx, rx) = futures::sync::oneshot::channel();
                context.cmdq_tx.unbounded_send(queue::Command {
                    sql,
                    session: state.session,
                    conn_id: context.conn_id,
                    tx,
                })?;
                transition!(HandleQuery { conn, rx })
            }
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
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterHandleQuery<A>, failure::Error> {
        match state.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(queue::Response {
                sql_result: Ok(response),
                session,
            })) => {
                let state = state.take();

                macro_rules! command_complete {
                    ($($arg:tt)*) => {
                        transition!(SendCommandComplete {
                            send: Box::new(state.conn.send(BackendMessage::CommandComplete {
                                tag: std::fmt::format(format_args!($($arg)*))
                            })),
                            session,
                        })
                    };
                }

                match response {
                    SqlResponse::CreatedSource => command_complete!("CREATE SOURCE"),
                    SqlResponse::CreatedSink => command_complete!("CREATE SINK"),
                    SqlResponse::CreatedView => command_complete!("CREATE VIEW"),
                    SqlResponse::DroppedSource => command_complete!("DROP SOURCE"),
                    SqlResponse::DroppedView => command_complete!("DROP VIEW"),
                    SqlResponse::EmptyQuery => transition!(SendCommandComplete {
                        send: Box::new(state.conn.send(BackendMessage::EmptyQueryResponse)),
                        session,
                    }),
                    SqlResponse::SendRows {
                        typ,
                        rows,
                        wait_for,
                    } => transition!(SendRowDescription {
                        send: state.conn.send(BackendMessage::RowDescription(
                            super::message::row_description_from_type(&typ)
                        )),
                        session,
                        row_type: typ,
                        rows,
                        wait_for: match wait_for {
                            WaitFor::NoOne => 0,
                            WaitFor::Optimizer => 1,
                            WaitFor::Workers => context.num_timely_workers,
                        }
                    }),
                    SqlResponse::SetVariable => command_complete!("SET"),
                    SqlResponse::Tailing => transition!(StartCopyOut {
                        send: state.conn.send(BackendMessage::CopyOutResponse),
                        session,
                    }),
                }
            }
            Ok(Async::Ready(queue::Response {
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

    fn poll_send_row_description<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendRowDescription<A>>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendRowDescription<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(WaitForRows {
            conn,
            session: state.session,
            row_type: state.row_type,
            peek_results: state.rows,
            remaining_results: state.wait_for,
        })
    }

    fn poll_wait_for_updates<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForUpdates<A>>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForUpdates<A>, failure::Error> {
        match context.dataflow_results_receiver.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(Some(results))) => {
                let state = state.take();
                let results = if let Exfiltration::Tail(results) = results {
                    results
                } else {
                    transition!(SendError {
                        send: state.conn.send(BackendMessage::ErrorResponse {
                            severity: Severity::Fatal,
                            code: "99999",
                            message: "internal error: did not receive results of the right type"
                                .into(),
                            detail: None,
                        }),
                        session: state.session,
                        fatal: true,
                    })
                };
                let stream: MessageStream = Box::new(futures::stream::iter_ok(
                    results.into_iter().map(format_update),
                ));
                transition!(SendUpdates {
                    send: Box::new(stream.forward(state.conn)),
                    session: state.session,
                })
            }
            Ok(Async::Ready(None)) | Err(()) => {
                panic!("Connection to dataflow server closed unexpectedly")
            }
        }
    }

    fn poll_wait_for_rows<'s, 'c>(
        state: &'s mut RentToOwn<'s, WaitForRows<A>>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterWaitForRows<A>, failure::Error> {
        let state = if state.remaining_results > 0 {
            match context.dataflow_results_receiver.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some(results))) => {
                    let mut state = state.take();
                    if let Exfiltration::Peek(results) = results {
                        state.peek_results.extend(results)
                    } else {
                        transition!(SendError {
                            send: state.conn.send(BackendMessage::ErrorResponse {
                                severity: Severity::Fatal,
                                code: "99999",
                                message:
                                    "internal error: did not receive results of the right type"
                                        .into(),
                                detail: None,
                            }),
                            session: state.session,
                            fatal: true,
                        })
                    }
                    state.remaining_results -= 1;
                    if state.remaining_results > 0 {
                        transition!(state)
                    }
                    state
                }
                Ok(Async::Ready(None)) | Err(()) => {
                    panic!("Connection to dataflow server closed unexpectedly")
                }
            }
        } else {
            state.take()
        };

        let mut peek_results = state.peek_results;
        peek_results.sort();
        let row_type = state.row_type;
        transition!(send_rows(state.conn, state.session, peek_results, row_type))
    }

    fn poll_send_updates<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendUpdates<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendUpdates<A>, failure::Error> {
        let (_, conn) = try_ready!(state.send.poll());
        let state = state.take();
        transition!(WaitForUpdates {
            conn: conn,
            session: state.session
        })
    }

    fn poll_send_command_complete<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendCommandComplete<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendCommandComplete<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
        transition!(SendReadyForQuery {
            send: conn.send(BackendMessage::ReadyForQuery),
            session: state.session,
        })
    }

    fn poll_send_error<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendError<A>>,
        _: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterSendError<A>, failure::Error> {
        let conn = try_ready!(state.send.poll());
        let state = state.take();
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
) -> SendCommandComplete<A>
where
    A: Conn + 'static,
    R: IntoIterator<Item = Vec<Datum>>,
    <R as IntoIterator>::IntoIter: 'static + Send,
{
    let rows = rows
        .into_iter()
        .map(move |row| BackendMessage::DataRow(message::field_values_from_row(row, &row_type)))
        .chain(iter::once(BackendMessage::CommandComplete {
            tag: "SELECT".into(),
        }));
    SendCommandComplete {
        send: Box::new(stream::iter_ok(rows).forward(conn).map(|(_, conn)| conn)),
        session,
    }
}
