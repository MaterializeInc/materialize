// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use byteorder::{ByteOrder, NetworkEndian};
use futures::sink::Send;
use futures::{try_ready, Async, Future, Poll, Sink, Stream};
use state_machine_future::StateMachineFuture as Smf;
use state_machine_future::{transition, RentToOwn};
use tokio::codec::Framed;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::dataflow;
use crate::pgwire::codec::Codec;
use crate::pgwire::message::{BackendMessage, FrontendMessage, Severity};
use crate::server::ConnState;
use ore::future::{Recv, StreamExt};

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

const VERSIONS: &'static [u32] = &[VERSION_1, VERSION_2, VERSION_3, VERSION_SSL, VERSION_CANCEL];

/// Reports whether the given stream begins with a pgwire handshake.
///
/// To avoid false negative, there must be at least eight bytes in `buf`.
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
/// [#41587]: https://github.com/rust-lang/rust/issues/41517
pub trait Conn:
    Stream<Item = FrontendMessage, Error = io::Error>
    + Sink<SinkItem = BackendMessage, SinkError = io::Error>
{
}

impl<A> Conn for Framed<A, Codec> where A: AsyncWrite + AsyncRead + 'static {}

/// A state machine that drives the pgwire backend.
///
/// Much of the state machine boilerplate is generated automatically with the
/// help of the [`state_machine_future`] package. There is a bit too much magic
/// in this approach for my taste, but attempting to write a futures-driven
/// server without it is unbelievably painful. Consider revisiting once
/// async/await support lands in stable.
#[derive(Smf)]
#[state_machine_future(context = "ConnState")]
pub enum StateMachine<A: Conn + 'static> {
    #[state_machine_future(start, transitions(RecvStartup))]
    Start {
        stream: A,
    },

    #[state_machine_future(transitions(SendAuthenticationOk, SendError, Error))]
    RecvStartup {
        recv: Recv<A>,
    },

    #[state_machine_future(transitions(SendReadyForQuery, SendError, Error))]
    SendAuthenticationOk {
        send: Send<A>,
    },

    #[state_machine_future(transitions(RecvQuery, SendError, Error))]
    SendReadyForQuery {
        send: Send<A>,
    },

    #[state_machine_future(transitions(HandleQuery, SendError, Error))]
    RecvQuery {
        recv: Recv<A>,
    },

    #[state_machine_future(transitions(SendResults, SendError, Error))]
    HandleQuery {
        handle: Box<Future<Item = (), Error = failure::Error> + std::marker::Send>,
        conn: A,
    },

    #[state_machine_future(transitions(SendReadyForQuery, Error))]
    SendResults {
        send: Send<A>,
    },

    #[state_machine_future(transitions(SendReadyForQuery, Done, Error))]
    SendError {
        send: Send<A>,
        fatal: bool,
    },

    #[state_machine_future(ready)]
    Done(()),

    #[state_machine_future(error)]
    Error(io::Error),
}

impl<A: Conn> PollStateMachine<A> for StateMachine<A> {
    fn poll_start<'s, 'c>(
        state: &'s mut RentToOwn<'s, Start<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterStart<A>, io::Error> {
        let state = state.take();
        transition!(RecvStartup {
            recv: state.stream.recv(),
        })
    }

    fn poll_recv_startup<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvStartup<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterRecvStartup<A>, io::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        let version = match msg {
            FrontendMessage::Startup { version } => version,
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow".into(),
                    detail: None,
                }),
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
                fatal: true,
            })
        }

        transition!(SendAuthenticationOk {
            send: conn.send(BackendMessage::AuthenticationOk)
        })
    }

    fn poll_send_authentication_ok<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendAuthenticationOk<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterSendAuthenticationOk<A>, io::Error> {
        let conn = try_ready!(state.send.poll());
        transition!(SendReadyForQuery {
            send: conn.send(BackendMessage::ReadyForQuery),
        })
    }

    fn poll_send_ready_for_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendReadyForQuery<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterSendReadyForQuery<A>, io::Error> {
        let conn = try_ready!(state.send.poll());
        transition!(RecvQuery {
            recv: conn.recv()
        })
    }

    fn poll_recv_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, RecvQuery<A>>,
        conn_state: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterRecvQuery<A>, io::Error> {
        let (msg, conn) = try_ready!(state.recv.poll());
        match msg {
            FrontendMessage::Query { query } => {
                transition!(HandleQuery {
                    handle: Box::new(crate::sql::handle_query(String::from(String::from_utf8_lossy(&query)), conn_state)),
                    conn: conn,
                })
            }
            _ => transition!(SendError {
                send: conn.send(BackendMessage::ErrorResponse {
                    severity: Severity::Fatal,
                    code: "08P01",
                    message: "invalid frontend message flow".into(),
                    detail: None,
                }),
                fatal: true,
            }),
        };

    }

    fn poll_handle_query<'s, 'c>(
        state: &'s mut RentToOwn<'s, HandleQuery<A>>,
        conn_state: &'c mut RentToOwn<'c, ConnState>,
    ) -> Poll<AfterHandleQuery<A>, io::Error> {
        match state.handle.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(())) => (),
            Err(err) => {
                let state = state.take();
                transition!(SendError {
                    send: state.conn.send(BackendMessage::ErrorResponse {
                        severity: Severity::Error,
                        code: "99999",
                        message: err.to_string(),
                        detail: None,
                    }),
                    fatal: false,
                });
            }
        }
        let state = state.take();
        let send = state.conn.send(BackendMessage::CommandComplete { tag: "SELECT 0" });
        transition!(SendResults {
            send: send,
        })
    }

    fn poll_send_results<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendResults<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterSendResults<A>, io::Error> {
        let conn = try_ready!(state.send.poll());
        transition!(SendReadyForQuery {
            send: conn.send(BackendMessage::ReadyForQuery),
        })
    }

    fn poll_send_error<'s, 'c>(
        state: &'s mut RentToOwn<'s, SendError<A>>,
        _: &'c mut RentToOwn<'c, ConnState>
    ) -> Poll<AfterSendError<A>, io::Error> {
        let conn = try_ready!(state.send.poll());
        if state.fatal {
            transition!(Done(()))
        } else {
            transition!(SendReadyForQuery {
                send: conn.send(BackendMessage::ReadyForQuery),
            })
        }
    }
}
