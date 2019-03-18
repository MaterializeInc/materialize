// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use futures::Future;
use tokio::codec::Framed;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::dataflow;
use crate::pgwire;

pub use pgwire::match_handshake;

pub fn handle_connection<A: AsyncRead + AsyncWrite + 'static>(
    a: A,
    conn_state: super::ConnState,
) -> impl Future<Item = (), Error = io::Error> {
    let stream = Framed::new(a, pgwire::Codec::new());
    pgwire::StateMachine::start(stream, conn_state)
}
