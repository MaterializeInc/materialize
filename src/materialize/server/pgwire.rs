// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use futures::Future;
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::pgwire;

pub use pgwire::match_handshake;

pub fn handle_connection<A: AsyncRead + AsyncWrite + 'static + Send>(
    a: A,
    conn_state: super::ConnState,
) -> impl Future<Item = (), Error = failure::Error> {
    let stream = Framed::new(a, pgwire::Codec::new());
    pgwire::StateMachine::start(stream, conn_state)
}
