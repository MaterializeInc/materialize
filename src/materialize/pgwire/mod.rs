// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! PostgreSQL network ("wire") protocol.
//!
//! For brevity, we often refer to the protocol as "pgwire," hence the name of
//! this module. Beware that this name is only commonly used in the CockroachDB
//! and Materialize ecosystems. The PostgreSQL documentation, for example, uses
//! the long-winded "Frontend/Backend Protocol" title instead.
//!
//! # Useful references
//!
//!   * [PostgreSQL Frontend/Backend Protocol documentation](https://www.postgresql.org/docs/11/protocol.html)
//!   * [CockroachDB pgwire implementation](https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/pgwire)
//!   * ["Postgres on the wire" PGCon talk](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)

use failure::format_err;
use futures::sync::mpsc::UnboundedSender;
use futures::{future, Future};
use lazy_static::lazy_static;
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};

use self::id_alloc::{IdAllocator, IdExhaustionError};
use crate::queue;
use dataflow::Exfiltration;
use ore::future::FutureExt;
use ore::mpmc::Mux;

mod codec;
mod id_alloc;
mod message;
mod protocol;
mod types;

pub use codec::Codec;
pub use protocol::match_handshake;

pub fn serve<A: AsyncRead + AsyncWrite + 'static + Send>(
    a: A,
    cmdq_tx: UnboundedSender<queue::Command>,
    dataflow_results_mux: Mux<u32, Exfiltration>,
    num_timely_workers: usize,
) -> impl Future<Item = (), Error = failure::Error> {
    lazy_static! {
        static ref CONN_ID_ALLOCATOR: id_alloc::IdAllocator = IdAllocator::new(1, 1 << 16);
    }
    let conn_id = match CONN_ID_ALLOCATOR.alloc() {
        Ok(id) => id,
        Err(IdExhaustionError) => {
            return future::err(format_err!("maximum number of connections reached")).left()
        }
    };
    let stream = Framed::new(a, codec::Codec::new());
    let dataflow_results_receiver = {
        let mut mux = dataflow_results_mux.write().unwrap();
        mux.channel(conn_id).unwrap();
        mux.receiver(&conn_id).unwrap()
    };
    protocol::StateMachine::start(
        stream,
        sql::Session::default(),
        protocol::Context {
            conn_id,
            cmdq_tx,
            dataflow_results_receiver,
            num_timely_workers,
        },
    )
    .then(move |res| {
        CONN_ID_ALLOCATOR.free(conn_id);
        res
    })
    .right()
}
