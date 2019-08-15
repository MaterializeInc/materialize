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

use futures::Future;
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::glue::*;

mod codec;
mod message;
mod protocol;
mod types;

pub use codec::Codec;
pub use protocol::match_handshake;

pub fn serve<A: AsyncRead + AsyncWrite + 'static + Send>(
    a: A,
    sql_command_sender: UnboundedSender<(SqlCommand, CommandMeta)>,
    sql_result_mux: SqlResultMux,
    dataflow_results_mux: DataflowResultsMux,
    num_timely_workers: usize,
) -> impl Future<Item = (), Error = failure::Error> {
    let uuid = Uuid::new_v4();
    let stream = Framed::new(a, codec::Codec::new());
    let sql_result_receiver = {
        let mut mux = sql_result_mux.write().unwrap();
        mux.channel(uuid).unwrap();
        mux.receiver(&uuid).unwrap()
    };
    let dataflow_results_receiver = {
        let mut mux = dataflow_results_mux.write().unwrap();
        mux.channel(uuid).unwrap();
        mux.receiver(&uuid).unwrap()
    };
    protocol::StateMachine::start(
        stream,
        crate::sql::Session::default(),
        protocol::Context {
            uuid,
            sql_command_sender,
            sql_result_receiver,
            dataflow_results_receiver,
            num_timely_workers,
        },
    )
}
