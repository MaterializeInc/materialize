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

// The prometheus macros (e.g. `register*`) all depend on each other, including
// on internal `__register*` macros, instead of doing the right thing and I
// assume using something like `$crate::__register_*`. That means that without
// using a macro_use here, we would end up needing to import several internal
// macros everywhere we want to use any of the prometheus macros.
#[macro_use]
extern crate prometheus;

use failure::format_err;
use futures::sync::mpsc::UnboundedSender;
use futures::{future, Future};
use lazy_static::lazy_static;
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};

use self::id_alloc::{IdAllocator, IdExhaustionError};
use ore::future::FutureExt;

mod codec;
mod id_alloc;
mod message;
mod protocol;
mod types;

pub use codec::Codec;
pub use protocol::match_handshake;

pub fn serve<A: AsyncRead + AsyncWrite + 'static + Send>(
    a: A,
    cmdq_tx: UnboundedSender<coord::Command>,
    gather_metrics: bool,
) -> impl Future<Item = (), Error = failure::Error> {
    lazy_static! {
        static ref CONN_ID_ALLOCATOR: id_alloc::IdAllocator = IdAllocator::new(1, 1 << 16);
    }
    let conn_id = match CONN_ID_ALLOCATOR.alloc() {
        Ok(id) => id,
        Err(IdExhaustionError) => {
            return future::err(format_err!("maximum number of connections reached")).left();
        }
    };
    protocol::StateMachine::start(
        Framed::new(a, codec::Codec::new()),
        sql::Session::default(),
        protocol::Context {
            conn_id,
            cmdq_tx,
            gather_metrics,
        },
    )
    .then(move |res| {
        CONN_ID_ALLOCATOR.free(conn_id);
        res
    })
    .right()
}
