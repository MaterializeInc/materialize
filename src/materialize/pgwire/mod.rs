// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

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

mod codec;
mod message;
mod oid;
mod protocol;

pub use codec::Codec;
pub use protocol::{match_handshake, Conn, StateMachine};
