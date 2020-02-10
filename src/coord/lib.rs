// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinates client requests with the dataflow layer.
//!
//! Client requests are either a "simple" query, in which SQL is parsed,
//! planned, and executed in one shot, or an "extended" query, where the client
//! controls the parsing, planning, and execution via individual messages,
//! allowing it to reuse pre-parsed and pre-planned queries (i.e., via "prepared
//! statements"), which can be more efficient when the same query is executed
//! multiple times.
//!
//! These commands are derived directly from the commands that
//! [`pgwire`](../pgwire/index.html) produces, though they can, in theory, be
//! provided by something other than a pgwire server.

mod command;
mod coord;
mod persistence;
mod timestamp;

pub use self::coord::{Config, Coordinator};
pub use self::timestamp::{TimestampChannel, TimestampMessage, Timestamper};
pub use command::{Command, ExecuteResponse, Response, RowsFuture, StartupMessage};
