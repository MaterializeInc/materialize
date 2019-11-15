// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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

pub use self::coord::{Config, Coordinator};
pub use command::{Command, ExecuteResponse, Response, RowsFuture};
