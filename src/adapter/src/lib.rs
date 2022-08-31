// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg_attr(nightly_doc_features, feature(doc_cfg))]

//! Coordinates client requests with the dataflow layer.
//!
//! This crate hosts the "coordinator", an object which sits at the center of
//! the system and coordinates communication between the various components.
//! Responsibilities of the coordinator include:
//!
//!   * Launching the dataflow workers.
//!   * Periodically allowing the dataflow workers to compact existing data.
//!   * Executing SQL queries from clients by parsing and planning them, sending
//!     the plans to the dataflow layer, and then streaming the results back to
//!     the client.
//!   * Assigning timestamps to incoming source data.
//!
//! The main interface to the coordinator is [`Client`]. To start a coordinator,
//! use the [`serve`] function.

// TODO(benesch): delete this once we use structured errors everywhere.
macro_rules! coord_bail {
    ($($e:expr),*) => {
        return Err(crate::error::AdapterError::Unstructured(::anyhow::anyhow!($($e),*)))
    }
}

mod command;
mod coord;
mod error;
mod explain_new;
mod sink_connection;
mod tail;
mod util;

pub mod catalog;
pub mod client;
pub mod session;

pub use crate::client::{Client, ConnClient, Handle, SessionClient};
pub use crate::command::{
    Canceled, ExecuteResponse, ExecuteResponsePartialError, RowsFuture, StartupMessage,
    StartupResponse,
};
pub use crate::coord::peek::PeekResponseUnary;
pub use crate::coord::{serve, Config, DUMMY_AVAILABILITY_ZONE};
pub use crate::error::AdapterError;
