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
        return Err(crate::error::CoordError::Unstructured(::anyhow::anyhow!($($e),*)))
    }
}

mod cache;
mod client;
mod command;
mod coord;
mod error;
mod id_alloc;
mod persistence;
mod sink_connector;
mod timestamp;
mod util;

pub mod catalog;
pub mod session;

pub use crate::cache::CacheConfig;
pub use crate::client::{Client, ConnClient, Handle, SessionClient};
pub use crate::command::{Cancelled, ExecuteResponse, StartupMessage, StartupResponse};
pub use crate::coord::{serve, Config, LoggingConfig};
pub use crate::error::CoordError;
pub use crate::persistence::PersistenceConfig;
