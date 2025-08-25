// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Disallow usage of `unwrap()`.
#![warn(clippy::unwrap_used)]
#![cfg_attr(nightly_doc_features, feature(doc_cfg))]
// Without this, cargo clippy complains with:
//     overflow evaluating the requirement `&str: std::marker::Send`
// in implement_peek_plan's return of a Result<crate::ExecuteResponse, AdapterError>.
#![recursion_limit = "256"]

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

mod active_compute_sink;
mod command;
mod coord;
mod error;
mod explain;
mod notice;
mod optimize;
mod util;

pub mod catalog;
pub mod client;
pub mod config;
pub mod continual_task;
pub mod flags;
pub mod metrics;
pub mod peek_client;
pub mod session;
pub mod statement_logging;
pub mod telemetry;
pub mod webhook;

pub use crate::peek_client::PeekClient;

pub use crate::client::{Client, Handle, SessionClient};
pub use crate::command::{ExecuteResponse, ExecuteResponseKind, StartupResponse};
pub use crate::coord::ExecuteContext;
pub use crate::coord::ExecuteContextExtra;
pub use crate::coord::id_bundle::CollectionIdBundle;
pub use crate::coord::peek::PeekResponseUnary;
pub use crate::coord::read_policy::ReadHolds;
pub use crate::coord::timeline::TimelineContext;
pub use crate::coord::timestamp_selection::{
    TimestampContext, TimestampExplanation, TimestampProvider,
};
pub use crate::coord::{Config, load_remote_system_parameters, serve};
pub use crate::error::AdapterError;
pub use crate::notice::AdapterNotice;
pub use crate::util::{ResultExt, verify_datum_desc};
pub use crate::webhook::{
    AppendWebhookError, AppendWebhookResponse, AppendWebhookValidator, WebhookAppenderCache,
};
