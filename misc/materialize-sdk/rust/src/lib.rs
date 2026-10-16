// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Consume Materialize `SUBSCRIBE` correctly.
//!
//! The rules of the subscribe protocol live in [`materialize_sdk_core`], which
//! this crate re-exports. This crate runs them over a `tokio-postgres` session.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use materialize_sdk::{Event, LiveOptions, LiveSubscription, Subscribe};
//!
//! let config: tokio_postgres::Config = "host=localhost port=6875 user=materialize".parse()?;
//! let subscribe = Subscribe::query("SELECT * FROM orders");
//! let mut live = LiveSubscription::connect(
//!     &config,
//!     tokio_postgres::NoTls,
//!     subscribe,
//!     LiveOptions::default(),
//! )
//! .await?;
//! while let Some(event) = live.next().await {
//!     match event? {
//!         Event::Batch(batch) => println!("{} updates up to {}", batch.updates().len(), batch.upper()),
//!         Event::SnapshotChunk { updates } => println!("{} snapshot updates", updates.len()),
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::unreachable,
        clippy::todo,
        clippy::unimplemented,
        clippy::arithmetic_side_effects,
    )
)]
// The repository's clippy rules ask for `mz_ore` and `mz_postgres_util`
// wrappers around `tokio::spawn` and the `tokio-postgres` client, which a
// standalone SDK cannot depend on.
#![allow(clippy::disallowed_methods)]

mod durable;
mod error;
mod live;
mod sink;
mod store;

pub use durable::{DurableOptions, DurableSink, NoStore};
pub use error::Error;
pub use live::{LiveOptions, LiveSubscription};
pub use materialize_sdk_core::catalog::ObjectName;
pub use materialize_sdk_core::{
    self as core, Batch, Change, CheckpointError, CheckpointWrite, Classification, CommitConflict,
    Envelope, ErrorKind, Event, Frontier, IdentityChange, ObjectRef, ProtocolError, Row, SinkType,
    Subscribe, Target, TimestampTooLarge, Update, UsageError, WriteKind,
};
pub use sink::{Rejected, Sink, SinkBatch, SinkError, WriteResult};
pub use store::{CheckpointStore, MaterializeStore, checkpoint_table_sql};
