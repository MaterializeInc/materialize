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

//! A correct-by-construction client for consuming Materialize
//! [`SUBSCRIBE`](https://materialize.com/docs/sql/subscribe/).
//!
//! Consuming `SUBSCRIBE` durably requires a specific protocol: buffer until a
//! progress message closes a timestamp, apply the closed batch and persist its
//! frontier atomically, and resume with `SNAPSHOT = false AS OF frontier - 1`.
//! Every step has a failure mode that is invisible in testing. This crate
//! encodes the protocol so callers cannot get it wrong:
//!
//! * The unit of consumption is a [`ConsistentBatch`], never a bare row. You
//!   physically cannot observe a half-closed timestamp.
//! * Resuming takes an opaque [`ResumeToken`]. The `- 1` boundary arithmetic
//!   lives inside it, so you never compute a timestamp.
//! * Failures surface as a small, typed [`SubscribeError`] taxonomy.
//!
//! # Example
//!
//! ```no_run
//! use mz_subscribe::{Subscribe, SubscribeClient};
//!
//! # async fn run() -> Result<(), mz_subscribe::SubscribeError> {
//! let client = SubscribeClient::connect("postgres://materialize@localhost:6875").await?;
//!
//! // Subscribe to a view with the upsert envelope, keyed by `id`.
//! let mut stream = client
//!     .subscribe(Subscribe::object("winning_bids").envelope_upsert(["id"]))
//!     .await?;
//!
//! while let Some(batch) = stream.next().await? {
//!     for change in &batch.updates {
//!         // Apply `change` to your sink.
//!         let _ = change;
//!     }
//!     // Persist `batch.resume_token` atomically with the applied changes to
//!     // get exactly-once state. On restart, hand it back to `client.resume`.
//!     let _token = batch.resume_token.encode();
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Guarantees
//!
//! Applying each batch and persisting its token in the same transaction yields
//! exactly-once *state*: after any crash, resuming from the last token neither
//! drops nor duplicates data. For side-effecting sinks (webhooks, queues) that
//! cannot transact with the checkpoint, use the token for at-least-once
//! delivery and deduplicate downstream.

mod batch;
mod client;
mod envelope;
mod error;
mod statement;
mod token;

pub use batch::{Batcher, ConsistentBatch};
pub use client::{BatchStream, SubscribeClient};
pub use envelope::{Change, Datum, Decoder, Envelope, Row, StreamMessage};
pub use error::SubscribeError;
pub use statement::{Relation, Subscribe};
pub use token::ResumeToken;
