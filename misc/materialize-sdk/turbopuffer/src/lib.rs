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

//! A sink that keeps turbopuffer namespaces equal to a Materialize view.
//!
//! The sink reads the upsert envelope, keyed by columns that are unique in the
//! view. Each key is one document whose id is a string: the key's text, or the
//! key's cells as a JSON array for a key of several columns. The document
//! holds every column as an attribute, except a single key column named `id`,
//! which is the id. It also holds the `mz_timestamp` of the key's last change
//! and `deleted`. A delete leaves a tombstone with `deleted = true`, so
//! searches filter on `deleted = false`.
//!
//! A sink named `articles` keeps its checkpoint in the namespace
//! `articles__checkpoint`, and each snapshot writes a new incarnation, such as
//! `articles__i2` (see [`data_namespace`]). Readers switch to an incarnation
//! once its snapshot completes: the `active` attribute of the checkpoint
//! document names it.
//!
//! turbopuffer has no transactions across requests, so the guarantee is
//! convergence: once the sink has caught up and turbopuffer has indexed its
//! writes, the active incarnation equals the view at the committed frontier.
//! Before that a reader can see part of a batch.
//!
//! With [`TurbopufferSink::embed`], turbopuffer embeds the text itself, and
//! bills by token. A change that leaves every embedded text as it was is
//! written as a patch that keeps the stored vectors, so only documents with
//! a changed text are embedded again, with all their texts. A snapshot
//! embeds every text it writes.
//!
//! A write sent again after a timeout skips the requests turbopuffer already
//! acknowledged, but each timeout still uses up an attempt, so raise
//! [`materialize_sdk::DurableOptions::write_timeout`] for batches that take
//! longer than a minute to write. A commit sends up to three requests, so
//! keep [`materialize_sdk::DurableOptions::store_timeout`] above three times
//! [`Connection::with_request_timeout`], or a slow commit counts as unknown
//! and is settled by reading the checkpoint back.
//!
//! To tear a sink down, stop its workers, wait until no delayed write of
//! theirs can still land, and delete every namespace named after it. Setup
//! refuses a name whose incarnations still exist, because a late write could
//! recreate one.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use materialize_sdk::{DurableOptions, DurableSink, Envelope, ObjectName};
//! use materialize_sdk_turbopuffer::{Connection, TurbopufferSink};
//!
//! let materialize: tokio_postgres::Config = "host=localhost port=6875 user=materialize".parse()?;
//! let turbopuffer = Connection::new("gcp-us-central1", std::env::var("TURBOPUFFER_API_KEY")?);
//! let sink = TurbopufferSink::new(&turbopuffer, "articles-search", "articles")?
//!     .full_text_search(["body"])
//!     .embed("body", "nvidia/nemotron-3-embed-8b", 1024);
//! let mut durable = DurableSink::new("articles-search", sink, DurableOptions::default());
//! let view = ObjectName {
//!     database: Some("materialize".into()),
//!     schema: "public".into(),
//!     name: "articles".into(),
//! };
//! let envelope = Envelope::Upsert { key: vec!["id".into()] };
//! durable.setup(&materialize, tokio_postgres::NoTls, &view, envelope).await?;
//! durable.run(&materialize, tokio_postgres::NoTls).await?;
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

mod checkpoint;
mod client;
mod documents;
#[cfg(test)]
mod live_tests;
mod pace;
mod sink;

pub use client::Connection;
pub use sink::{TurbopufferSink, data_namespace};
