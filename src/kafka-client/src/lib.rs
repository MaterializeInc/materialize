// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A native Kafka client in safe Rust with the API of rust-rdkafka.
//!
//! This crate replaces `rdkafka` (rust-rdkafka over librdkafka) in
//! Materialize. It provides the subset of rust-rdkafka's API that Materialize
//! uses, under the same module paths, so that switching is mostly a matter of
//! replacing `rdkafka::` with `mz_kafka_client::`. Behavior that Materialize
//! observes follows librdkafka 2.12.1: configuration property names, defaults
//! and validation, error codes and their texts, the `Display` of errors, and
//! log line texts that Materialize parses.
//!
//! # Deviations from rust-rdkafka
//!
//! * `ClientContext::stats_raw` does not exist; statistics arrive parsed, at
//!   [`ClientContext::stats`]. [`Statistics`] uses `BTreeMap`s, and so do
//!   [`admin::AlterConfig::entries`] and [`admin::ConfigResource::entry_map`].
//! * [`consumer::ConsumerContext`] has only `pre_rebalance`, `post_rebalance`
//!   and `commit_callback`. There is no `NativeClient`.
//! * [`types::RDKafkaRespErr`] is an alias of [`types::RDKafkaErrorCode`].
//! * [`version`] replaces `rdkafka_sys::rd_kafka_version_str`.
//! * [`ClientContext`] requires `'static`, and [`util::IntoOpaque`] is a
//!   `'static` marker trait without pointer conversions.
//! * [`consumer::StreamConsumer`] and [`producer::FutureProducer`] are not
//!   generic over an async runtime; their futures work with any executor.
//! * Configuration properties outside the set this crate implements are
//!   rejected with librdkafka's "No such configuration property" error, except
//!   for a list of librdkafka tuning properties that are accepted and ignored.
//!
//! # Threading
//!
//! All network I/O of all clients in the process runs on one shared tokio
//! runtime with a small number of threads (`MZ_KAFKA_CLIENT_IO_THREADS`,
//! default `min(4, available_parallelism)`), not on threads per client and
//! broker as in librdkafka. Blocking APIs (`fetch_metadata`, `commit`,
//! `flush`, transactions, `poll` with a timeout) wait on that runtime from the
//! calling thread, so they are safe to call from any thread, including tokio
//! worker threads, which they block. Context callbacks run on the runtime; see
//! [`ClientContext`] for which ones may block.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// TODO: remove once the engines use the crate-internal constructors and
// settings.
#![allow(dead_code)]

pub mod admin;
pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod message;
pub mod metadata;
pub mod producer;
pub mod statistics;
pub mod topic_partition_list;
pub mod types;
pub mod util;

mod internal;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod api_shape_tests;

pub use crate::client::ClientContext;
pub use crate::config::ClientConfig;
pub use crate::message::{Message, Timestamp};
pub use crate::statistics::Statistics;
pub use crate::topic_partition_list::{Offset, TopicPartitionList};
pub use crate::util::IntoOpaque;

/// Returns the version of this client.
///
/// Replaces librdkafka's `rd_kafka_version_str`.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
