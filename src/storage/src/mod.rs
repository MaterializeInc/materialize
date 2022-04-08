// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod decode;
pub(crate) mod render;
pub mod source;
pub mod storage_state;

use timely::logging::WorkerIdentifier;

use mz_expr::SourceInstanceId;
use mz_repr::adt::jsonb::Jsonb;

/// Type alias for logging of materialized events.
pub type Logger = timely::logging_core::Logger<StorageEvent, WorkerIdentifier>;

/// Introspection events produced by the storage layer.
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum StorageEvent {
    /// Underling librdkafka statistics for a Kafka source.
    KafkaSourceStatistics {
        /// Materialize source identifier.
        source_id: SourceInstanceId,
        /// The old JSONB statistics blob to retract, if any.
        old: Option<Jsonb>,
        /// The new JSONB statistics blob to produce, if any.
        new: Option<Jsonb>,
    },
    /// Tracks the source name, id, partition id, and received/ingested offsets
    SourceInfo {
        /// Name of the source
        source_name: String,
        /// Source identifier
        source_id: SourceInstanceId,
        /// Partition identifier
        partition_id: Option<String>,
        /// Difference between the previous offset and current highest offset we've seen
        offset: i64,
        /// Difference between the previous timestamp and current highest timestamp we've seen
        timestamp: i64,
    },
}
