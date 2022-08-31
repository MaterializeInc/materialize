// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow raw sources.
//!
//! Raw sources are streams (currently, Timely streams) of data directly
//! produced by the upstream service. The main export of this module is
//! [`create_raw_source`], which turns
//! [`RawSourceCreationConfigs`](RawSourceCreationConfig),
//! [`SourceConnections`](crate::types::sources::SourceConnection), and
//! [`SourceReader`] implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the
//! actual object created by a `CREATE SOURCE` statement, is created by
//! composing [`create_raw_source`] with decoding,
//! [`SourceEnvelope`](crate::types::sources::SourceEnvelope) rendering, and
//! more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use differential_dataflow::Hashable;

use mz_expr::PartitionId;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_repr::GlobalId;

use crate::controller::CollectionMetadata;
use crate::source::types::SourceMessageType;
use crate::source::types::SourceReaderError;
use crate::source::types::{NextMessage, SourceMessage, SourceReader};

mod delimited_value_reader;
pub mod generator;
mod healthcheck;
mod kafka;
mod kinesis;
pub mod metrics;
pub mod persist_source;
mod postgres;
mod reclock;
mod s3;
mod source_reader_pipeline;
pub mod types;
pub mod util;

pub use delimited_value_reader::DelimitedValueSource;
pub use generator::LoadGeneratorSourceReader;
pub use kafka::KafkaSourceReader;
pub use kinesis::KinesisSourceReader;
pub use postgres::PostgresSourceReader;
pub use s3::S3SourceReader;
pub use source_reader_pipeline::create_raw_source;
pub use source_reader_pipeline::RawSourceCreationConfig;

/// Returns true if the given source id/worker id is responsible for handling the given
/// partition.
pub fn responsible_for(
    _source_id: &GlobalId,
    worker_id: usize,
    worker_count: usize,
    pid: &PartitionId,
) -> bool {
    // Distribute partitions equally amongst workers.
    (usize::cast_from(pid.hashed()) % worker_count) == worker_id
}
