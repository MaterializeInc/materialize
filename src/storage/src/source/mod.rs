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
//! [`RawSourceCreationConfigs`](RawSourceCreationConfig) and
//! [`SourceConnections`](mz_storage_client::types::sources::SourceConnection)
//! implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the
//! actual object created by a `CREATE SOURCE` statement, is created by
//! composing [`create_raw_source`] with decoding,
//! [`SourceEnvelope`](mz_storage_client::types::sources::SourceEnvelope) rendering, and
//! more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use differential_dataflow::Hashable;

use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;

use crate::source::types::SourceMessage;
use crate::source::types::SourceReaderError;

pub mod generator;
mod kafka;
pub mod metrics;
mod postgres;
pub(crate) mod reclock;
mod resumption;
mod source_reader_pipeline;
// Public for integration testing.
#[doc(hidden)]
pub mod testscript;
pub mod types;

pub use kafka::KafkaSourceReader;
pub use postgres::PostgresSourceReader;
pub use source_reader_pipeline::create_raw_source;
pub(crate) use source_reader_pipeline::health_operator;
pub use source_reader_pipeline::{RawSourceCreationConfig, SourceCreationParams};

/// Returns true if the given source id/worker id is responsible for handling the given
/// partition.
pub fn responsible_for<P: Hashable>(
    _source_id: &GlobalId,
    worker_id: usize,
    worker_count: usize,
    pid: P,
) -> bool {
    // Distribute partitions equally amongst workers.
    (usize::cast_from(pid.hashed().into()) % worker_count) == worker_id
}
