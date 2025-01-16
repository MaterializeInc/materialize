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
//! [`SourceConnections`](mz_storage_types::sources::SourceConnection)
//! implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the
//! actual object created by a `CREATE SOURCE` statement, is created by
//! composing [`create_raw_source`] with decoding,
//! [`SourceEnvelope`](mz_storage_types::sources::SourceEnvelope) rendering, and
//! more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use crate::source::types::SourceMessage;

pub mod generator;
mod kafka;
mod mysql;
mod postgres;
mod probe;
pub(crate) mod reclock;
mod source_reader_pipeline;
mod statistics;
pub mod types;

pub use kafka::KafkaSourceReader;
pub use source_reader_pipeline::{
    create_raw_source, RawSourceCreationConfig, SourceExportCreationConfig,
};
