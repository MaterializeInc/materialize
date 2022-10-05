// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Moving data to external systems

mod kafka;
mod metrics;
mod sink_connection;

pub(crate) use metrics::KafkaBaseMetrics;
pub use metrics::SinkBaseMetrics;
pub use sink_connection::build_sink_connection;
