// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod kafka;
mod metrics;
mod persist_sink;
mod tail;

pub(crate) use metrics::KafkaBaseMetrics;
pub(crate) use persist_sink::persist_sink;

pub use metrics::SinkBaseMetrics;
