// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for sinks.

use mz_ore::metrics::MetricsRegistry;

pub mod kafka;

/// A set of base metrics that hang off a central metrics registry, labeled by the sink they
/// belong to.
#[derive(Debug, Clone)]
pub(crate) struct SinkMetricDefs {
    pub(crate) kafka_defs: kafka::KafkaSinkMetricDefs,
}

impl SinkMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            kafka_defs: kafka::KafkaSinkMetricDefs::register_with(registry),
        }
    }
}
