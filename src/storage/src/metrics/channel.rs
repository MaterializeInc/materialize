// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for tracking metrics related to decoding.

use mz_ore::metric;
use mz_ore::metrics::raw::IntCounterVec;
use mz_ore::metrics::MetricsRegistry;

/// Metrics specific to a single worker.
#[derive(Clone, Debug)]
pub struct ChannelMetricDefs {
    pub(crate) sends: IntCounterVec,
    pub(crate) recvs: IntCounterVec,
}

impl ChannelMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            sends: registry.register(metric!(
                name: "mz_storage_source_channel_sends",
                help: "Sends to unbounded channels in the source pipeline.",
                var_labels: ["source_id", "worker_id", "worker_count", "location"],
            )),
            recvs: registry.register(metric!(
                name: "mz_storage_source_channel_recvs",
                help: "Receives from unbounded channels in the source pipeline.",
                var_labels: ["source_id", "worker_id", "worker_count", "location"],
            )),
        }
    }
}
