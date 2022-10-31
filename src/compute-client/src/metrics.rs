// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prometheus::{IntGauge};

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;

#[derive(Clone, Debug)]
pub struct ComputeMetrics {
    pub command_history_size: IntGauge,
}

impl ComputeMetrics {
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            command_history_size: registry.register(metric!(
                name: "mz_compute_comamnd_history_size",
                help: "The size of the compute command history.",
            )),
        }
    }
}
