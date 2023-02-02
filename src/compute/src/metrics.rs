// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::UIntGauge;

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;

#[derive(Clone, Debug)]
pub struct ComputeMetrics {
    pub command_history_size: UIntGauge,
    pub dataflow_count_in_history: UIntGauge,
}

impl ComputeMetrics {
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            command_history_size: registry.register(metric!(
                name: "mz_compute_command_history_size",
                help: "The size of the compute command history.",
            )),
            dataflow_count_in_history: registry.register(metric!(
                name: "mz_compute_dataflow_count_in_history",
                help: "The number of dataflow descriptions in the compute command history.",
            )),
        }
    }
}
