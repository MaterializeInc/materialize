// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics that sinks report.

use mz_ore::metrics::MetricsRegistry;

/// TODO(undocumented)
#[derive(Clone)]
pub struct SinkBaseMetrics {
    // TODO(teskje): Collect metrics for persist sinks.
}

impl SinkBaseMetrics {
    /// TODO(undocumented)
    pub fn register_with(_registry: &MetricsRegistry) -> Self {
        Self {}
    }
}
