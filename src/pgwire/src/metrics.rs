// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ore::{
    metric,
    metrics::{HistogramVec, MetricsRegistry, UIntCounter},
};

#[derive(Clone, Debug)]
pub struct Metrics {
    pub command_durations: HistogramVec,
    pub bytes_sent: UIntCounter,
    pub rows_returned: UIntCounter,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Metrics {
        Metrics {
            command_durations: registry.register(metric!(
                name: "mz_command_durations",
                help: "how long individual commands took",
                var_labels: ["command", "status"],
            )),

            rows_returned: registry.register(metric!(
                name: "mz_pg_sent_rows",
                help: "total number of rows sent to clients from pgwire",
            )),

            bytes_sent: registry.register(metric!(
                name: "mz_pg_sent_bytes",
                help: "total number of bytes sent to clients from pgwire",
            )),
        }
    }
}
