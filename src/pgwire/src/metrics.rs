// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::{
    metric,
    metrics::{raw::HistogramVec, MetricsRegistry, UIntCounter},
};

#[derive(Clone, Debug)]
pub struct Metrics {
    pub command_durations: HistogramVec,
    pub bytes_sent: UIntCounter,
    pub rows_returned: UIntCounter,
    pub query_count: UIntCounter,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Metrics {
        Metrics {
            command_durations: registry.register(metric!(
                name: "mz_command_durations",
                help: "how long individual commands took",
                var_labels: ["command", "status"],
            )),

            query_count: registry.register(metric!(
                name: "mz_query_count",
                help: "total number of queries executed",
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
