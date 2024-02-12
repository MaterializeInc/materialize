// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metric;
use mz_ore::metrics::raw::IntCounterVec;
use mz_ore::metrics::{IntCounter, MetricsRegistry};

#[derive(Clone, Debug)]
pub struct MetricsConfig {
    connection_status: IntCounterVec,
}

impl MetricsConfig {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            connection_status: registry.register(metric! {
                name: "mz_connection_status",
                help: "Count of completed network connections, by status",
                var_labels: ["source", "status"],
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Metrics {
    inner: MetricsConfig,
    internal: bool,
}

impl Metrics {
    pub fn new(inner: MetricsConfig, internal: bool) -> Self {
        let self_ = Self { inner, internal };

        // pre-initialize labels we are planning to use to ensure they are all
        // always emitted as time series
        self_.connection_status(false);
        self_.connection_status(true);

        self_
    }

    pub fn connection_status(&self, is_ok: bool) -> IntCounter {
        self.inner
            .connection_status
            .with_label_values(&[self.source_label(), Self::status_label(is_ok)])
    }

    fn status_label(is_ok: bool) -> &'static str {
        if is_ok {
            "success"
        } else {
            "error"
        }
    }

    fn source_label(&self) -> &'static str {
        if self.internal {
            "internal_pgwire"
        } else {
            "external_pgwire"
        }
    }
}
