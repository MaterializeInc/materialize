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
    client_cert_validations: IntCounterVec,
}

impl MetricsConfig {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            connection_status: registry.register(metric! {
                name: "mz_connection_status",
                help: "Count of completed network connections, by status",
                var_labels: ["source", "status"],
            }),
            client_cert_validations: registry.register(metric! {
                name: "mz_pgwire_client_cert_validations_total",
                help: "Count of mutual TLS client certificate evaluations, by outcome",
                var_labels: ["source", "result"],
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Metrics {
    inner: MetricsConfig,
    label: &'static str,
}

impl Metrics {
    pub fn new(inner: MetricsConfig, label: &'static str) -> Self {
        let self_ = Self { inner, label };

        // pre-initialize labels we are planning to use to ensure they are all
        // always emitted as time series
        self_.connection_status(false);
        self_.connection_status(true);
        // `trusted` and `absent` are the two outcomes an operator watches during
        // an mTLS rollout, so emit them from the start rather than only once one
        // occurs.
        self_.client_cert_validation("trusted");
        self_.client_cert_validation("absent");

        self_
    }

    pub fn connection_status(&self, is_ok: bool) -> IntCounter {
        self.inner
            .connection_status
            .with_label_values(&[self.source_label(), Self::status_label(is_ok)])
    }

    /// Counts one client certificate evaluation. `result` is `trusted`, or the
    /// label of the [`mz_authenticator::client_cert::MtlsError`] that rejected
    /// it.
    pub fn client_cert_validation(&self, result: &str) -> IntCounter {
        self.inner
            .client_cert_validations
            .with_label_values(&[self.source_label(), result])
    }

    fn status_label(is_ok: bool) -> &'static str {
        if is_ok { "success" } else { "error" }
    }

    fn source_label(&self) -> &'static str {
        self.label
    }
}
