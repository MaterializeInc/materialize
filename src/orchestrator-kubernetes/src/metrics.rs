// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for the Kubernetes orchestrator.

use std::error::Error;
use std::io;

use kube::error::Error as K8sError;
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::metrics::raw::IntCounterVec;

/// Metrics for a [`KubernetesOrchestrator`](crate::KubernetesOrchestrator).
#[derive(Debug, Clone)]
pub(crate) struct OrchestratorMetrics {
    process_metrics_fetch_failures: IntCounterVec,
}

/// The step of a per-process metrics fetch that failed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FetchStep {
    /// Reading the pod's `PodMetrics` from the Kubernetes metrics API.
    PodMetrics,
    /// Reading usage metrics from the clusterd process.
    ClusterdUsage,
}

impl FetchStep {
    fn as_str(&self) -> &'static str {
        match self {
            FetchStep::PodMetrics => "pod_metrics",
            FetchStep::ClusterdUsage => "clusterd_usage",
        }
    }
}

impl OrchestratorMetrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            process_metrics_fetch_failures: registry.register(metric!(
                name: "mz_orchestrator_kubernetes_process_metrics_fetch_failures_total",
                help: "The number of failed per-process metrics fetches, by fetch step and error kind.",
                var_labels: ["step", "kind"],
            )),
        }
    }

    /// Records a failed fetch whose error has the given `kind`.
    pub(crate) fn record_fetch_failure(&self, step: FetchStep, kind: &str) {
        self.process_metrics_fetch_failures
            .with_label_values(&[step.as_str(), kind])
            .inc();
    }

    /// Records a failed fetch, classifying `error` by [`error_kind`].
    pub(crate) fn record_fetch_error(&self, step: FetchStep, error: &(dyn Error + 'static)) {
        self.record_fetch_failure(step, error_kind(error));
    }
}

/// Classifies an error by walking its source chain.
///
/// Returns `timeout` if any error in the chain is a timeout. Otherwise returns the kind of the
/// outermost error that is a Kubernetes client or HTTP client error: `api` for an error status
/// from the API server, `decode` for a response that failed to deserialize, `transport` for a
/// connection or protocol error. Returns `other` for anything else.
pub(crate) fn error_kind(error: &(dyn Error + 'static)) -> &'static str {
    let mut kind = None;
    let mut next = Some(error);
    while let Some(err) = next {
        next = err.source();
        if let Some(e) = err.downcast_ref::<io::Error>() {
            if e.kind() == io::ErrorKind::TimedOut {
                return "timeout";
            }
        } else if let Some(e) = err.downcast_ref::<reqwest::Error>() {
            if e.is_timeout() {
                return "timeout";
            }
            kind.get_or_insert_with(|| if e.is_decode() { "decode" } else { "transport" });
        } else if let Some(e) = err.downcast_ref::<K8sError>() {
            kind.get_or_insert(match e {
                K8sError::Api(_) => "api",
                K8sError::SerdeError(_) => "decode",
                K8sError::HyperError(_) | K8sError::Service(_) => "transport",
                _ => "other",
            });
        }
    }
    kind.unwrap_or("other")
}

#[cfg(test)]
mod tests;
