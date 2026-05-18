// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for the MCP HTTP endpoints.
//!
//! Tracks request counts, tool call counts, and tool call durations,
//! labeled by endpoint type (`agent` / `developer`) and either the
//! JSON-RPC method name or the MCP tool name. The status label is `ok`
//! for successful calls and the `McpRequestError` error type
//! (e.g. `ToolNotFound`, `DataProductNotFound`) for failures.

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{HistogramTimer, HistogramVec, IntCounterVec};

/// Metrics emitted by the MCP HTTP handlers.
///
/// Cheaply `Clone`: Prometheus collector handles are `Arc`-shared internally,
/// so the struct can be cloned freely and stored as an axum `Extension`.
#[derive(Debug, Clone)]
pub struct McpMetrics {
    /// Total MCP requests by endpoint type, JSON-RPC method, and status.
    pub requests: IntCounterVec,
    /// Total MCP `tools/call` invocations by endpoint type, tool name, and status.
    pub tool_calls: IntCounterVec,
    /// Duration of MCP `tools/call` invocations by endpoint type and tool name.
    pub tool_call_duration: HistogramVec,
}

/// RAII guard for a single `tools/call` invocation. On drop, increments
/// `tool_calls_total` with the current status and observes
/// `tool_call_duration_seconds` via the embedded [`HistogramTimer`]'s own
/// drop. Designed so that if the surrounding future is dropped before
/// completion (e.g. by `tokio::time::timeout`), the metric still records
/// with the default `"cancelled"` status instead of being silently lost.
pub struct ToolCallGuard<'a> {
    metrics: &'a McpMetrics,
    endpoint_label: &'static str,
    tool_label: String,
    status: &'static str,
    /// `HistogramTimer::drop` observes the duration into the histogram, so
    /// holding the timer here means we get the duration recorded for both
    /// normal completion and early drop.
    _timer: HistogramTimer,
}

impl<'a> ToolCallGuard<'a> {
    /// Starts a new tool call: begins the duration timer and reserves the
    /// counter increment that will happen on drop.
    pub fn new(metrics: &'a McpMetrics, endpoint_label: &'static str, tool_label: String) -> Self {
        let timer = metrics
            .tool_call_duration
            .with_label_values(&[endpoint_label, &tool_label])
            .start_timer();
        Self {
            metrics,
            endpoint_label,
            tool_label,
            status: "cancelled",
            _timer: timer,
        }
    }

    /// Records the outcome of the call. Callers should set this on the
    /// normal completion path right before the guard is dropped.
    pub fn set_status(&mut self, status: &'static str) {
        self.status = status;
    }
}

impl Drop for ToolCallGuard<'_> {
    fn drop(&mut self) {
        self.metrics
            .tool_calls
            .with_label_values(&[self.endpoint_label, &self.tool_label, self.status])
            .inc();
    }
}

impl McpMetrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            requests: registry.register(metric!(
                name: "mz_mcp_requests_total",
                help: "Total number of MCP requests received.",
                var_labels: ["endpoint_type", "method", "status"],
            )),
            tool_calls: registry.register(metric!(
                name: "mz_mcp_tool_calls_total",
                help: "Total number of MCP tools/call invocations.",
                var_labels: ["endpoint_type", "tool_name", "status"],
            )),
            tool_call_duration: registry.register(metric!(
                name: "mz_mcp_tool_call_duration_seconds",
                help: "Duration of MCP tools/call invocations in seconds.",
                var_labels: ["endpoint_type", "tool_name"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::McpMetrics;
    use mz_ore::metrics::MetricsRegistry;

    /// All three metrics register cleanly and show up in the gathered output
    /// with the expected names. `IntCounterVec` / `HistogramVec` families
    /// only appear in `gather()` after at least one label combination has
    /// been observed, so each metric is touched once before gathering.
    #[mz_ore::test]
    fn test_register_into() {
        let registry = MetricsRegistry::new();
        let metrics = McpMetrics::register_into(&registry);

        metrics
            .requests
            .with_label_values(&["agent", "initialize", "ok"])
            .inc_by(0);
        metrics
            .tool_calls
            .with_label_values(&["agent", "read_data_product", "ok"])
            .inc_by(0);
        metrics
            .tool_call_duration
            .with_label_values(&["agent", "read_data_product"])
            .observe(0.0);

        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|m| m.name().to_string())
            .collect();

        assert!(
            names.iter().any(|n| n == "mz_mcp_requests_total"),
            "mz_mcp_requests_total should be registered, got: {names:?}",
        );
        assert!(
            names.iter().any(|n| n == "mz_mcp_tool_calls_total"),
            "mz_mcp_tool_calls_total should be registered, got: {names:?}",
        );
        assert!(
            names
                .iter()
                .any(|n| n == "mz_mcp_tool_call_duration_seconds"),
            "mz_mcp_tool_call_duration_seconds should be registered, got: {names:?}",
        );
    }

    /// Incrementing each counter with realistic label values produces the
    /// expected counts in the gathered output.
    #[mz_ore::test]
    fn test_record_metrics() {
        let registry = MetricsRegistry::new();
        let metrics = McpMetrics::register_into(&registry);

        metrics
            .requests
            .with_label_values(&["agent", "tools/call", "ok"])
            .inc();
        metrics
            .requests
            .with_label_values(&["agent", "tools/call", "ok"])
            .inc();
        metrics
            .requests
            .with_label_values(&["developer", "initialize", "ok"])
            .inc();

        metrics
            .tool_calls
            .with_label_values(&["agent", "read_data_product", "ok"])
            .inc();
        metrics
            .tool_calls
            .with_label_values(&["agent", "read_data_product", "DataProductNotFound"])
            .inc();

        metrics
            .tool_call_duration
            .with_label_values(&["agent", "read_data_product"])
            .observe(0.123);

        let gathered = registry.gather();

        // requests_total: 3 increments produce 2 distinct label sets (the
        // first two share labels and so collapse into the same series).
        let requests = gathered
            .iter()
            .find(|m| m.name() == "mz_mcp_requests_total")
            .expect("requests metric present");
        assert_eq!(requests.get_metric().len(), 2);

        // tool_calls_total: 2 distinct label sets (one for each status).
        let tool_calls = gathered
            .iter()
            .find(|m| m.name() == "mz_mcp_tool_calls_total")
            .expect("tool_calls metric present");
        assert_eq!(tool_calls.get_metric().len(), 2);

        // tool_call_duration_seconds: one observation in one bucket set.
        let duration = gathered
            .iter()
            .find(|m| m.name() == "mz_mcp_tool_call_duration_seconds")
            .expect("tool_call_duration metric present");
        assert_eq!(
            duration.get_metric()[0].get_histogram().get_sample_count(),
            1
        );
    }
}
