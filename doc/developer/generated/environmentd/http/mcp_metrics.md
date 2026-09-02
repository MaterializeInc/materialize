---
source: src/environmentd/src/http/mcp_metrics.rs
revision: 2736da1635
---

# environmentd::http::mcp_metrics

Defines Prometheus metrics for the MCP HTTP endpoints and the RAII guard used to record per-tool-call outcomes.

## Metrics

`McpMetrics` holds three Prometheus collectors, all registered via `MetricsRegistry`:

* `mz_mcp_requests_total` (`IntCounterVec`) — total MCP requests, labeled by `endpoint_type` (`agent` or `developer`), `method` (JSON-RPC method name), and `status` (a `McpCallStatus` variant: `ok`, `cancelled`, `timeout`, `endpoint_disabled`, or an `McpRequestError::error_type()` string such as `ToolNotFound`).
* `mz_mcp_tool_calls_total` (`IntCounterVec`) — total `tools/call` invocations, labeled by `endpoint_type`, `tool_name`, and `status`.
* `mz_mcp_tool_call_duration_seconds` (`HistogramVec`) — duration of `tools/call` invocations, labeled by `endpoint_type` and `tool_name`, using `histogram_seconds_buckets(0.000_128, 8.0)`.

`McpMetrics` implements `Clone`; Prometheus collector handles are `Arc`-shared internally, so cloning is cheap. The struct is stored and passed as an Axum `Extension`.

## McpCallStatus

`McpCallStatus` is a closed enum of outcomes recorded in the MCP `status` metric label. Using an enum rather than free-form strings at call sites pins the label cardinality and prevents typos from silently creating new label values. Variants: `Ok`, `Cancelled`, `Timeout`, `EndpointDisabled`, and `Error(&'static str)` (carrying the `McpRequestError::error_type()` string). `as_str()` converts each variant to its wire label string.

## ToolCallGuard

`ToolCallGuard` is an RAII guard for a single `tools/call` invocation. On construction it starts a `HistogramTimer` for `mz_mcp_tool_call_duration_seconds`. On drop it increments `mz_mcp_tool_calls_total` with the current `McpCallStatus`. The default status is `McpCallStatus::Cancelled`, so if the surrounding future is dropped before completion (e.g. by `tokio::time::timeout`), the metric records as cancelled rather than being silently lost. Callers set the status to the actual outcome via `set_status` before the guard goes out of scope. `McpMetrics::record_request` encapsulates the label ordering and `McpCallStatus` to `&str` conversion so call sites never touch raw label strings.
