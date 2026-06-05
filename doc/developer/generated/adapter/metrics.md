---
source: src/adapter/src/metrics.rs
revision: 967672afc3
---

# adapter::metrics

Registers and vends all Prometheus metrics for the adapter and coordinator.
`Metrics` is the top-level struct holding counters, gauges, and histograms covering query counts, active sessions, subscribe/COPY-TO activity, timestamp determination, statement logging, message handling latency, and more; `SessionMetrics` is a lightweight subset scoped to a single session.
The `mz_group_commit_table_advancement_seconds` histogram has been removed along with the O(n) table advancement loop it measured.
Helper functions `session_type_label_value`, `statement_type_label_value`, and `subscribe_output_label_value` produce the label strings used for partitioning these metrics.
