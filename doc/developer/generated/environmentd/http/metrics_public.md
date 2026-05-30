---
source: src/environmentd/src/http/metrics_public.rs
revision: 8b77a4950e
---

# environmentd::http::metrics_public

Implements the customer-facing federated `/metrics` endpoint (`handle_public_metrics`), which aggregates environmentd's local Prometheus metrics with those scraped from every clusterd replica process.

The endpoint is gated by the `ENABLE_PUBLIC_METRICS_ENDPOINT` dynamic configuration flag; requests are rejected with `503 Service Unavailable` when the flag is off.

## Aggregation flow

1. Gather environmentd's local metrics via `MetricsRegistry::gather()`.
2. Fan out concurrently (`join_all`) to every `(cluster_id, replica_id, process_index, address)` tuple enumerated from `ClusterProxyConfig`'s replica locator. Each replica is scraped at its `/metrics` endpoint using the Prometheus protobuf content type.
3. Decode each successful response with `mz_prometheus_protobuf::decode_length_delimited`. Failed scrapes or failed decodes emit a `tracing::warn` and are skipped.
4. Attach replica-identifying labels to each scraped `MetricFamily` via `add_replica_labels`: `cluster_id`, `replica_id`, `process` are always added; `cluster_name` and `replica_name` are added when the catalog lookup succeeds.
5. Merge all collected families (environmentd-local and replica-sourced) by metric name via `merge_families_by_name`, which collapses duplicate `MetricFamily` entries sharing a name into one, retaining the first occurrence's help text and type declaration. This prevents Prometheus text-format scrapers from rejecting the response due to duplicate `# HELP`/`# TYPE` header lines, which arise because environmentd and clusterd register many metrics under the same names.
6. Encode the merged families as Prometheus text format and return with a `text/plain` content type.
