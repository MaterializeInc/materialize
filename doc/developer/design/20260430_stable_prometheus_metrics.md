# Materialize User-facing Prometheus Metrics Endpoint

- Associated:
  - PRD: https://www.notion.so/materialize/Improved-Prometheus-Metrics-2cb13f48d37b80a3bcbdcd93a1e3e5fd#31b13f48d37b8003b4dbd5a8b6d17449
  - Original metrics audit: https://github.com/SangJunBak/materialize/blob/823b5c9594ec94d7e2579de305a5c79032f66102/metrics-audit/proposed-metrics.md
  - Federated endpoint MVP: https://github.com/MaterializeInc/materialize/pull/36303

## The Problem

Self-managed customers and cloud customers need to ingest Materialize metrics into their native observability stack, but suffer from the following problems:

1. **No single scrape target.** Metrics live on environmentd and on every clusterd. Cloud users only have access to envd, and self-managed users have to discover and scrape every clusterd separately.
2. **Opaque labels.** Materialize Prometheus metrics carry only opaque IDs in their labels — `instance_id`, `replica_id`, `source_id`, `sink_id`, `collection_id`, `parent_source_id`, etc. Users querying these metrics would need another metric that maps ids to names and join against it
3. **No stability contract.** Once a metric is named in a customer dashboard or alert, renaming or removing it breaks them. But marking a metric stable also locks in the underlying implementation, so we need to be deliberate about which metrics we promise.
4. **Extraneous infrastructure for PromSQL**
Our current recommendation is installing a promsql exporter and querying off our system tables. This is also what we do in Cloud as well. However this requires additional infrastructure on the user's end and relies on each cluster + environmentd to be responsive.

## Success Criteria

1. A single Prometheus-compatible HTTP endpoint on environmentd returns metrics from envd + every clusterd in one scrape, with human-readable names attached to every label that today carries an opaque ID. PromQL queries written against this endpoint can group by `cluster_name`, `source_name`, etc. directly.
2. The enrichment mechanism is **declarative**: a metric's owner declares once which of its labels are ID-bearing, and the federated endpoint enriches them automatically. No per-metric handler code, no central rule table that must be edited every time a metric is added.
3. A small, curated subset of metrics is marked **Stable**, meaning we promise their name, type, and label set will not change without a deprecation of one major release. The remaining metrics are exposed but explicitly **Internal** (subject to change between releases). We allow a user to filter on stability through a query parameter in this endpoint.
4. The default `/metrics` route on every process remains unchanged.
5. Implementation reuses existing `MetricsRegistry` infrastructure (`src/ore/src/metrics.rs`) and `prometheus` crate APIs

## Out of Scope
- **Replacing our prometheus SQL exporter in cloud** We want to do this eventually
- **Replacing the existing `/metrics` route.** External Prometheus servers scraping clusterd directly continue to work unchanged.
- **Cardinality reduction.** Adding name labels widens series cardinality slightly. We accept that as a cost of usability and bound it by the catalog's natural object count.
- **Alerts and dashboards.** Cloud dashboards/alerts to consume the new federated endpoint is a future effort.
- **Stabilizing the long tail of metrics.** This proposal stabilizes a deliberately small initial set; broader stabilization happens organically as metrics prove out.

## Solution Proposal

Two composable pieces:

A. **A federated `/metrics` endpoint on environmentd** that scrapes envd locally and every clusterd in parallel, attaching catalog-derived names automatically. Driven by registry-level enrichment rules that each metric's owner declares once.

B. **A lifecycle annotation on the `metric!` macro** that classifies each metric as `Internal`, `Stable`, or `Deprecated`. The federated endpoint serves all of them; the lifecycle determines our compatibility promise.

This is largely inspired by CockroachDB's separation of [Essential metrics](https://www.cockroachlabs.com/docs/v25.3/essential-metrics-advanced) ([metrics docs](https://www.cockroachlabs.com/docs/v26.2/metrics.html)), where they don't promise stability on their larger set; ClickHouse's [Prometheus integration](https://clickhouse.com/docs/integrations/prometheus) and `filtered_metrics` parameter; and [Kubernetes' stability tiers](https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/).

### A. Federated endpoint with name-bearing labels

#### Overview

1. **Registry-level enrichment rules.** Each crate's metrics module declares which label names are ID-bearing and which catalog accessor resolves them:
   ```rust
   registry.register_rule(Rule::ObjectNameLookup { object_id_label: "collection_id".into(), output_label: "collection_name".into() });
   registry.register_rule(Rule::ObjectNameLookup { object_id_label: "source_id".into(), output_label: "source_name".into() });
   ```
   Rules live on the `MetricsRegistry` and are serialized to JSON in the `X-Mz-Enrich-Rules` HTTP response header on every `/metrics` scrape. Prometheus servers ignore unknown response headers; only Materialize's federated handler reads them.
1. **`/metrics/federated` route on env.** Scrapes envd locally and every clusterd in parallel, requesting `Accept: application/vnd.google.protobuf` for compactness and zero-cost decode. We'll have an initial connection timeout of 5 seconds and a request timeout of 60 seconds where failures will lead to partial metrics.

1. **Cluster and replica labels attached at scrape time.** When env scrapes each clusterd pod via [`ReplicaHttpLocator`](src/controller/src/replica_http_locator.rs), it already knows the `cluster_id` and `replica_id` of the target, so it stamps those labels onto every series in the response rather than requiring clusterd to emit them itself.

#### Wire format

| | `/metrics` (default Accept) | `/metrics` + `Accept: protobuf` | env's `/metrics/federated` |
|---|---|---|---|
| Caller | external Prometheus, curl | env's federated scraper, Prometheus 2.x | external Prometheus |
| Body | vanilla Prometheus text | vanilla delimited Prometheus protobuf | text, enriched with names |
| Headers | standard + `X-Mz-Enrich-Rules` | standard + `X-Mz-Enrich-Rules` | standard |

External clients always get the rules header alongside their data; they ignore unknown headers, so behavior is unchanged for them.

#### `Rule` enum (in `src/ore/src/metrics.rs`)

Each variant carries the label names it consumes — the rule is fully self-describing on the wire.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Rule {
    /// Looks up the cluster via
    /// `catalog.try_get_cluster` and attaches `cluster_name`.
    ClusterNameLookup { cluster_id_label: String, output_label: String },
    /// Looks up the replica via `catalog.try_get_cluster_replica`
    /// and attaches `cluster_name` and `replica_name`.
    ReplicaNameLookup { cluster_id_label: String, replica_id_label: String, output_label: String },
    /// Resolve via `catalog.try_get_entry_by_global_id`.
    /// and attaches `object_name`.
    ObjectNameLookup { object_id_label: String, output_label: String  },
}
```

#### Authentication

We will be exposing this endpoint in our external ports in both Cloud and Self Managed and reuse the authentication method configured on these ports. For Cloud, this would be using `AuthenticatorKind::Frontegg`.


### B. Stability tiers

A small, curated subset of metrics is marked Stable; the rest stay Internal — exposed but explicitly not promised. The mechanism is a lifecycle annotation on the `metric!` macro:

```rust
enum Lifecycle {
    Internal,                       // default for current metrics
    Stable,
    Deprecated { version: String }, // version where deprecation began;
                                    // help message auto-prefixes "DEPRECATED at vX"
}
```

Policies:

- Every metric removal must be called out in the release notes. We can generate an HTML manifest of the current metrics from the `metric!` macro using the existing `walkabout` crate; the release-notes skill is updated to look at diffs of that file.
- A Stable metric marked `Deprecated` stays alive through one major release before removal. (Common practice per @Alphadelta14.)
- A lint enforces that Stable metrics cannot be removed directly — they have to transition through `Deprecated { version }` first. The lint reads from the generated manifest.
- Deprecation is encoded on the wire by appending `(DEPRECATED at version X)` to the metric's HELP text, which is visible to anyone scraping.

#### Initial Stable set

The initial set is deliberately small — only metrics we feel confident about today, with a clear path to expose all of them through the federated endpoint. New metrics promote into Stable organically as they prove out.

**Replica utilization**
- `mz_memory_limiter_memory_usage_bytes` — memory + swap
- `mz_memory_limiter_memory_limit_bytes` — memory + swap
- Prometheus-native equivalents of:
  - `cpu_nano_cores` (from `mz_cluster_replica_metrics`)
  - CPU limit
  - `memory_bytes` (no swap) (from `mz_cluster_replica_metrics`)
  - Memory limit (no swap)

**Compute controller**
- `mz_compute_controller_subscribe_count`
- `mz_dataflow_wallclock_lag_seconds` (and `_sum` / `_count`)

**Dataflow**
- `mz_compute_collection_count`
- Prometheus-native equivalents of these dataflow SQL exporter queries (`src/environmentd/src/http/prometheus.rs`):
  - `mz_arrangement_size_bytes`
  - `mz_compute_replica_park_duration_seconds_total`
  - `mz_dataflow_elapsed_seconds_total`
  - `mz_compute_hydration_time_seconds`
    Not in `prometheus.rs`:
    ```sql
    SELECT
      object_id AS collection_id,
      (coalesce(time_ns, 0) / 1000000000)::float8 AS time_s,
      (time_ns IS NOT NULL)::int as hydrated,
      cluster_id AS instance_id,
      replica_id AS replica_id
    FROM mz_internal.mz_compute_hydration_times
    JOIN mz_cluster_replicas ON (id = replica_id)
    ```

**Source**
- `mz_source_bytes_received`
- `mz_source_updates_staged_total`
- `mz_source_updates_committed_total`
- `mz_source_snapshot_committed`
- `mz_source_snapshot_records_known`
- `mz_source_snapshot_records_staged`
- `mz_source_offset_known`
- `mz_source_offset_committed`
- `mz_source_rehydration_latency_ms`
- `mz_source_messages_received`
- `mz_source_error_inserts`
- `mz_source_error_retractions`

**Sink**
- `mz_sink_messages_staged`
- `mz_sink_messages_committed`
- `mz_sink_bytes_staged`
- `mz_sink_bytes_committed`

**Adapter**
- `mz_query_total`
- `mz_catalog_transact_seconds`
- `mz_catalog_collection_entries`
- `mz_active_sessions`

## Minimal Viable Prototype

A working prototype of the federated endpoint exists at https://github.com/MaterializeInc/materialize/pull/36303. The lifecycle annotation can ride the same wire mechanism (HTTP headers) as the enrichment rules.

## Alternatives

### A. Name mapping as separate metric series

**Why not chosen:** storing the names would require clients to do a PromQL join which is non-ergonomic and also wouldn't be compatible with datadog.

## Open questions
- **Which extra labels to attach in `ObjectNameLookup`.** `object_name` Should we also add an object's schema name and database name?
- **`catalog_snapshot` cost on every federated request.** Returns `Arc<Catalog>` so the read itself is cheap, but the call is async and goes through the coordinator. If the federated route ends up scraped every 5–15s that's ~one extra `CatalogSnapshot` command per scrape per caller. Should be fine but worth measuring against `mz_adapter_catalog_snapshot_seconds`.
