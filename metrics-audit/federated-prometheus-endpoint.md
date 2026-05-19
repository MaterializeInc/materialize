# Federated `/metrics` endpoint with catalog-driven name labels

## Context

Today Prometheus metrics across environmentd and the N clusterd processes live in separate scrape targets and carry only opaque IDs (`instance_id`, `replica_id`, `collection_id`, `source_id`, `sink_id`, etc.). Customers have no way to correlate a spiking metric with the cluster, replica, or object they know by name. `metrics-audit/proposed-metrics.md` calls this out as a top-level goal: "create a single prometheus endpoint, proxy all processes' metrics through environmentd … such that we can add object/cluster/replica names to each series."

This plan designs a new federated endpoint on environmentd that scrapes its local registry + every clusterd replica in parallel, and — using an on-demand catalog snapshot — augments every metric's labels with `cluster_name`, `replica_name`, and `object_name` derived from the IDs already present. Existing `/metrics` endpoints on environmentd and clusterd stay intact for backwards compat; this is a new, opt-in scrape target.

## Shape of the solution

Augment, don't replace: every metric keeps its `instance_id` / `replica_id` / `{collection,source,sink,id}` label and gains a sibling name label. This preserves all existing dashboards and lets customers gradually migrate queries.

Names come from three catalog lookups — `cluster_id → cluster_name`, `(cluster_id, replica_id) → replica_name`, `GlobalId → object_name` — exactly matching the user's framing.

The enrichment is a pure function of `(Vec<MetricFamily>, CatalogSnapshot)`, so it's trivial to unit-test.

## Key interfaces

```rust
// src/environmentd/src/metrics/federated.rs (new)

/// Captures the id→name lookups needed to enrich metric labels.
/// Built from a single `CatalogSnapshot`; read-only after construction.
pub struct NameResolver {
    clusters: BTreeMap<ClusterId, String>,
    replicas: BTreeMap<(ClusterId, ReplicaId), String>,
    objects:  BTreeMap<GlobalId, String>,
}

impl NameResolver {
    pub fn from_catalog(catalog: &CatalogState) -> Self { /* walk clusters/replicas/items */ }

    /// Table-driven: when label X is present, add label Y sourced from this resolver.
    /// Returns a description of every label the enricher can add, so we keep one
    /// source of truth for which ID labels get names attached.
    pub fn enrichment_rules(&self) -> &'static [EnrichmentRule] { &ENRICHMENT_RULES }
}

/// Declarative table of id→name enrichment rules.
///
/// `trigger_labels` is treated as a *set*, not a tuple: the rule fires when every
/// listed label is present on the metric. Order of declaration is irrelevant —
/// `lookup` reads labels by name off the `Metric`, never by position. This means
/// a rule author can list trigger labels in any order without silently breaking
/// the lookup.
pub struct EnrichmentRule {
    pub trigger_labels: &'static [&'static str],  // e.g. ["instance_id"], or ["instance_id","replica_id"]
    pub added_label:    &'static str,             // e.g. "cluster_name"
    pub lookup:         fn(&NameResolver, &Metric) -> Option<String>,
}

/// Returns a new Vec<MetricFamily> with name labels added in place.
pub fn enrich(families: &mut Vec<MetricFamily>, resolver: &NameResolver) { /* … */ }
```

The enrichment table is the single place that encodes "when you see `source_id`, look it up as an object". Storage-side metrics using `source_id` / `sink_id` / `shard_id` get their own rules; compute-side `collection_id` gets another. Adding a new ID label later is a one-line table edit.

### Concrete rule table

Each row says "when this set of trigger labels is present on a metric, add this new label, sourced from this lookup." `trigger_labels` is the presence gate (all must be on the metric for the rule to fire); the `lookup` then reads each label it cares about **by name** off the `Metric` and returns the name to emit, or `None` if unknown (in which case nothing is added — the ID label survives alone). No reliance on declaration order.

```rust
static ENRICHMENT_RULES: &[EnrichmentRule] = &[
    // Cluster name whenever instance_id is present.
    EnrichmentRule {
        trigger_labels: &["instance_id"],
        added_label:    "cluster_name",
        lookup:         lookup_cluster_name,
    },
    // Replica name whenever both instance_id and replica_id are present.
    EnrichmentRule {
        trigger_labels: &["instance_id", "replica_id"],
        added_label:    "replica_name",
        lookup:         lookup_replica_name,
    },
    // Compute controller collection metrics (mz_dataflow_wallclock_lag_seconds, etc.).
    EnrichmentRule {
        trigger_labels: &["collection_id"],
        added_label:    "object_name",
        lookup:         lookup_object_name,
    },
    // Storage source metrics (mz_source_messages_received, mz_source_bytes_received, …).
    EnrichmentRule {
        trigger_labels: &["source_id"],
        added_label:    "source_name",
        lookup:         lookup_object_name,
    },
    EnrichmentRule {
        trigger_labels: &["parent_source_id"],
        added_label:    "parent_source_name",
        lookup:         lookup_object_name,
    },
    // Storage sink metrics (mz_sink_messages_staged, mz_sink_bytes_committed, …).
    EnrichmentRule {
        trigger_labels: &["sink_id"],
        added_label:    "sink_name",
        lookup:         lookup_object_name,
    },
];

// Each lookup reads the labels it needs by name. No positional coupling with
// `trigger_labels` — reordering a rule's trigger_labels can't break the lookup.

fn lookup_cluster_name(r: &NameResolver, m: &Metric) -> Option<String> {
    let id: ClusterId = get_label(m, "instance_id")?.parse().ok()?;
    r.clusters.get(&id).cloned()
}

fn lookup_replica_name(r: &NameResolver, m: &Metric) -> Option<String> {
    let cluster: ClusterId = get_label(m, "instance_id")?.parse().ok()?;
    let replica: ReplicaId = get_label(m, "replica_id")?.parse().ok()?;
    r.replicas.get(&(cluster, replica)).cloned()
}

// Generator over the trigger label's name so the same helper works for source_id,
// parent_source_id, sink_id, collection_id, etc.
fn lookup_object_by(label: &'static str)
    -> impl Fn(&NameResolver, &Metric) -> Option<String>
{
    move |r, m| {
        let id: GlobalId = get_label(m, label)?.parse().ok()?;
        r.objects.get(&id).cloned()
    }
}
```

### Applying the rules

A single pass over `Vec<MetricFamily>` → `Vec<Metric>` → `Vec<LabelPair>`. The helper `get_label` is a linear scan of the label vector (~5 labels typical, so not worth indexing):

```rust
pub fn enrich(families: &mut [MetricFamily], resolver: &NameResolver) {
    for family in families {
        for metric in family.mut_metric() {
            for rule in ENRICHMENT_RULES {
                // Skip if this metric doesn't match the trigger set.
                let values: Option<Vec<&str>> = rule
                    .trigger_labels
                    .iter()
                    .map(|name| get_label(metric, name))
                    .collect();
                let Some(values) = values else { continue };

                // Skip if the name is already present (idempotent; lets clusterd
                // or tests pre-populate names without breaking enrichment).
                if get_label(metric, rule.added_label).is_some() {
                    continue;
                }

                // Skip if the catalog doesn't know the ID (was dropped, or the
                // metric arrived before the catalog entry did).
                let Some(name) = (rule.lookup)(resolver, &values) else { continue };

                let mut pair = LabelPair::new();
                pair.set_name(rule.added_label.to_string());
                pair.set_value(name);
                metric.mut_label().push(pair);
            }
        }
    }
}

fn get_label<'a>(metric: &'a Metric, name: &str) -> Option<&'a str> {
    metric
        .get_label()
        .iter()
        .find(|lp| lp.get_name() == name)
        .map(|lp| lp.get_value())
}
```

### Worked example

Catalog snapshot:
- Cluster `u1` named `analytics`, with replica `3` named `r1`.
- Object `u25` named `events_raw` (a source); object `u24` named `events` (its parent).

Scrape input (concatenated from env + two clusterds):
```
mz_compute_controller_replica_count{instance_id="u1"} 2
mz_compute_commands_total{instance_id="u1",replica_id="3",command_type="peek"} 42
mz_source_messages_received{source_id="u25",worker_id="0",parent_source_id="u24"} 1000
mz_sink_messages_staged{sink_id="u99",worker_id="0"} 7    # u99 not in catalog (was dropped)
```

After `enrich`:
```
mz_compute_controller_replica_count{instance_id="u1",cluster_name="analytics"} 2
mz_compute_commands_total{instance_id="u1",replica_id="3",command_type="peek",cluster_name="analytics",replica_name="r1"} 42
mz_source_messages_received{source_id="u25",worker_id="0",parent_source_id="u24",source_name="events_raw",parent_source_name="events"} 1000
mz_sink_messages_staged{sink_id="u99",worker_id="0"} 7    # unchanged — ID survives, no fake name
```

Three properties this shows: (1) rules stack — one metric can pick up multiple name labels in a single pass; (2) an unknown ID leaves the metric alone rather than emitting empty-string names; (3) metrics that carry no trigger label (e.g. plain `mz_persist_*` counters) pass through untouched.

### Handler wiring

```rust
pub async fn handle_federated_metrics(
    State(state): State<FederatedState>,
) -> impl IntoResponse {
    let catalog = state.client.catalog_snapshot("federated_metrics").await;
    let resolver = NameResolver::from_catalog(&catalog);

    let mut families = state.registry.gather();

    // Target list comes from the locator, not the catalog: the locator is the
    // authoritative "what's actually reachable right now" map (updated by the
    // controller on provision/drop), and it already stores every live replica's
    // per-process HTTP addresses. This avoids a race where the catalog lists a
    // replica whose addresses haven't registered yet, or vice versa.
    let targets = state.locator.all_replicas(); // Vec<(ClusterId, ReplicaId, Vec<HttpAddr>)>
    families.extend(scrape_all(targets, state.scrape_timeout).await);

    enrich(&mut families, &resolver);
    encode_response(families)
}
```

Unit tests drop in easily: build a `NameResolver` with a hand-written `BTreeMap`, construct a `Vec<MetricFamily>` literally, call `enrich`, assert on labels. No HTTP, no catalog, no clusterd.

```rust
// Federated scrape handler — new route on environmentd's internal HTTP server.
pub async fn handle_federated_metrics(
    State(state): State<FederatedState>,
) -> impl IntoResponse {
    // 1. Catalog snapshot — one per scrape.
    let catalog = state.client.catalog_snapshot("federated_metrics").await;
    let resolver = NameResolver::from_catalog(&catalog);

    // 2. Local env registry.
    let mut families = state.registry.gather();

    // 3. Scrape each replica's clusterd /metrics in parallel (bounded timeout, partial failure OK).
    //    Target list comes from the locator (not a catalog walk) — see above.
    let targets = state.locator.all_replicas();
    let clusterd_families = scrape_all(targets, state.scrape_timeout).await;
    families.extend(clusterd_families);

    // 4. Enrich.
    enrich(&mut families, &resolver);

    // 5. Encode (text or openmetrics based on Accept header) and return.
    encode_response(families)
}
```

Reused existing types:
- `mz_controller::ReplicaHttpLocator` (defined at `src/controller/src/replica_http_locator.rs:24`; already powering `/api/cluster/{cluster_id}/replica/{replica_id}/process/{process}/metrics` at `src/environmentd/src/http/cluster.rs:29`) — gives us `(ClusterId, ReplicaId, process) → HTTP addr`. Its inner `BTreeMap<(ClusterId, ReplicaId), Vec<String>>` is the authoritative live-replica registry (updated by the controller on provision/drop). We need to add one small public accessor to enumerate it — see below.
- `AuthedClient::catalog_snapshot(name)` pattern (`src/environmentd/src/http/cluster.rs:237`, `src/environmentd/src/http/sql.rs:234`, `src/environmentd/src/http/mcp.rs:446`) — standard on-demand catalog read.
- `mz_ore::metrics::MetricsRegistry::gather` (`src/ore/src/metrics.rs:262`) — returns `Vec<MetricFamily>` we can mutate before encoding.
- `prometheus::TextEncoder` — same encoder used today by `mz_http_util::handle_prometheus` at `src/http-util/src/lib.rs:118`.

## Critical files

- **New**: `src/environmentd/src/metrics/federated.rs` — `NameResolver`, `EnrichmentRule`, `enrich`, scrape orchestrator, handler.
- **New**: `src/environmentd/src/metrics/mod.rs` if the module doesn't already exist; wire it into `src/environmentd/src/lib.rs`.
- **Modify**: `src/environmentd/src/http.rs` — add route `/metrics/federated` (new) that dispatches to `handle_federated_metrics`. Leave `/metrics` alone.
- **Modify**: `src/environmentd/src/lib.rs` — thread `ReplicaHttpLocator` and `MetricsRegistry` into the new handler's state; the locator already flows through `ClusterProxyConfig` (`src/environmentd/src/http/cluster.rs:41-50`).
- **Modify**: `src/controller/src/replica_http_locator.rs` — add a small public enumeration accessor, since today only point-lookup (`get_http_addr`) is exposed. Something like:
  ```rust
  /// Returns every registered replica with its per-process HTTP addresses.
  pub fn all_replicas(&self) -> Vec<(ClusterId, ReplicaId, Vec<String>)> {
      let guard = self.replica_addresses.read().expect("lock poisoned");
      guard.iter().map(|((c, r), a)| (*c, *r, a.clone())).collect()
  }
  ```
  Snapshotting into a `Vec` (rather than returning a guard) keeps the lock held only for the copy and matches the one-shot nature of a scrape.
- **Dependency**: add `prometheus-parse` (or similar) to `[workspace.dependencies]` in the root `Cargo.toml` to parse each clusterd's text-format response into `Vec<MetricFamily>`. Alternative: content-negotiate protobuf on clusterd (set `Accept: application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily`) and avoid parsing — but verify the `prometheus` crate's HTTP handler serves protobuf first; if not, parse the text format.
- **Unchanged**: clusterd's own `/metrics` endpoint (`src/clusterd/src/lib.rs:244-336`) — federated scraping targets the same route each clusterd already exposes.

## Design decisions (baked in)

1. **Augment, don't rewrite**. Add `cluster_name` alongside `instance_id` rather than replacing it. Preserves dashboards; supports gradual migration; handles ID-only metrics emitted before a catalog entry is cached.
2. **On-demand catalog snapshot per scrape**. Matches the SQL-based `/metrics/mz_*` pattern. Bounded by Prometheus's scrape_timeout (default 10s). No background refresh loop to maintain.
3. **Clusterd scraped in parallel with bounded timeout**. If a replica is unreachable, emit a `mz_federated_scrape_errors_total{cluster_id,replica_id,reason}` counter and keep going — partial results beat 500s for a scrape endpoint.
4. **Separate route, not overlay**. Mount at `/metrics/federated` (or similar) so existing Prometheus configs don't silently change shape. Migration becomes a scrape-config swap.
5. **Declarative enrichment table** so storage/compute owners adding new ID labels update one file.

## Open design questions (not blockers; resolve in implementation PR)

- **Object-label name collisions**: if a metric has `source_id` *and* `shard_id` (e.g. some storage stats), should we emit one `object_name` or disambiguate (`source_name`, `shard_name`)? Leaning: one `object_name` per rule, with separate rules per trigger label — so a metric with both gets both names.
- **Rename behavior**: renaming a cluster/replica/object starts a new series under the new name; the old series goes stale and ages out under Prometheus's retention window. This is standard Prometheus behavior (same as `kube_pod_info`) and is additive, O(renames) — not a cardinality concern under normal usage. `rate()` and `last_over_time(...[5m])` ignore stale series naturally, so dashboards Just Work. Worth a one-line note in docs; no mitigation required.
- **Auth on the new endpoint**: reuse whatever `/metrics` uses today. If it's unauthenticated, federated should be too; if it's behind auth, so is federated.
- **Protobuf vs text parsing on clusterd**: measure. If parsing text at scrape time is <50ms per clusterd, it's fine; otherwise negotiate protobuf.

## Verification

- **Unit**: table-test `NameResolver` + `enrich` over hand-built `MetricFamily` fixtures. Cover: unknown ID (no added label), rename (two series), object referenced by multiple label names.
- **Integration (mzcompose)**: spin up env + ≥2 clusterd replicas, hit `/metrics/federated`, parse the text format, assert every `mz_compute_*` metric with `instance_id` has a sibling `cluster_name`, and every clusterd-emitted `mz_source_*` metric has `source_name` + `cluster_name`.
- **Partial-failure test**: kill one clusterd, scrape, confirm response is 200 with metrics from surviving replicas plus a non-zero `mz_federated_scrape_errors_total`.
- **Load sanity**: hit `/metrics/federated` repeatedly (e.g. 5s interval) against a stack with ~20 replicas and ≥100 objects; make sure scrape latency stays under a few seconds and no unbounded growth in env's heap.
- **Existing endpoints unchanged**: curl the original `/metrics` and a clusterd `/metrics` and diff against pre-change output — must be byte-identical.
