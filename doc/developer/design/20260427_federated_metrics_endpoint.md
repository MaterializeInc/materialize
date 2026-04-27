# Federated `/metrics` endpoint with name-bearing labels

- Associated:
Product spec:
https://www.notion.so/materialize/Improved-Prometheus-Metrics-2cb13f48d37b80a3bcbdcd93a1e3e5fde

## The Problem

Materialize Prometheus metrics carry only opaque IDs in their labels —
`instance_id`, `replica_id`, `source_id`, `sink_id`, `collection_id`,
`parent_source_id`, etc. Users querying these metrics see IDs like
`source_id="u25"` and have no way to correlate them with the names they
actually know (`source_name="events_raw"`, `cluster_name="analytics"`).

Names are resolvable on environmentd, which holds the catalog. However, metrics scraped
from clusterd cannot resolve names. Additionally, Cloud users should only scrape a single endpoint
and shouldn't have knowledge of clusterd metric endpoints.

## Success Criteria

1. A single Prometheus-compatible HTTP endpoint on environmentd returns metrics with
   human-readable names attached to every label that today carries an opaque
   ID. PromQL queries written against the federated endpoint can group by
   `cluster_name`, `source_name`, etc. directly.
2. The mechanism is **declarative**: a metric's owner declares once which of
   its labels are ID-bearing, and the federated endpoint enriches them
   automatically. No per-metric handler code, no central rule table that
   must be edited every time a metric is added.
3. The default `/metrics` route on every process remains unchanged
4. Implementation reuses existing `MetricsRegistry` infrastructure
   (`src/ore/src/metrics.rs`) and `prometheus` crate APIs — no new wire
   formats, no custom protocols.

## Out of Scope

- **Replacing the existing `/metrics` route.** External Prometheus servers
  scraping clusterd directly continue to work unchanged.
- **Cardinality reduction.** Adding name labels widens series cardinality
  slightly. We accept that as a cost of usability and bound it by the
  catalog's natural object count.
- **Alerts and dashboards.** Console and dashboard work to consume the
  new federated endpoint is a future effort.

## Solution Proposal

### Overview

Two composable pieces that build on stock Prometheus patterns:

1. **Registry-level enrichment rules.** Each crate's metrics module declares
   which label names are ID-bearing and which catalog accessor resolves them:
   ```rust
   registry.register_rule(Rule::ObjectNameLookup { object_id_label: "collection_id".into() });
   registry.register_rule(Rule::ObjectNameLookup { object_id_label: "source_id".into() });
   ```
   Rules live on the `MetricsRegistry` and are serialized to JSON in the
   `X-Mz-Enrich-Rules` HTTP response header on every `/metrics` scrape.
   Prometheus servers ignore unknown response headers; only Materialize's
   federated handler reads them.
2. **`/metrics/federated` route on env.** Scrapes envd locally and every
   clusterd in parallel, requesting `Accept: application/vnd.google.protobuf`
   for compactness and zero-cost decode. There are two sources of cluster
   and replica labels, both feeding the same pair of helper functions:

   - **For metrics scraped from a clusterd**, envd already knows which
     replica it dispatched the scrape to. The scrape loop stamps
     `cluster_id`, `replica_id`, `cluster_name`, and `replica_name` on
     every metric in that batch, looking up the names in `CatalogState`.
     No rule needed.
   - **For metrics emitted by envd itself that already carry
     `cluster_id` / `replica_id` labels** — e.g. `replica_connects_total`
     in `src/compute-client/src/metrics.rs` — the metric registry
     declares `Rule::ClusterNameLookup` and `Rule::ReplicaNameLookup`.
     The federated handler reads the IDs off the metric, then calls the
     same `attach_cluster_name` / `attach_cluster_replica_metadata`
     helpers the scrape loop uses.

   The remaining per-label ID→name lookups (where the label name varies
   — `collection_id`, `source_id`, `parent_source_id`) are driven by
   `Rule::ObjectNameLookup`, which reads each response's rule header and
   dispatches into `try_get_entry_by_global_id`. The final result is
   re-emitted as standard Prometheus text.

The federated handler's enrichment rules **come from the metrics responses
themselves** — clusterd publishes its rules via the JSON header, env consumes
them. No static rule table on env, and no separate info-metric series — env
already has the catalog in-process, so it reads names directly.

### Wire format

| | `/metrics` (default Accept) | `/metrics` + `Accept: protobuf` | env's `/metrics/federated` |
|---|---|---|---|
| Caller | external Prometheus, curl | env's federated scraper, Prometheus 2.x | external Prometheus |
| Body | vanilla Prometheus text | vanilla delimited Prometheus protobuf | text, enriched with names |
| Headers | standard + `X-Mz-Enrich-Rules` | standard + `X-Mz-Enrich-Rules` | standard |

External clients always get the rules header alongside their data; they
ignore unknown headers, so behavior is unchanged for them.

### `Rule` enum (in `src/ore/src/metrics.rs`)

Each variant carries the label names it consumes — the rule is fully
self-describing on the wire.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Rule {
    /// For metrics that already carry a cluster_id label (under whatever
    /// name `cluster_id_label` specifies). Looks up the cluster via
    /// `catalog.try_get_cluster` and attaches `cluster_name`.
    /// Used by env-side metrics like `replica_connects_total`.
    ClusterNameLookup { cluster_id_label: String },
    /// Same shape, for metrics carrying both cluster and replica IDs.
    /// Attaches `cluster_name` and `replica_name`.
    ReplicaNameLookup { cluster_id_label: String, replica_id_label: String },
    /// Resolve via `catalog.try_get_entry_by_global_id(metric[object_id_label])`.
    /// Attaches `object_name`. Used for `collection_id`, `source_id`,
    /// `parent_source_id`, etc.
    ObjectNameLookup { object_id_label: String },
}
```

Cluster and replica rules are still useful even though the scrape loop
already attaches those labels for clusterd-scraped metrics: env-emitted
metrics never go through the scrape loop, so they need a rule to opt
in. Both code paths reuse the same helpers — see the federated handler
below.

`MetricsRegistry` gains `rules: Arc<Mutex<Vec<Rule>>>` plus:
- `register_rule(rule: Rule)` — appends a rule.
- `rules_snapshot() -> Vec<Rule>` — clones the vec under the lock.

### Extended `handle_prometheus` (`src/http-util/src/lib.rs:118-125`)

```rust
pub async fn handle_prometheus(
    registry: &MetricsRegistry,
    accept: Option<TypedHeader<headers::Accept>>,
) -> impl IntoResponse + use<> {
    use prometheus::{Encoder, ProtobufEncoder, TextEncoder, PROTOBUF_FORMAT};

    let want_proto = accept
        .as_ref()
        .map(|a| a.0.iter().any(|m| m.essence_str() == "application/vnd.google.protobuf"))
        .unwrap_or(false);

    let families = registry.gather();
    let mut body = Vec::new();
    let body_ct = if want_proto {
        ProtobufEncoder::new().encode(&families, &mut body)?;
        PROTOBUF_FORMAT
    } else {
        TextEncoder::new().encode(&families, &mut body)?;
        "text/plain; version=0.0.4"
    };

    let rules_json = serde_json::to_string(&registry.rules_snapshot())?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(body_ct));
    headers.insert(
        HeaderName::from_static("x-mz-enrich-rules"),
        HeaderValue::from_str(&rules_json)?,
    );
    Ok((headers, body))
}
```

The handler is shared across env and clusterd, so this single change makes
every `/metrics` route on every process content-negotiate and emit the
rules header. Existing route registrations
(`src/clusterd/src/lib.rs:256-260`, `src/environmentd/src/http.rs:420-452`)
are unchanged.

### Federated handler (`src/environmentd/src/metrics/federated.rs`, new)

The shared helpers — `attach_cluster_name` and `attach_cluster_replica_metadata`
— do all the catalog lookups and label pushes. The scrape loop calls them
with address-derived IDs; the rule-driven `enrich` calls them with IDs read
off the metric's labels.

```rust
/// Catalog lookup for cluster name. Pushes cluster_id and cluster_name onto
/// the metric. Idempotent (uses push_label_if_missing).
fn attach_cluster_name(metric: &mut Metric, cluster_id: ClusterId, catalog: &CatalogState) {
    push_label_if_missing(metric, "cluster_id", &cluster_id.to_string());
    if let Some(c) = catalog.try_get_cluster(cluster_id) {
        push_label_if_missing(metric, "cluster_name", c.name());
    }
}

/// Full cluster + replica metadata. Calls attach_cluster_name then layers
/// replica_id / replica_name on top.
fn attach_cluster_replica_metadata(
    metric: &mut Metric,
    cluster_id: ClusterId,
    replica_id: ReplicaId,
    catalog: &CatalogState,
) {
    attach_cluster_name(metric, cluster_id, catalog);
    push_label_if_missing(metric, "replica_id", &replica_id.to_string());
    if let Some(r) = catalog.try_get_cluster_replica(cluster_id, replica_id) {
        push_label_if_missing(metric, "replica_name", &r.name);
    }
}

pub async fn handle_federated_metrics(State(s): State<FederatedState>) -> impl IntoResponse {
    // 1. Read-only catalog snapshot — same API used by HTTP SQL / MCP / cluster pages
    //    (src/environmentd/src/http/sql.rs:234, http/cluster.rs:237, http/mcp.rs:439).
    let catalog: Arc<Catalog> = s.adapter_client.catalog_snapshot("metrics_federated").await;
    let cs = catalog.state();

    // 2. Local gather — env's own metrics. Rules in env's registry will
    //    drive cluster/replica name lookup for env-emitted metrics that
    //    carry those IDs (e.g. replica_connects_total).
    let mut families = s.registry.gather();
    let mut all_rules = s.registry.rules_snapshot();

    // 3. Scrape every clusterd in parallel. For each batch, env knows the
    //    (cluster_id, replica_id) it dispatched to — call the same helper
    //    `enrich` would call when a rule matches.
    let scraped = scrape_all(&s.client, s.locator.all_replicas(), s.timeout).await;
    for mut replica in scraped {
        for family in &mut replica.families {
            for metric in family.mut_metric() {
                attach_cluster_replica_metadata(metric, replica.cluster_id, replica.replica_id, cs);
            }
        }
        families.extend(replica.families);
        all_rules.extend(replica.rules);
    }
    dedup_rules(&mut all_rules);

    // 4. Apply rules. Cluster/replica rules use the same helpers as the
    //    scrape loop; object-name rules dispatch to try_get_entry_by_global_id.
    enrich(&mut families, &all_rules, cs);

    // 5. Encode standard Prometheus text.
    encode_response(families)
}

async fn scrape_one(
    client: &reqwest::Client, cluster_id: ClusterId, replica_id: ReplicaId,
    addr: String, timeout: Duration,
) -> Result<ScrapedReplica, ScrapeError> {
    let resp = client.get(format!("http://{addr}/metrics"))
        .header(reqwest::header::ACCEPT, prometheus::PROTOBUF_FORMAT)
        .timeout(timeout).send().await?.error_for_status()?;

    let rules: Vec<Rule> = resp.headers().get("x-mz-enrich-rules")
        .map(|h| serde_json::from_slice(h.as_bytes())).transpose()?
        .unwrap_or_default();

    let bytes = resp.bytes().await?;
    let mut input = protobuf::CodedInputStream::from_bytes(&bytes);
    let mut families = vec![];
    while !input.eof()? {
        let len = input.read_raw_varint32()? as u64;
        let old_limit = input.push_limit(len)?;
        let mut family = MetricFamily::new();
        family.merge_from(&mut input)?;
        input.pop_limit(old_limit);
        families.push(family);
    }
    Ok(ScrapedReplica { families, rules, cluster_id, replica_id })
}

fn enrich(families: &mut [MetricFamily], rules: &[Rule], catalog: &CatalogState) {
    for family in families {
        for metric in family.mut_metric() {
            for rule in rules {
                match rule {
                    Rule::ClusterNameLookup { cluster_id_label } => {
                        let Some(s) = get_label(metric, cluster_id_label) else { continue };
                        let Ok(cluster_id) = s.parse::<ClusterId>() else { continue };
                        attach_cluster_name(metric, cluster_id, catalog);
                    }
                    Rule::ReplicaNameLookup { cluster_id_label, replica_id_label } => {
                        let (Some(c), Some(r)) =
                            (get_label(metric, cluster_id_label), get_label(metric, replica_id_label))
                            else { continue };
                        let (Ok(cluster_id), Ok(replica_id)) =
                            (c.parse::<ClusterId>(), r.parse::<ReplicaId>())
                            else { continue };
                        attach_cluster_replica_metadata(metric, cluster_id, replica_id, catalog);
                    }
                    Rule::ObjectNameLookup { object_id_label } => {
                        let Some(s) = get_label(metric, object_id_label) else { continue };
                        let Ok(global_id) = s.parse::<GlobalId>() else { continue };
                        if let Some(entry) = catalog.try_get_entry_by_global_id(&global_id) {
                            push_label_if_missing(metric, "object_name", entry.name().item.as_str());
                        }
                    }
                }
            }
        }
    }
}
```

`catalog_snapshot` is the standard read-only adapter API (returns
`Arc<Catalog>` from a snapshot taken under the coordinator's read lock).
The same call is already used by env's HTTP SQL handler, MCP handler, and
cluster proxy pages. Lookups are O(1) hash hits against `CatalogState`. An
unknown ID (e.g. an object that was just dropped) silently leaves the
metric unenriched — value metrics still serve.

### Cargo.toml change

`Cargo.toml:426`:
```toml
prometheus = { version = "0.14.0", default-features = false, features = ["protobuf"] }
```

Per `CLAUDE.md`'s dependency rules: change once in workspace deps, run
`cargo check` (not `cargo update`), diff `Cargo.lock` and pin back any
unintended bumps. Add rust-protobuf's license to `deny.toml` and `about.toml`
if not already present.

### Critical files

- **Modify** `src/ore/src/metrics.rs` — add the `Rule` enum, the `rules`
  field on `MetricsRegistry`, `register_rule`, `rules_snapshot`.
- **Modify** `src/http-util/src/lib.rs:118-125` — extend `handle_prometheus`
  with `Accept` content negotiation and the `X-Mz-Enrich-Rules` header.
- **Modify** `Cargo.toml`, `deny.toml`, `about.toml` — flip the `prometheus`
  feature; verify license.
- **New** `src/environmentd/src/metrics/federated.rs` — `handle_federated_metrics`,
  `scrape_one`, `scrape_all`, `attach_cluster_name`,
  `attach_cluster_replica_metadata`, `enrich`, `encode_response`.
- **New** `src/environmentd/src/metrics/mod.rs` — module wiring.
- **Modify** `src/controller/src/replica_http_locator.rs` — add
  `pub fn all_replicas(&self) -> Vec<(ClusterId, ReplicaId, Vec<String>)>`
  (snapshot the BTreeMap under read lock).
- **Modify** `src/environmentd/src/http.rs` — register `/metrics/federated`;
  thread `AdapterClient` (for `catalog_snapshot`), `MetricsRegistry`,
  `ReplicaHttpLocator`, and a shared `reqwest::Client` into the federated
  handler's state.
- **Modify** `src/environmentd/src/lib.rs`, `src/clusterd/src/lib.rs` — call
  `<crate>::register_rules(&registry)` during startup.
- **Modify** crate metrics modules (`src/compute/src/metrics.rs`,
  `src/storage/src/metrics/**`, `src/compute-client/src/metrics.rs`) — add
  `pub fn register_rules(registry: &MetricsRegistry)` declaring rules for
  ID-bearing labels in that crate. Long-tail rollout; not required for v1
  of the federated route.

## Minimal Viable Prototype

End-to-end demo on `mzcompose`:

1. Bring up env + 2 clusterd replicas of the same cluster. The scrape
   loop must attach distinct `replica_id` / `replica_name` labels per
   replica, otherwise their series collide.
2. `CREATE SOURCE events_raw FROM KAFKA ...` so a real `source_id` flows
   through the pipeline.
3. `register_rules` is called in compute and storage with the appropriate
   `Rule::ObjectNameLookup`, `Rule::ClusterNameLookup`,
   `Rule::ReplicaNameLookup` variants for the ID labels each crate emits.
4. `curl http://clusterd:port/metrics` — confirm body is text and
   `X-Mz-Enrich-Rules` header lists the registered rules as JSON.
5. `curl -H "Accept: application/vnd.google.protobuf; ..."
   http://clusterd:port/metrics | protoc --decode=io.prometheus.client.MetricFamily`
   — confirm protobuf body decodes and same rules header is present.
6. `curl http://env:port/metrics/federated` — confirm
   `mz_source_messages_received` carries `source_name="events_raw"`,
   `cluster_name`, `replica_name`, distinct series per replica.
7. Run a real Prometheus container against clusterd's `/metrics` — confirm
   it ingests cleanly with no warnings about the extra header (Prometheus
   ignores unknown response headers) and no extra time-series rows in the
   body. This is the regression we most need to avoid.

If steps 1–7 pass, the prototype is validated.

## Alternatives

### A. Rules as `mz_enrich_rule_info` metric series

The original federated-prometheus-endpoint note proposed encoding rules as a
regular metric family on each clusterd's `/metrics` body. Rules ride the
standard wire format as data; env reads them from the gathered families.

**Why not chosen:** rules are schema, not data. Mixing them into the metric
body complicates downstream tooling (label-rewrite rules, recording rules,
alerting templates) that may match on metric-name patterns.
`X-Mz-Enrich-Rules` keeps schema and data on separate channels at zero wire-
format cost.

### B. New `/internal/metrics-bundle` endpoint

A dedicated route serving protobuf body + JSON rules header (or a custom
envelope proto wrapping both). Cleaner separation; rule data fully invisible
to anyone who doesn't know the URL.

**Why not chosen:** stronger isolation than needed. Public Prometheus
servers ignore unknown response headers, so leaking the rules manifest on
the existing `/metrics` route causes no observable harm. A separate route
also doubles route registration boilerplate and complicates auth / network
ACL configuration.

### C. Per-metric `metric!{ added_labels: [...] }` declarations

Rules declared next to each metric definition rather than centrally on the
registry. Compile-time validation that referenced labels actually exist on
the metric.

**Why not chosen:** verbose. A label like `collection_id` appears on dozens
of metrics; declaring the same `Rule::ObjectNameLookup` mapping on each is
redundant. Registry-level declaration handles all instances of a label name
with one call. Validation moves to runtime (a label that no metric carries
is a no-op) but the simplification is worth it.

### D. Embed rules inside `MetricFamily` proto fields

Extend the Prometheus `client_model.proto` with an `mz_rules` field on
`MetricFamily`.

**Why not chosen:** impossible without forking the upstream Prometheus
client model. Forking would break compatibility with `prometheus::ProtobufEncoder`
on the public `/metrics` route — external Prometheus servers couldn't scrape
us. JSON header sidesteps this entirely.

### E. Info metrics on env (kube-state-metrics pattern)

Have env emit `mz_cluster_info`, `mz_source_info`, `mz_object_info`, etc. as
metric series whose value is `1` and whose labels carry `(id, name, schema,
database, type, ...)`. The federated handler then joins these into value
metrics by matching label keys, the same way kube-state-metrics works in
Kubernetes. An attractive side benefit: power users running their own
Prometheus stack against env's `/metrics` could write `label_replace`-style
joins or use the `info()` PromQL function themselves.

**Why not chosen:** redundant work. Env already has the catalog in-process —
hopping through info metrics means serializing the catalog into metric form,
re-parsing it during enrichment, and shipping the extra series in every env
scrape. Direct `CatalogState` dispatch from inside the federated handler is
both simpler and avoids permanently adding info-metric cardinality to env's
`/metrics` output. We can revisit publishing info metrics later if external
PromQL-side joining becomes a requirement.


## Open questions

- **Auth on `/metrics/federated`.** Match whatever `/metrics` uses today.
  Confirm no new auth surface is exposed.
- **Which extra labels to attach in `ObjectNameLookup`.** `object_name` is
  always useful. Adding `schema` and `database` widens cardinality (an MV
  renamed across schemas would produce two distinct series). Decide before
  enabling for the first metric.
- **`catalog_snapshot` cost on every federated request.** Returns
  `Arc<Catalog>` so the read itself is cheap, but the call is async and
  goes through the coordinator. If the federated route ends up scraped
  every 5–15s that's ~one extra `CatalogSnapshot` command per scrape per
  caller. Should be fine but worth measuring against
  `mz_adapter_catalog_snapshot_seconds`.
- **Header size guard.** At <50 rules per process the JSON is well under
  4KB. Add a runtime warning if `rules_json.len() > 6KB` and a hard fail
  at 14KB (typical 8KB header cap, leaves headroom for other headers).
- **Should the federated route strip the `X-Mz-Enrich-Rules` header from
  its own response?** Probably yes — by the time env serves the federated
  text, names are merged into labels and the rule manifest is no longer
  useful.
- **Phasing.** Two PRs land independently:
  1. Registry-level rules + protobuf feature flip + extended
     `handle_prometheus`. Self-contained; every `/metrics` route across all
     processes immediately starts emitting the rules header.
  2. `/metrics/federated` route on env. Builds on (1) plus the
     catalog-dispatch enrichment.

  Long-tail `register_rules` calls in metric-owning crates land
  organically; the federated route enriches more metrics as more rules are
  registered.
