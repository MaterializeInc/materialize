# Runtime control of environmentd metric cardinality

- Associated: [DB-199](https://linear.app/materializeinc/issue/DB-199), [INC-1252](https://app.incident.io/materializeinc/incidents/1252), [#38616](https://github.com/MaterializeInc/materialize/pull/38616)

## The Problem

environmentd exposes one `/metrics` endpoint whose sample count is proportional
to catalog size: many families carry one series per catalog object, persist
shard, replica or cluster. The AMP scraper rejects any response over 50 MiB,
which is not configurable on our side. In INC-1252 the largest environment in
the fleet (about 14k catalog objects) returned about 58 MB and 450k samples
across 531 families, so every scrape failed with `body size limit exceeded`
and every environmentd metric for the environment disappeared for hours,
twice. Recovery was coincidental, when the sample count dipped back under the
limit. The environment is growing, so it will cross the limit again.

Until now there was no runtime control over which families or which scopes
environmentd exports. The only levers were a code change and a rollout.

The largest families on the affected endpoint, from the incident channel:

| Family | Samples | Labels |
|---|---|---|
| `mz_dataflow_wallclock_lag_seconds{,_sum,_count}` | 61,548 | instance_id, replica_id, collection_id |
| `mz_time_to_first_row_seconds` | 52,080 | instance_id, isolation_level, strategy, application_name |
| `mz_timestamp_difference_for_strict_serializable_ms` | 34,125 | compute_instance |
| `mz_compute_peek_duration_seconds` | 31,176 | instance_id, result |
| `mz_object_info` | 14,488 | one per catalog object |

Of the 173 labeled metric families in environmentd-side crates, 92 scale with
objects, replicas or clusters: 43 per persist shard (Persist), 3 per
collection per replica and 29 per replica or per cluster in the compute and
storage controllers (Cluster), 5 per catalog object and 2 per unbounded
`application_name` in the adapter (SQL).

## Success Criteria

- An operator can shed environmentd samples at runtime through `ALTER SYSTEM`,
  with no restart, and get well under the scrape limit.
- The shedding can be scoped: per-replica or per-object detail can be kept for
  named clusters or replicas while dropped elsewhere. Per-replica metrics are
  used daily to compare old and new generation replicas during rollouts and to
  track OOM-looping replicas, so all-or-nothing is not acceptable.
- Under growth, one family degrades at a time rather than the whole endpoint.
- Payload size and per-family sample counts are observable before the limit is
  reached.
- Every knob defaults to current behavior.

## Out of Scope

- What clusterd exposes. clusterd endpoints were not near the limit.
- Changes to the metrics pipeline (compression, remote-write batching). These
  are tracked on the Cloud side as INC-1252 follow-ups.
- Deciding which families to delete outright. That is a per-team audit;
  [#38614](https://github.com/MaterializeInc/materialize/pull/38614) is the
  first instance.

## Solution Proposal

Two layers. Layer 1 is one chokepoint owned by one team and is the incident
fix, implemented in #38616. Layer 2 is per-owner work that removes the cost at
the source.

### Layer 1: export-side filter at the registry chokepoint

`mz_ore::metrics::MetricsRegistry::gather` runs a list of postprocessors over
the gathered families. The compute controller already uses one to add a
`workload_class` label to every series carrying `instance_id`. The export
filter (`mz_metrics::export_filter`) is a second postprocessor, installed once
per environmentd instance from `mz_environmentd::serve`, that drops families
and series before encoding according to four dyncfgs. All are
`ParameterScope::Environment` and default to no filtering.

| dyncfg | Type | Default | Effect |
|---|---|---|---|
| `metrics_export_disabled_families` | comma list, trailing `*` glob | `""` | Families whose name matches are removed. |
| `metrics_export_cluster_allowlist` | comma list of cluster IDs | `""` (all) | Series carrying `instance_id`, `cluster_id` or `compute_instance` are kept only for the listed clusters. Series without such a label, or with an empty value, are unaffected. |
| `metrics_export_replica_allowlist` | comma list of replica IDs | `""` (all) | The same for `replica_id`. |
| `metrics_export_max_series_per_family` | usize | `0` (unlimited) | A family with more exported samples than this is dropped whole and counted in a self-metric. |

Semantics as predicates over the output of one gather, for every retained
family `f` not named with the `mz_metrics_export_` prefix, with `S(f)` its
series and `samples(f)` the number of text-format lines it encodes to:

- `not glob_match(disabled_families, f.name)`
- `for all s in S(f) with a cluster label: cluster_allowlist is empty or label value is empty or label value in cluster_allowlist`
- `for all s in S(f) with a replica label: replica_allowlist is empty or label value is empty or label value in replica_allowlist`
- `max == 0 or samples(f) <= max`
- `S(f)` is non-empty

Design decisions:

- **Samples, not label sets.** The cap and the series gauge count what the
  text encoder emits. A histogram series expands to one line per bucket plus
  `+Inf`, `_count` and `_sum`, so counting label sets would undercount
  histogram-heavy families by an order of magnitude, and those are exactly the
  families that dominated the incident.
- **Whole-family drop at the cap, never truncation.** A truncated family is an
  arbitrary subset that looks complete on a dashboard. An absent family is
  visibly absent, and the `*_info` dashboards already fall back when a family
  is missing.
- **Self-metrics are exempt.** The filter's own families are never filtered,
  so a small cap or a broad prefix cannot hide the metrics that explain what
  was dropped.
- **Configuration is read from the live system dyncfg set.** environmentd
  already holds an `Arc<ConfigSet>` built from every dyncfg that the storage
  controller updates on every system-config change. The filter reads the four
  raw values at each gather and re-parses only when they change, so there is
  no catalog round-trip per scrape, no process-global state, and no separate
  update hook to keep in sync. The parsed configuration is shared through an
  `Arc` swapped under a short mutex hold, so a coordinator config update never
  waits on an in-flight filter pass.
- **Per-instance, not per-process.** The test harness starts several
  environmentd instances in one process, each with its own registry, so the
  filter is installed from `serve` rather than from the binary's `main`.
- **Where it applies.** The postprocessor filters every consumer of
  `gather()`. `/metrics/public` gathers environmentd's own families through it
  and then merges clusterd-sourced series it fetches separately, so the filter
  governs environmentd's own series only. That is the intended scope.

Self-metrics, registered by the filter:

| Metric | Labels | Purpose |
|---|---|---|
| `mz_metrics_export_series` (gauge) | `family` | Samples exported per family at the previous gather, after filtering. This is the leading indicator we lacked during the incident. |
| `mz_metrics_export_dropped_series_total` (counter) | `family`, `reason` in {`disabled_family`, `cluster_allowlist`, `replica_allowlist`, `over_cap`} | What the filter is doing. |
| `mz_metrics_export_encoded_bytes` (gauge) | none | Size of the most recently encoded internal `/metrics` response. |

Suggested alert: `mz_metrics_export_encoded_bytes > 30 MiB` fleet-wide, well
under the 50 MiB limit. `topk(10, mz_metrics_export_series)` gives the
reduction candidates per environment.

Cost: gather still materializes every registered series before the filter
runs. The filter fixes the payload and the scrape, not the process-side CPU
and memory of maintaining the series. That is Layer 2.

### Layer 2: emission-side gating per owner

Each per-object family gets a dyncfg that stops series from being created, so
the process stops paying for them and leaks are bounded. Ordered by sample
contribution in the incident environment.

- **Cluster: `mz_dataflow_wallclock_lag_seconds{,_sum,_count}`.** Created per
  collection per replica by `wallclock_lag_metrics` in `mz_cluster_client`,
  from the compute controller and the storage controller. Proposal: dyncfg
  `wallclock_lag_metrics_scope` with values `all` (default), `replica`
  (aggregate over collections into a per-replica family), `none`.
  Per-collection lag remains queryable through
  `mz_internal.mz_wallclock_global_lag_history`.
- **SQL: the `application_name` label** on `mz_time_to_first_row_seconds` and
  `mz_adapter_commands`. The label is client-controlled and unbounded, and a
  histogram multiplies it by about 19 buckets. Proposal: dyncfg
  `adapter_metrics_application_name_label` with values `full` (default),
  `allowlist` (names not in a configured list collapse to `other`), `off`.
- **Persist: `ShardsMetrics`.** 43 families with one series per shard.
  Proposal: dyncfg `persist_shard_metrics_enabled` in `PersistConfig`. When
  false, `ShardMetrics::new` builds unregistered local metric instances so
  call sites keep working with no branching.
- **SQL: the `*_info` families.** A zero
  `catalog_info_metrics_reconcile_interval` stops reconciliation but leaves
  existing series in place. Change: zero also clears the series.
- **Cluster: per-replica and per-cluster controller metrics.** Bounded by
  replica and cluster count rather than object count. Cover with Layer 1
  allowlists first.

### Cross-cutting: declare cardinality class at the definition

`metric!` accepts `tags:` and `visibility:` as documentation-only metadata
consumed by `bin/gen-metrics-catalog`. Add a `scales_with:` field with values
`Object`, `Shard`, `Collection`, `Replica`, `Cluster`, `Client`, keep a
name-to-class map in the registry at registration, and let
`metrics_export_disabled_families` accept `class:per_object` in addition to
family names. A metrics-catalog lint that fails CI when a metric has a
scaling label and no class keeps the inventory above from rotting.

## Minimal Viable Prototype

#38616 implements Layer 1 in full: the four dyncfgs, the postprocessor, the
self-metrics, unit tests for every rule, and an environmentd integration test
that flips each dyncfg through `ALTER SYSTEM` and scrapes `/metrics`. CI
system-parameter defaults run the filter path with a non-existent probe family
and a high cap so the postprocessor executes in every mzcompose-based test
without changing what tests observe. The drop branches themselves are covered
by the unit and integration tests, not by the CI defaults.

## Alternatives

- **Emission-side gating only.** Fixes process cost as well as payload, but
  needs a change in each owning crate and gives no single lever during an
  incident. It is Layer 2, sequenced after the chokepoint.
- **Gzip on the endpoint.** About 10x smaller payload, but AMP's limit is
  checked against the decoded body in practice and compression does not
  reduce sample count. Tracked on the Cloud side as a complementary
  mitigation.
- **Truncating an over-cap family** instead of dropping it. Rejected because a
  partial family is indistinguishable from a complete one on a dashboard.
- **A process-global filter updated through `mz_metrics::update_dyncfg`.**
  The first implementation. Rejected because the test harness runs several
  environmentd instances per process and the coordinator's config update
  would have contended with an in-flight filter pass.

## Open questions

1. Should `/metrics/public` also filter the clusterd-sourced series it merges
   in? The current scope is environmentd's own series only.
2. Is per-collection wallclock lag as a Prometheus metric load-bearing for any
   alert, or is `mz_wallclock_global_lag_history` sufficient? Decides whether
   the `replica` aggregate in Layer 2 is needed or `none` is enough.
3. Should the cap default to a non-zero value in production once the
   self-metrics have a few weeks of data?
