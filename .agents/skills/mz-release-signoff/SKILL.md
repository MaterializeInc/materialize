---
name: mz-release-signoff
description: >
  Verify a release candidate on the Grafana dashboards and sign off in #release.
  Trigger: "verify the release", "sign off on the release", "release
  verification", "check the canary", "does this release look healthy", a pasted
  grafana.dev.materialize.com dashboard link, or a release-bot "please verify
  the vX.Y.Z" request. Also when asked to compare metrics across a release
  boundary.
argument-hint: <release version, dashboard URL, or Slack thread URL>
---

Verify a release candidate by comparing metrics across the release boundary, then report the outcome.

The release bot posts a request in the `#release` Slack channel naming the version and linking one dashboard per area. Each area is verified by that area's team. This skill covers the shared method plus per-area metric references.

## Prerequisites

This workflow needs the Grafana MCP server against `grafana.dev.materialize.com`. Verify it answers before starting:

```
mcp__grafana__list_datasources with type "prometheus"
```

Reading the bot request also needs the Slack MCP server, but a user-pasted dashboard link or version string is enough to proceed without it.

## Read the panel instructions, then apply this skill

Two dashboards carry their own `Signing off on Releases` text panel, and they say different things. Read both at the start of every run, because they are maintained separately from this skill:

```
mcp__grafana__get_dashboard_property
  uid: f248986d-81c6-42a6-817b-00cd7759d808     # compute
  jsonPath: $.panels[?(@.type=="text")].options.content

mcp__grafana__get_dashboard_property
  uid: e6dc7745-7d35-4968-a23d-689883a984bb     # storage-overview
  jsonPath: $.panels[?(@.type=="text")].options.content
```

If a panel and this skill disagree, the panel wins on *what* to inspect and this skill wins on *how* to measure it. Report the disagreement so one of them gets fixed.

## Every dashboard names its variables differently

This is the single most common source of a wasted query. There is no shared convention.

| Dashboard | Environment variable | Others |
|---|---|---|
| `compute-overview` | `namespace` | `version`, `organization`, `cluster_id`, `replica_id`, `worker_id`, `collection_id` |
| `storage-overview` | `env` | `cluster`, `replica`, `object_id`, `pod` |
| `storage-upsert-sources` | `namespace` | `pod`, `source` |
| `persist` | `env` | `pod` |
| `environmentd-health` | `env` | `pod` |
| `release-health` | `organization`, an organization id, not a namespace | `mz_cluster`, `version` |
| `networking` | `namespace` | `pod`, `tenant` |

The release bot sets both `var-namespace` and `var-env` on every link for this reason.

## Step 1: Establish the version and the boundaries

Every judgement in this workflow is a before/after comparison, so the upgrade times must be pinned first. Ask Prometheus which version each environment runs over the past 10 days:

```promql
count by (mz_version) (group by (namespace, mz_version) (v2_mz_compute_cluster_status{mz_version!~".*-dev.*"}))
```

Run it as a range query with a 6h step. The `-dev` exclusion drops personal development environments, which run arbitrary old builds and only add noise.

Two facts come out of this. The release under test is the highest `vX.Y.0-rc.N` present, and the boundary per stack is the step where the previous version disappears and it appears. Expect two boundaries for the same release, because a later `rc` usually supersedes an earlier one, and both are the new release for sign-off purposes.

A second, sharper boundary marker is the process count doubling. Zero-downtime upgrades run the old and new generation side by side, so any per-pod series count roughly doubles for one bucket:

```promql
count(avg_over_time(container_memory_working_set_bytes{pod=~".*cluster-.*-replica-.*", container="clusterd"}[6h]))
```

Use those doubling buckets as the boundary, and exclude them from both the before and after samples. They contain two full fleets and will corrupt any sum.

## Step 2: Choose the namespace set

The dashboards' `version` variable does not filter panels directly. It narrows the `organization` variable, which narrows `namespace`, and the panels filter on `namespace`. Setting `version` to the new release therefore means "the environments that run the new release now", and the panels then show those same environments on both sides of the boundary. Reproduce that selection explicitly rather than relying on the variable chain.

Production and staging need different selectors. In production only the canary environments run a release candidate, and they share the plain released version with every customer environment for part of the week, so a version-based filter loses them. Derive their namespaces once and pin them by name:

```promql
group by (namespace, mz_context_org_name) (v2_mz_compute_cluster_status{mz_version=~"<new release>.*"})
```

The canary organizations are `Materialize Production Sandbox` and `Materialize Production Analytics`. Which regions carry which has changed over time, so always derive rather than assume, and note that the bot's links and Prometheus have disagreed on this. Sources and Sinks historically inspected only the sandbox environment.

The canonical canary list lives in `MaterializeInc/release`, in `templates/issue.md`, as the `--environment` arguments to `bin/deploy upgrade production`. The `storage-overview` sign-off panel points instead at `MaterializeInc/cloud/.github/ISSUE_TEMPLATE/03-release.md`, which no longer exists.

In staging every environment runs a release candidate, so a version join both selects the right set and excludes development environments:

```promql
sum(rate(<metric>[6h]) * on(namespace) group_left()
    group by (namespace) (v2_mz_compute_cluster_status{mz_version=~".*-rc[.].*"}))
```

Escaping note: write `-rc[.]` rather than `-rc\\.` so the expression survives JSON encoding unchanged.

## Step 3: Choose the time window

The bot's links default to roughly `now-2d`, resolved to an absolute timestamp at post time. That is too short. The window must contain a clean stretch of the previous release, the boundary, and a clean stretch of the new release, which in practice means seven days or more. Look back far enough to include the previous upgrade, so that the previous boundary is available as a calibration reference.

A window of `Sun 12:00 UTC` through the current day at `12:00 UTC` with a 6h step has worked well. The 6h step averages away diurnal structure while leaving enough points to see a step.

Sample the three phases separately, and never straddle a boundary or a doubling bucket:

* Previous release, steady state.
* New release under the earlier `rc`, steady state.
* New release under the current `rc`.

## Step 4: Derive the metric roster

Take the roster from the dashboard itself so it cannot drift out of date:

```
mcp__grafana__get_dashboard_property  uid: <dashboard uid>  jsonPath: $.panels[*].title
mcp__grafana__get_dashboard_panel_queries  uid: <dashboard uid>
```

The panel titles are cheap and give the row structure. The panel queries are not. On the compute dashboard that call returns about 62 KB across 165 panels, which overflows the tool result and is written to a file instead. Slice that file with a script and extract only metric names and label selectors. Never read it whole, and never paste it into the conversation.

Row panels appear in the title list but not in the query list, so the two are offset. Match them by title, not by index.

## Step 5: Measure

Batch many metrics into one range query by tagging each aggregate with a synthetic label and combining with `or`. This turns twenty tool calls into one:

```promql
label_replace(sum(rate(<counter>[6h])), "m", "1_name", "", "")
or label_replace(sum(avg_over_time(<gauge>[6h])), "m", "2_name", "", "")
```

Prefix the tags so the result order is stable and readable. Use `rate(x[6h])` for counters and `avg_over_time(x[6h])` for gauges, both matching the step so buckets do not overlap.

Run each area twice. Once across all clusters, and once restricted to the system clusters, which the panel instructions call out because a system-cluster regression is easy to lose in the noise of user clusters. System clusters are `instance_id=~"s[0-9]+"` for the controller and replica metrics, and `pod=~".*cluster-s[0-9]+-replica-.*"` for the container metrics.

## Step 6: Judge

There are no thresholds, so the discipline is in ruling out the confounders before believing a signal.

**Calibrate before calling anything a regression.** Fleet composition and workload drift produce steps of the same size as most real regressions, so pull three weeks of the metric and look at its natural spread before believing a step. Do this live rather than against a recorded figure, because the spread itself changes as the fleet does. When this was first measured, staging fleet clusterd CPU ranged from 2.83 to 5.55 cores over three weeks, which put a 3% step across the boundary far inside the noise; the useful part of that observation is its size, roughly a factor of two, not the numbers.

**Compare at equal post-restart age.** Every upgrade restarts `clusterd`, and a fresh process holds less memory than one that has been running for days. Comparing the pre-upgrade level against the post-upgrade level therefore flatters the new release, and comparing a post-upgrade level against a mid-week pre-upgrade level exaggerates a regression. Sample both sides at a similar age since restart, and treat a monotonic climb within one release as more informative than any level difference across the boundary.

**Read the base level of bimodal metrics.** Arrangement record counts and sizes swing by a factor of three or more as periodic dataflows rebuild. Compare the low state against the low state; spike heights are not comparable.

**Discount pre-existing noise.** Some staging environments crashloop or carry permanently erroring dataflows. In August 2026 staging us-east-1 sustained roughly 90 `clusterd` restarts per 6h and staging eu-west-1 carried 50 to 400 dataflow errors continuously, both flat across the boundary. Flat means not release-related. The panel instructions suggest filtering such environments out with the dashboard variables, which is worth doing when they mask everything else.

**Separate a signal from its location.** A fleet-wide step and one environment moving the fleet total are different findings. Split them with a per-namespace ratio of the two windows:

```promql
sort_desc(sum by (namespace) (rate(<metric>[15h]))
        / sum by (namespace) (rate(<metric>[42h] offset 126h)))
```

Set the instant query's end time to the end of the new window, and pick the offset so the second window lands in the previous release.

## Step 7: Drill deeper

When a signal survives Step 6, tighten the aggregation one level at a time. Each level costs one query and narrows the search:

1. Fleet total, which establishes that something moved.
2. Per namespace, which finds the environments involved.
3. Per cluster, via `instance_id`, which separates system from user clusters.
4. Per replica, via `replica_id`, which distinguishes a replica-local effect from a cluster-wide one.
5. Per worker, via `worker_id`, which exposes skew across workers of one replica.
6. Per collection, via `collection_id`, which names the dataflow.

Below the metric layer, hand off rather than guess. Use `mz-profile` for CPU and memory attribution inside a process, `mz-query-tracing` for the latency breakdown of a statement, and the Polar Signals MCP server for on-CPU profiles of a running environment. If the signal implicates a specific change, `mz-debug-ci` covers finding the responsible build.

## Step 8: Report

State the verdict first, then the method, then the findings. The method matters because the reader has to judge whether the comparison was fair: name the version, the boundaries, the window, the namespace selection, and which regions and rows were covered. Say explicitly what was skipped and why.

Separate release-blocking findings from notes. A note is something worth a second look next release that does not survive Step 6 as a regression. Give each note its magnitude and the reason it is not conclusive, so the next person can compare against it rather than rediscover it.

Sign-off happens in the bot's thread. As of August 2026 there is a proposal to sign off by reacting to each team's message rather than replying, which was not yet confirmed; check the thread's convention before posting, and never post to Slack without the user asking.

## Areas

The bot links one dashboard per area. All are on `grafana.dev.materialize.com`.

| Area | Dashboard | UID | Reference |
|---|---|---|---|
| Compute | `compute-overview` | `f248986d-81c6-42a6-817b-00cd7759d808` | `references/compute.md` |
| Sources and Sinks | `storage-overview` | `e6dc7745-7d35-4968-a23d-689883a984bb` | `references/sources-and-sinks.md` |
| Sources and Sinks | `storage-upsert-sources` | `ac2de0ab-4a35-48b5-93aa-7e645569debb` | `references/sources-and-sinks.md` |
| Persist | `persist` | `m3U1U6ZVk` | `references/persist.md` |
| Adapter | `environmentd-health` | `mR1Kg1d4z` | `references/adapter.md` |
| Reference | `release-health` | `zKe0K0N4z` | `references/reference-dashboards.md` |
| Reference | `networking` | `bHQE8bN4k` | `references/reference-dashboards.md` |

Each reference names the metrics, their types and labels, the invariants that hold at any fleet size, and the hazards specific to that area. Read the one for the area you are verifying before running a single query. The `Storage` label the bot still uses refers to work now split between Sources and Sinks and Persist.

Sweep sizes differ by an order of magnitude. Compute has about 60 sweep-relevant panels, adapter about 45, storage-overview about 55, and persist 375 panel targets over roughly 230 metrics. For persist, the dashboard's own `should be small` panel defines the sweep; see its reference.

The bot links production only, but staging is a larger and earlier sample of the same release, and Adapter verification has covered staging in practice. Prefer running both.

Datasource UIDs, which the dashboards take as the `datasource` variable:

| Stack | Region | UID |
|---|---|---|
| Production | us-east-1 | `2K85O21Vz` |
| Production | eu-west-1 | `E0J0O2J4k` |
| Production | us-west-2 | `ee2e6227-dc2d-4ca5-bb58-ca826fd6d614` |
| Staging | us-east-1 | `Ks85Oh14z` |
| Staging | eu-west-1 | `JKT0Oh1Vk` |
| Staging | us-west-2 | `c979f3ee-16d0-44f1-9f24-9e208e0326d9` |

Staging us-west-2 held no compute environments in August 2026, so `v2_mz_compute_cluster_status` returns nothing there. Confirm it is still empty rather than reporting a region as clean.

## Characterizing an area

All seven dashboards have a reference, written from a sweep of v26.38.0-rc.3 against v26.37.0 in August 2026 across production canary and staging in us-east-1, with compute additionally covering production eu-west-1, production us-west-2, and staging eu-west-1.

To characterize a new dashboard, or to refresh one, run Steps 1 through 6 against it for one release and record in `references/<area>.md` what the run taught you:

* Each metric with its type, the labels that select cluster and replica, and what it means.
* Which metrics are bimodal, restart-sensitive, or absent when zero.
* Invariants: relationships that hold at any fleet size, such as one counter equalling the difference of two others, a gauge whose only meaningful aggregate is a series count, or a metric that is structurally absent in one stack.
* Label naming inconsistencies, duplicate-series hazards, and any panel expression whose filters are not what they appear to be.
* Known noise classes, meaning the environments that are unhealthy independently of any release and whose flat contribution can dominate a fleet aggregate.

Record invariants, not levels. A recorded level is stale the week after it is written, because environments are created, deleted, and resized continuously, and a stale reference value is worse than none: it invites a comparison the reader should not make. The comparison that matters is always derived in-run, since the before-window of your own query is the only baseline guaranteed to describe the same fleet as the after-window.

Coarse order-of-magnitude figures are worth keeping for one narrow purpose: catching a mis-scoped selector, for example a missing `container="clusterd"` that inflates a result tenfold. Keep them dated, keep them to one significant figure, and say plainly that they are not for comparison.

## Traps

**A gauge divided by a limit can fail on duplicate series.** `container_spec_memory_limit_bytes` is exported once per node-label set, and labels such as `karpenter_sh_initialized` flip during node lifecycle, which yields two series for one pod and a `many-to-many matching not allowed` error. Collapse both sides first:

```promql
max by (namespace, pod, container) (avg_over_time(container_memory_working_set_bytes{...}[6h]))
/ on (namespace, pod, container)
max by (namespace, pod, container) (avg_over_time(container_spec_memory_limit_bytes{...}[6h]))
```

**An absent series is not the same as a healthy zero.** Error and orphan counters are only exported when non-zero, so an empty result reads as clean when it can also mean the metric was renamed. Confirm the metric exists somewhere in the window before reporting zero.

**Label names are not consistent across metrics.** Most compute metrics carry `instance_id` and `replica_id`, but the arrangement maintenance metric carries `cluster_environmentd_materialize_cloud_cluster_id` and `cluster_environmentd_materialize_cloud_replica_id` instead. Copy selectors from the panel expressions rather than writing them from memory.

**Some panel filters are variable-substitution artifacts.** Several compute panels append `instance_id!="$cluster_id"`, which exists to blank a series when a single cluster is selected and is not a semantic filter. Reproducing it in an aggregate query is unnecessary.

**Counter resets hide inside long rate windows.** Pods are replaced at every upgrade, so restart counters reset. Prefer counting the pods whose last termination matched a condition over summing increases across a boundary:

```promql
count(kube_pod_container_status_last_terminated_exitcode{pod=~".*cluster-.*-replica-.*", container="clusterd"} == 137)
```

**Which exit code matters depends on the dashboard.** The compute dashboard looks for 137, an OOM kill. The `release-health` non-clusterd restart panel joins against `!= 166`, treating 166 as an expected termination. Do not carry one dashboard's convention into another.

**An empty panel is the most dangerous reading on any dashboard.** Metrics get renamed and panels do not follow, so the panel renders blank and looks like health. This was not hypothetical: the sweep found four dead expressions on `environmentd-health` and two on `storage-overview`, all silently empty. Before reporting any metric as zero, confirm the name still exists:

```
mcp__grafana__list_prometheus_metric_names  datasourceUid: ...  regex: mz_catalog.*
```

The per-area references list the dead panels found so far. Check for new ones whenever a panel that should have data does not.

**Some odd-looking expressions are deliberate and must be preserved.** Two idioms recur. An `or` between two aggregations, as on `storage-overview`'s controller protocol panels, straddles a metric rename so the panel keeps working across the version boundary; imitate it rather than deleting the dead arm. A trailing `^0`, as in `networking`'s egress panels, raises a series to the power zero to yield 1 for every series that exists, making it a set-membership filter rather than arithmetic.

**An implausibly constant quantile is a bucket artifact, not stability.** A `histogram_quantile` landing inside one wide bucket returns the bucket boundary and cannot move, which reads as a rock-steady latency. Two adapter panels do this. When a quantile is stable to five significant figures while its counters advance, switch to `rate(_sum) / rate(_count)`.

**Counting series is sometimes the measurement.** Several metrics are per-entity gauges whose only useful aggregate is a series count: `mz_persist_shard_upper` for shards, `mz_balancer_metadata_seconds` for balancers, `mz_persist_metadata_seconds` by `version` for persist client builds. Similarly `mz_source_progress` is a millisecond frontier timestamp, so a healthy series contributes exactly 1000 to its rate and the panel's absolute value is really a series count in disguise.
