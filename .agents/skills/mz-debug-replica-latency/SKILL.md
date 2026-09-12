---
name: mz-debug-replica-latency
description: >
  Triage a production Materialize replica that is stalling or running slow,
  using Grafana (Prometheus) and PolarSignals. Trigger: a "persist ... heartbeat
  call took Ns" or slow-consensus warning, "why is this replica slow", "persist
  op taking seconds", "consensus/CRDB looks slow", "environmentd/clusterd
  stalled", "reads timing out in prod", or a pasted prod log line plus Grafana
  access. Reach for this whenever a single replica or environment shows
  multi-second internal latency in production and you need to decide whether the
  cause is the backing store, the process/runtime, shard contention, or memory
  pressure, even if the user only pastes a log line and asks "what is this".
---

# Debugging production replica latency

A single warning like `mz_persist_client::read reader (...) of shard (...)
heartbeat call took 60.6s` almost never means what it says on the surface.
The warning names a downstream symptom (a slow consensus/persist operation),
but the real cause is usually one layer down: the backing store, the process
runtime, shard contention, or local memory pressure.
This skill is the triage procedure to tell those apart with evidence, instead
of guessing from the log line.

The core mental model: most of these "slow op" warnings time an `.await` on an
external call. That call can be slow because the store is genuinely slow, OR
because the local process is not polling the future (it is stalled off-CPU on
paging, blocked threads, or CPU starvation). The whole job here is to figure out
which, because the fixes are completely different.

## What you need

* Grafana MCP (Prometheus) access to the production metrics datasources.
* PolarSignals MCP access for CPU profiles (optional but useful).
* The identifying labels for the affected workload: namespace (the environment),
  and pod (the specific replica or environmentd). A log line usually does not
  carry these, so the user must supply them or you infer them from context.

Do not hardcode datasource UIDs or environment identifiers. Discover them at
runtime, as below. Treat metric names as the stable contract, since UIDs and
label values drift.

## Step 0: Locate the environment

List Prometheus datasources (`list_datasources type=prometheus`). Production
regions follow a `cloud-production <region>` naming pattern, with separate
datasources per region. You do not know the region up front, so probe.

Confirm the environment lives in a datasource before querying deeply. A cheap
existence check:

```promql
count by (namespace) (mz_persist_external_started_count{namespace="<env-namespace>"})
```

Run it across the production datasources until one returns data. That datasource
is your environment's region. The infra metrics (`kube_*`, `container_*`,
cadvisor) live in the same datasource as the `mz_*` app metrics.

NOTE: the pod label is `pod`. If a `pod=` filter returns empty but the namespace
has data, you probably have the wrong metric name, not the wrong label. Verify
the exact metric name with `list_prometheus_metric_names` before assuming the
label is wrong. Persist external-op metrics are `mz_persist_external_seconds`,
`mz_persist_external_started_count`, `mz_persist_external_succeeded_count` (note:
no `op` in the metric name; `op` is a label).

## The decision tree

Work top to bottom. Each step narrows the cause and tells you what to read next.

1. **Is the slowness one replica, or the whole environment?**
   Compare the suspect pod's external-op latency against its siblings on the
   same backing store. Average op latency:

   ```promql
   sum(rate(mz_persist_external_seconds{op="consensus_cas", namespace="<ns>"}[2m]))
     by (pod)
   / sum(rate(mz_persist_external_started_count{op="consensus_cas", namespace="<ns>"}[2m]))
     by (pod)
   ```

   Or `topk(10, ...)` of the same. If every pod is elevated, suspect the backing
   store (CRDB) or a shared dependency. If only one pod is elevated while others
   sit at single-digit milliseconds, the store is healthy and the problem is
   local to that replica. This single comparison eliminates half the hypotheses.

2. **If isolated to one pod: are reads slow too, or only writes?**
   Check a trivial read op:

   ```promql
   rate(mz_persist_external_seconds{op="consensus_head", pod="<pod>"}[2m])
   / rate(mz_persist_external_started_count{op="consensus_head", pod="<pod>"}[2m])
   ```

   `consensus_head` is a point read. If it averages multiple seconds, the store
   cannot be the cause, because the same store serves other pods in microseconds.
   Multi-second point reads mean the process is not polling its futures, i.e. the
   runtime itself is stalled. Go to the memory-pressure and CPU-starvation
   checks. If reads are fast but only writes (`consensus_cas`) are slow, suspect
   shard contention (next step).

3. **Is it shard contention (lost compare-and-set races)?**
   A persist command that loses the CaS re-fetches state and retries with no
   timeout, so heavy contention inflates wall-clock.

   ```promql
   increase(mz_persist_cmd_cas_mismatch_count{pod="<pod>"}[4m])
   ```

   This metric carries a `cmd` label, so you see which command is contending
   (e.g. `downgrade_since`, `compare_and_append`). A large mismatch count on the
   command named in the warning confirms a retry loop. Contention alone rarely
   produces 60s stalls on its own. It usually compounds with a runtime stall:
   each retry has to touch state that is slow to access.

4. **Is the process stalled by memory pressure / swap?**
   See `references/memory-pressure.md`. This is the most common root cause of an
   isolated replica whose reads AND writes are all slow.

5. **Is the process CPU-starved or throttled?**
   See `references/cpu-and-runtime.md`.

## Confirming off-CPU stalls with profiles

When you suspect a runtime stall, a CPU profile is the tie-breaker. In
PolarSignals, list projects and pick the cloud project, then pull an on-CPU
flame graph or table for the pod and time window.

The key inference: the production cloud project typically exposes ONLY an on-CPU
profile (`parca_agent:samples:count:cpu:nanoseconds:delta`), not an off-CPU /
wallclock profile. So you cannot directly profile a blocked thread. Instead, use
on-CPU as a process of elimination: if the relevant subsystem (e.g. persist) is
near-idle on-CPU during a window where its ops were taking seconds, the time was
spent OFF-CPU (blocked on IO, locks, or page faults), not computing. A near-idle
on-CPU profile during a stall is itself the evidence of a blocking, not
compute-bound, problem.

Flame graphs for Rust binaries are huge. Filter the table by `filename` (e.g.
the crate name) and start with a tight `time_range` to avoid oversized results.

## A telling corroboration: scrape blackouts

When a process stalls hard (paging, blocked runtime), it stops serving its own
`/metrics` endpoint, and often the node's cadvisor loses the container too. In
the raw counter series you will see a gap: timestamps jump across the stall
window with no samples in between. If both the app `/metrics` series and the
node cadvisor series blank out over the same window, that is strong independent
evidence the whole process stalled, not that one dependency was slow. Always
look at raw counter series (not just `rate()`) around the incident, because
`rate()` over a gap can silently return empty or interpolate.

## Metric cheat-sheet

* `mz_persist_external_seconds{op,pod}` / `mz_persist_external_started_count{op,pod}`
  — average external-op latency. Ops include `consensus_cas`, `consensus_head`,
  `consensus_scan`, `consensus_truncate`, `blob_get`, `blob_set`, etc.
* `mz_persist_cmd_cas_mismatch_count{cmd,pod}` — CaS retry / contention per command.
* `mz_persist_cmd_seconds{cmd,pod}` — per-command time, to see which command dominates.
* `mz_metrics_libc_ru_majflt_total{pod}` — major page faults (swap-ins). See memory reference.
* `container_memory_rss`, `container_memory_swap`, `container_memory_working_set_bytes`
  — memory footprint. See memory reference.
* `kube_pod_container_resource_limits{resource="memory"}` — the cgroup limit
  (dedupe duplicate kube-state-metrics series with `max()`).
* `container_cpu_cfs_throttled_seconds_total`, `mz_metrics_libc_ru_nivcsw_total`
  — CPU throttling / involuntary preemption. See CPU reference.

## Writing it up

These investigations usually end in a ticket to the team that owns the
subsystem. Lead with the conclusion (root cause), then the evidence chain in the
order that eliminates alternatives: scope (one pod vs fleet), then the
store-is-healthy proof (sibling latencies, fast point reads elsewhere), then the
local cause (memory/CPU), then how it ties back to the original warning. State
plainly that the warning was a symptom, not the cause, so the next responder
does not chase the wrong layer.
