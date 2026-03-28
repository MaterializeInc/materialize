---
name: cloud-debug
description: >
  Debug a struggling Materialize cloud environment. Triggers when the user
  mentions "cloud environment", "struggling cluster", "slow queries in cloud",
  "mz_catalog_server", "arrangement sizes", "OOM", "high memory", or wants to
  connect to a remote Materialize environment via kubectl/k9s for diagnostics.
  Use this skill even if the user just says "the environment is slow" or
  "something is wrong in staging/production".
argument-hint: <namespace or environment ID, e.g. environment-365f789f-...>
---

# Cloud Environment Debugging

Debug performance issues in a remote Materialize cloud environment. All
`kubectl` and `psql` commands require `dangerouslyDisableSandbox: true`.

Read `doc/user/content/transform-data/dataflow-troubleshooting.md` for
additional diagnostic queries and guidance.

## Step 0: Verify kubectl access

Before starting, check that kubectl is configured with the right contexts.
Run `kubectl config get-contexts -o name | grep mzcloud` to see if any
`mzcloud-staging-*` or `mzcloud-production-*` contexts are available.

**If no contexts are found or you get permission errors**, kubectl needs to be
set up against our cloud infrastructure. Follow these steps:

1. Clone the [Cloud repo](https://github.com/MaterializeInc/cloud) as a
   sibling directory to the materialize repo.
2. Install prerequisites: `brew install kubectl@1.24 k9s kind` and the
   [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
3. In the cloud repo, run:
   ```bash
   bin/rev-env          # Install dependencies and configure Python venv
   bin/mzadmin aws setup   # Configure AWS credentials
   bin/mzadmin k8s setup   # Configure kubectl contexts for staging/production
   ```

See also `console/README.md` (Cloud setup section) and the cloud repo's
`doc/developer.md` for the most up-to-date instructions.

If you get `Refreshing temporary credentials failed` errors, run
`bin/mzadmin aws login` in the cloud repo to refresh your AWS session.

## Step 1: Connect to the environment

Establish a kubectl port-forward to the environmentd pod.

The context is typically `mzcloud-staging-<region>-0` or
`mzcloud-production-<region>-0`. Search across contexts if unsure:
```bash
for ctx in $(kubectl config get-contexts -o name | grep mzcloud); do
  kubectl --context "$ctx" get ns 2>/dev/null | grep "<ENV_ID>" && echo "  ^^ context: $ctx"
done
```

```bash
# Find the namespace and pod
kubectl --context <CONTEXT> -n <NAMESPACE> get pods

# Port-forward to environmentd (internal SQL port)
kubectl --context <CONTEXT> -n <NAMESPACE> \
  port-forward pod/<ENVIRONMENTD_POD> 6877:6877 &
sleep 5

# Connect
psql "postgres://mz_system@localhost:6877/materialize"
```

Port 6877 is the internal SQL port (`mz_system` user). Port 6875 is the
external SQL port.

**Pod names change on restarts.** The environmentd pod includes a generation
number (e.g., `mzpgpv7o4kmd-environmentd-10-0` → `...-11-0`). Always
re-discover the pod name with `kubectl get pods` if a port-forward stops
working with `pods "..." not found`.

### Port-forward drops after every query

`kubectl port-forward` exits when the remote side resets the TCP connection,
which happens every time psql closes a connection (i.e., after every `-c`
query). This is a known kubectl limitation, not a Materialize issue.

**Solution: auto-restarting loop + retry wrapper.** Start the port-forward
in a loop that auto-restarts, and use a retry helper for queries:

```bash
# Start auto-restarting port-forward (run once per session)
pkill -f "port-forward.*6877" 2>/dev/null; sleep 1
(while true; do
  kubectl --context <CTX> -n <NS> \
    port-forward pod/<POD> 6877:6877 2>/dev/null
  sleep 0.5
done) &

# Retry helper: retries psql up to 5 times to ride out the restart gap
run_query() {
  for i in 1 2 3 4 5; do
    result=$(psql "postgres://mz_system@localhost:6877/materialize" \
      -c "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "$result"; return 0; fi
    sleep 1
  done
  echo "FAILED: $result"; return 1
}

# Usage:
run_query "SELECT 1;"
run_query "SET CLUSTER = mz_catalog_server; SELECT count(*) FROM mz_internal.mz_wallclock_global_lag_recent_history;"
```

The loop restarts the port-forward within ~0.5s after it dies. The retry
wrapper handles the brief gap where the port is not yet listening.

To clean up when done:
```bash
pkill -f "port-forward.*6877"
```

## Step 2: Identify clusters and their sizes

```sql
SELECT c.name, c.id, r.name as replica_name, r.size
FROM mz_catalog.mz_clusters c
JOIN mz_catalog.mz_cluster_replicas r ON c.id = r.cluster_id
ORDER BY c.name;
```

## Step 3: Diagnose the struggling cluster

Set the cluster context for introspection queries:
```sql
SET CLUSTER = <cluster_name>;
```

### 3a: Arrangement sizes (memory)

```sql
SELECT id, name, records, pg_size_pretty(size) AS size
FROM mz_introspection.mz_dataflow_arrangement_sizes
ORDER BY size DESC
LIMIT 20;
```

Look for outsized arrangements. Common culprits on `mz_catalog_server`:
- `mz_source_statistics_with_history_ind` — controlled by `metrics_retention`
- `mz_cluster_replica_metrics_ind` — also controlled by `metrics_retention`
- `mz_wallclock_global_lag_recent_history_ind` — hardcoded 1-day temporal filter

### 3b: Expensive dataflows

```sql
SELECT mdo.id, mdo.name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_introspection.mz_scheduling_elapsed AS mse,
    mz_introspection.mz_dataflow_operators AS mdo,
    mz_introspection.mz_dataflow_addresses AS mda
WHERE mse.id = mdo.id AND mdo.id = mda.id AND list_length(address) = 1
ORDER BY elapsed_ns DESC
LIMIT 20;
```

### 3c: Expensive operators (drill down)

```sql
SELECT mdod.id, mdod.name, mdod.dataflow_name,
    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_introspection.mz_scheduling_elapsed AS mse,
    mz_introspection.mz_dataflow_addresses AS mda,
    mz_introspection.mz_dataflow_operator_dataflows AS mdod
WHERE mse.id = mdod.id AND mdod.id = mda.id
    AND mda.address NOT IN (
        SELECT DISTINCT address[:list_length(address) - 1]
        FROM mz_introspection.mz_dataflow_addresses)
ORDER BY elapsed_ns DESC
LIMIT 20;
```

### 3d: Scheduling duration histogram (blocking operators)

The elapsed time queries show *total* time since creation. To find operators
that **block the worker right now** for long stretches (causing query latency
spikes), use the scheduling duration histogram:

```sql
WITH histograms AS (
    SELECT
        mdod.id,
        mdod.name,
        mdod.dataflow_name,
        mcodh.count,
        mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval AS duration
    FROM mz_introspection.mz_compute_operator_durations_histogram AS mcodh,
        mz_introspection.mz_dataflow_addresses AS mda,
        mz_introspection.mz_dataflow_operator_dataflows AS mdod
    WHERE
        mcodh.id = mdod.id
        AND mdod.id = mda.id
        AND mda.address NOT IN (
            SELECT DISTINCT address[:list_length(address) - 1]
            FROM mz_introspection.mz_dataflow_addresses
        )
)
SELECT *
FROM histograms
WHERE duration > '100 millisecond'::interval
ORDER BY duration DESC
LIMIT 30;
```

Operators with durations of multiple seconds are blocking all other work on
that worker. This is the primary cause of query latency spikes — concurrent
queries queue up waiting for the worker to become available. Common offenders:
- `ReduceMinsMaxes` in `mz_wallclock_global_lag_recent_history_ind` — seen
  blocking for up to 17s due to min/max recomputation over expiring temporal data
- `ArrangeAccumulable` in `mz_console_cluster_utilization_overview_ind` — steady
  high CPU from maintaining the 14-day utilization window

### 3e: Worker skew

```sql
SELECT mse.id, dod.name, mse.worker_id, elapsed_ns, avg_ns,
    elapsed_ns/avg_ns AS ratio
FROM mz_introspection.mz_scheduling_elapsed_per_worker mse,
    (SELECT id, avg(elapsed_ns) AS avg_ns
     FROM mz_introspection.mz_scheduling_elapsed_per_worker
     GROUP BY id) aebi,
    mz_introspection.mz_dataflow_operator_dataflows dod
WHERE mse.id = aebi.id
    AND mse.elapsed_ns > 2 * aebi.avg_ns
    AND mse.id = dod.id
ORDER BY ratio DESC
LIMIT 20;
```

## Step 4: Check slow queries

```sql
SELECT finished_at - began_at as latency, execution_strategy,
    application_name, left(sql, 150) as sql_prefix, began_at
FROM mz_internal.mz_recent_activity_log
WHERE finished_at IS NOT NULL
ORDER BY began_at DESC
LIMIT 30;
```

Filter for standard peeks (ad-hoc dataflow queries, typically the slow ones):
```sql
... WHERE execution_strategy = 'standard' ...
```

Use `EXPLAIN <query>` to check whether a query uses fast-path (index lookup)
or standard peek (temporary dataflow). Look for:
- `Explained Query (fast path):` heading = fast path, good
- `Explained Query:` heading = standard peek, creates temporary dataflow
- `*** full scan ***` in Used Indexes = scanning entire arrangement

## Step 4b: Live monitoring with SUBSCRIBE

When correlating slow queries with worker-blocking operators, run these two
SUBSCRIBEs side by side in separate terminals. They stream changes as they
happen, so you can watch blocking events and the queries they affect in real
time.

**Suggest these to the user** when investigating latency spikes or when the
point-in-time diagnostic queries from Steps 3-4 suggest ongoing issues worth
monitoring live.

### Blocking operators (scheduling duration histogram)

Shows all leaf operators across all dataflows that block the worker for longer
than the threshold. Adjust the interval filter to taste.

```sql
SET CLUSTER = <cluster_name>;
COPY (
    SUBSCRIBE (
        SELECT
            mdod.name AS operator_name,
            mdod.dataflow_name,
            mcodh.count,
            mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval AS duration
        FROM mz_introspection.mz_compute_operator_durations_histogram AS mcodh,
            mz_introspection.mz_dataflow_addresses AS mda,
            mz_introspection.mz_dataflow_operator_dataflows AS mdod
        WHERE mcodh.id = mdod.id
            AND mdod.id = mda.id
            AND mcodh.count > 0
            AND mcodh.duration_ns / 1000 * '1 MICROSECONDS'::interval > '10 millisecond'::interval
            AND mda.address NOT IN (
                SELECT DISTINCT address[:list_length(address) - 1]
                FROM mz_introspection.mz_dataflow_addresses
            )
    ) WITH (SNAPSHOT = true, PROGRESS = true)
) TO STDOUT;
```

To narrow to a specific dataflow, add a filter like:
```sql
AND mdod.dataflow_name LIKE '%mz_wallclock_global_lag_recent_history_ind'
```

To narrow to a specific operator, add:
```sql
AND mdod.name LIKE 'ReduceMinsMaxes%'
```

### Slow queries (activity log)

Shows standard peek queries that exceed the latency threshold as they
complete. Uses `SNAPSHOT = false` to skip historical backfill and only emit
new queries.

```sql
SET CLUSTER = <cluster_name>;
COPY (
    SUBSCRIBE (
        SELECT
            finished_at - began_at AS latency,
            execution_strategy,
            application_name,
            left(sql, 150) AS sql_prefix,
            began_at
        FROM mz_internal.mz_recent_activity_log
        WHERE finished_at IS NOT NULL
            AND execution_strategy = 'standard'
            AND finished_at - began_at > '1 second'::interval
    ) WITH (SNAPSHOT = false, PROGRESS = true)
) TO STDOUT;
```

Drop the `execution_strategy = 'standard'` filter to also see slow
subscribes and fast-path queries. Adjust the `'1 second'::interval`
threshold as needed.

**Note:** These SUBSCRIBEs install dataflows on the target cluster. On an
already-overloaded cluster the initial snapshot may take a while to hydrate.
If no output appears after a couple of minutes, the cluster may be too
saturated — fall back to polling with the SELECT versions from Steps 3-4.

## Step 5: Common fixes

### Reduce metrics_retention

Objects with `is_retained_metrics_object: true` in `src/catalog/src/builtin.rs`
are governed by the `metrics_retention` system variable (default: **30 days**).
This controls how much history is kept for metrics-related indexes.

```sql
-- Check current value
SHOW metrics_retention;
```

**Important:** In cloud environments, `metrics_retention` cannot be changed
directly via `ALTER SYSTEM SET`. It must be changed through **LaunchDarkly**.
The `ALTER SYSTEM SET` command will work for local/dev environments only.

Once changed, the effect is immediate — no restart needed. The coordinator
calls `update_metrics_retention()` which pushes new read policies to storage
and compute. Compaction of old data begins immediately but takes time to
reclaim memory.

Key retained metrics objects:
- `mz_source_statistics_with_history_ind`
- `mz_sink_statistics_ind`
- `mz_cluster_replica_metrics_ind`
- `mz_cluster_replica_metrics_history_ind`
- `mz_kafka_sources_ind`

### Resize a system cluster

```sql
-- See available sizes
SELECT * FROM mz_cluster_replica_sizes ORDER BY credits_per_hour;

-- Resize (creates new replica, hydrates, then removes old one)
ALTER CLUSTER mz_catalog_server SET (SIZE = 'M.1-xsmall');
```

This causes a brief interruption while the new replica hydrates.

### Objects NOT controlled by metrics_retention

These indexes have `is_retained_metrics_object: false` and hardcoded temporal
filters in their view definitions. There is no user-facing knob to reduce them.

- **`mz_wallclock_global_lag_recent_history_ind`** — 1-day window
  (`WHERE occurred_at + '1 day' > mz_now()`). With many objects (e.g., 9,800
  sources/views), this can grow to 46M+ uncompacted records / 2+ GB in
  arrangements. Its `ReduceMinsMaxes` operator is the most common cause of
  worker-blocking latency spikes on `mz_catalog_server`.

- **`mz_console_cluster_utilization_overview_ind`** — 14-day window
  (hardcoded `+ INTERVAL '14 DAYS'` in the view SQL). This is typically the
  heaviest dataflow on `mz_catalog_server` by total elapsed CPU time. Its
  `ArrangeAccumulable` operator does steady continuous work rather than burst
  blocking.

## Diagnostic flowchart

1. **Connect** to the environment via kubectl port-forward
2. **Identify** which cluster is struggling (check pod restarts, user reports)
3. **Check arrangement sizes** — is something using too much memory?
4. **Check expensive dataflows/operators** — what's consuming CPU?
5. **Check recent queries** — are standard peeks slow? What are they?
6. **Apply fixes** — reduce retention, resize cluster, or investigate further
7. **Verify** — re-run diagnostics to confirm improvement
