---
title: "Cluster CPU troubleshooting"
description: "How to diagnose a spike in cluster CPU utilization, including checking for worker skew."
menu:
  main:
    name: "Troubleshoot CPU spikes"
    identifier: cpu-troubleshooting
    parent: clusters
    weight: 90
---

A cluster's CPU utilization can spike because the cluster is genuinely
undersized for its workload, or because the workload is unevenly distributed
across the cluster's workers. The two look similar from the outside, so start
by checking for the more common cause in practice: **worker skew**.

## Check for worker skew

Materialize distributes work across a cluster replica's workers by hashing
keys, such as join keys and `GROUP BY` keys. If one key value accounts for a
disproportionate share of rows, the worker responsible for that value does
disproportionate work while its peers idle. A replica in this state can show
elevated CPU utilization, or an elevated peak per-core utilization, even
though the total workload volume hasn't changed.

To check for skew across an entire cluster, connect to it and run
[`EXPLAIN ANALYZE CLUSTER CPU WITH SKEW`](/sql/explain-analyze/#explain-analyze--with-skew):

```mzsql
SET CLUSTER TO <cluster_name>;
EXPLAIN ANALYZE CLUSTER CPU WITH SKEW;
```

The output reports each dataflow's CPU time per worker against the average
across workers, alongside the `global_id` of the underlying index,
materialized view, or sink. A ratio near `1` means a worker is doing a
roughly average share of the work; a ratio far above `1` on one worker points
to skew in that object.

## Localize the skew to an operator

Once you've identified a skewed object by its `global_id`, drill into it to
find the specific operator responsible:

```mzsql
EXPLAIN ANALYZE CPU WITH SKEW FOR MATERIALIZED VIEW <object_name>;
```

(Use `FOR INDEX <object_name>` for an index.) The operator with the highest
ratio, most often a `Join` or `Reduce`, is where the skew originates.

## Find the hot key

A skewed `Join` or `Reduce` operator is almost always caused by a **hot
key**: one value in the join or `GROUP BY` column(s) accounts for far more
rows than the rest. Confirm it by counting rows per value on the suspect
column:

```mzsql
SELECT <key_column>, count(*) AS num_rows
FROM <object_name>
GROUP BY <key_column>
ORDER BY num_rows DESC
LIMIT 20;
```

A single value with an outsized count relative to the rest confirms the hot
key. See [Is work distributed equally across
workers?](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
for other common causes of skew, such as cross joins and `ORDER BY`/`LIMIT`/`OFFSET`
queries.

To resolve skew, restructure the query so the hot key's rows aren't
concentrated on a single worker, for example by pre-aggregating or filtering
rows before the join. If the skew can't be eliminated, size the cluster for
the busiest worker's load rather than the average, since the other workers
won't absorb it.

## Rule out general cluster overload

If `EXPLAIN ANALYZE CLUSTER CPU WITH SKEW` shows a roughly even ratio across
workers, the spike isn't skew. See [Check cluster
health](/transform-data/freshness-troubleshooting/#check-cluster-health) for
how to check overall CPU/memory pressure and OOM crash loops.
