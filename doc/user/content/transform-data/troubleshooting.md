---
title: "Troubleshooting"
description: "How to troubleshoot common data transformation scenarios where Materialize is not working as expected."
menu:
  main:
    name: "Troubleshooting"
    identifier: transform-troubleshooting
    parent: transform-data
    weight: 30
---

Once data is flowing into Materialize and you start modeling it in SQL, you
might run into some snags or unexpected scenarios. This guide collects common
questions around data transformation to help you troubleshoot your queries.

## Why is my query slow?

<!-- TODO: update this to use the query history UI once it's available -->
The most common reasons for query execution taking longer than expected are:

* Processing lag in upstream dependencies, like materialized views and indexes
* Index design
* Query design

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of slowness, and then address it
accordingly.

### Lagging materialized views or indexes

#### Detect

When a materialized view or index upstream of your query is behind on
processing, your query must wait for it to catch up before returning results.
This is how Materialize ensures consistent results for all queries.

To check if any materialized views or indexes are lagging, use the workflow
graphs in the Materialize console.

1. Go to https://console.materialize.com/.
2. Click on the **"Clusters"** tab in the side navigation bar.
3. Click on the cluster that contains your upstream materialized view or index.
4. Go to the **"Materialized Views"** or **"Indexes"** section, and click on the
object name to access its workflow graph.

If you find that one of the upstream materialized views or indexes is lagging,
this could be the cause of your query slowness.

#### Address

To troubleshoot and fix a lagging materialized view or index, follow the steps
in the [dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

*Do you have multiple materialized views chained on top of each other? Are you
seeing small amounts of lag?*<br>
Tip: avoid intermediary materialized views where not necessary. Each chained
materialized view incurs a small amount of processing lag from the previous
one.
<!-- TODO add more guidance on avoiding chained mat views-->

Other options to consider:

* If you've gone through the dataflow troubleshooting and do not want to make
  any changes to your query, consider [sizing up your cluster](/sql/create-cluster/#size).
* You can also consider changing your [isolation level](/get-started/isolation-level/),
  depending on the consistency guarantees that you need. With a lower isolation
  level, you may be able to query stale results out of lagging indexes and
  materialized views.
* You can also check whether you're using a [transaction](#transactions) and
  follow the guidance there.

### Slow query execution

Query execution time largely depends on the amount of on-the-fly work that needs
to be done to compute the result. You can cut back on execution time in a few
ways:

#### Indexing and query optimization

Like in any other database, index design affects query performance. If the
dependencies of your query don't have [indexes](/sql/create-index/) defined,
you should consider creating one (or many). Check out the [optimization guide](/transform-data/optimization)
for guidance on how to optimize query performance.

If the dependencies of your query are indexed, you should confirm that the query
is actually using the index! This information is available in the query plan,
which you can view using the [`EXPLAIN PLAN`](/sql/explain-plan/) command. If
you run `EXPLAIN PLAN` for your query and see the index(es) under `Used indexes`,
this means that the index was correctly used. If that's not the case, consider:

* Are you running the query in the same cluster which contains the index? You
  must do so in order for the index to be used.
* Does the index's indexed expression (key) match up with how you're querying
  the data?

#### Computation-intensive queries

Another thing to be aware of, in Materialize, aggregations like `COUNT(*)` and limit constraints that return more than 25 rows like `LIMIT 50` perform a computation across the full underlying dataset being queried, so can be resource intensive. If you want to execute these types of queries you should either:
* Create a materialized view or index for these queries if you plan to issue them often
* Use a large enough cluster to fit the full underlying dataset being queried

### Other things to consider

#### Transactions
<!-- Copied from doc/user/content/manage/troubleshooting.md#Transactions -->
Transactions are a database concept for bundling multiple query steps into a
single, all-or-nothing operation. You can read more about them in the
[transactions](https://materialize.com/docs/sql/begin) section of our docs.

In Materialize, `BEGIN` starts a transaction block. All statements in a
transaction block will be executed in a single transaction until an explicit
`COMMIT` or `ROLLBACK` is given. All statements in that transaction happen at
the same timestamp, and that timestamp must be valid for all objects the
transaction may access.

What this means for latency: Materialize may delay queries against "slow"
tables, materialized views, and indexes until they catch up to faster ones in
the same schema. We recommend you avoid using transactions in contexts where
you require low latency responses and are not certain that all objects in a
schema will be equally current.

What you can do:

- Avoid using transactions where you don’t need them. For example, if you’re
  only executing single statements at a time.
- Double check whether your SQL library or ORM is wrapping all queries in
  transactions on your behalf, and disable that setting, only using
  transactions explicitly when you want them.

#### Client-side latency
<!-- Copied from doc/user/content/manage/troubleshooting.md#client-side-latency -->
To minimize the roundtrip latency associated with making requests from your
client to Materialize, make your requests as physically close to your
Materialize region as possible. For example, if you use the AWS `us-east-1`
region for Materialize, your client server would ideally also be running in AWS
`us-east-1`.

#### Result size
<!-- TODO: Use the query history UI to fetch result size -->
Smaller results lead to less time spent transmitting data over the network. You
can calculate your result size as `number of rows returned x byte size of each
row`, where `byte size of each row = sum(byte size of each column)`. If your
result size is large, this will be a factor in query latency.

#### Cluster CPU
Another thing to check is how busy the cluster you're issuing queries on is. A
busy cluster means your query might be blocked by some other processing going
on, taking longer to return. As an example, if you issue a lot of
resource-intensive queries at once, that might spike the CPU.

The measure of cluster busyness is CPU. You can see the CPU by going to
Materialize console at https://console.materialize.com/, clicking
the **"Clusters"** tab in the nav bar, and clicking into the cluster. You can
also grab CPU usage from the system catalog using SQL:

```sql
SELECT cru.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization cru
LEFT JOIN mz_catalog.mz_cluster_replicas cr ON cru.replica_id = cr.id
LEFT JOIN mz_catalog.mz_clusters c ON cr.cluster_id = c.id
WHERE c.name = <CLUSTER_NAME>;
```

## Why is my query not responding?

The most common reasons for query hanging are:

* An upstream source is stalled
* Your cluster is unhealthy

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of the hang, and then address it
accordingly.

{{< note >}}
Your query may be running, just slowly. If none of the reasons below detects
your issue, jump to [Why is my query slow?](#why-is-my-query-slow) for further
guidance.
{{< /note >}}

### Stalled source

<!-- TODO: update this to use the query history UI once it's available -->
To detect and address stalled sources, follow the [`Ingest data` troubleshooting](/ingest-data/troubleshooting)
guide.

### Unhealthy cluster

#### Detect

If your cluster runs out of memory (i.e., it OOMs), this will result in a crash.
After a crash, the cluster has to restart, which can take a few seconds. On
cluster restart, your query will also automatically restart execution from the
beginning.

If your cluster is CPU-maxed out (\~100% utilization), your query may be blocked
while the cluster processes the other activity. It may eventually complete, but
it will continue to be slow and potentially blocked until the CPU usage goes
down. As an example, if you issue a lot of resource-intensive queries at once,
that might spike the CPU.

To see memory and CPU usage for your cluster in the Materialize console, go to
https://console.materialize.com/, click the **“Clusters”** tab in the
navigation bar, and click on the cluster name.

#### Address

Your query may have been the root cause of the increased memory and CPU usage,
or it may have been something else happening on the cluster at the same time.
To troubleshoot and fix memory and CPU usage, follow the steps in the
[dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

For guidance on how to reduce memory and CPU usage for this or another query,
take a look at the [indexing and query optimization](#indexing-and-query-optimization)
and [computation-intensive queries](#computation-intensive-queries) sections
above.

If your query was the root cause, you'll need to kill it for the cluster's
memory or CPU to go down. If your query was causing an OOM, the cluster will
continue to be in an "OOM loop" - every time the cluster restarts, the query
restarts executing automatically then causes an OOM again - until you kill the
query.

If your query was not the root cause, you can wait for the other activity on the
cluster to stop and memory/CPU to go down, or switch to a different cluster.

If you've gone through the dataflow troubleshooting and do not want to make
any changes to your query, consider [sizing up your cluster](/sql/create-cluster/#size). A larger size cluster will provision more memory and CPU resources.
