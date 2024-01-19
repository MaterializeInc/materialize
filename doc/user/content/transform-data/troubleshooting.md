---
title: "Troubleshooting"
description: ""
menu:
  main:
    name: "How to troubleshoot common scenarios where Materialize is not working as expected."
    identifier: troubleshooting-transform
    parent: transform
    weight: 25
---

## Why is my query slow?

### Step 1: Inspect the query lifecycle
TODO -
Without this, we can also just suggest folks look into the each of the detection steps, sthg like "The below are all reasons why your query might be slow. You can follow the guidance to investigate whether one or more of these are happening to you."

### Step 2: Detect 

#### Lagging materialized views or indexes
When a materialized view or index upstream of your query is behind on processing, your query must wait for it to catch up before returning. This is how we ensure consistent results for all queries.

To see whether any materialized views or indexes are lagging you can:

1. Look at the workflow graphs for your objects in the Materialize console. Go to https://console.materialize.com/ and TODO.

OR

2. Do we even want to give someone the complicated WMR queries etc to fetch this themselves?

If you find that one of the upstream materialized views or indexes is lagging, this could be the cause of your query slowness.

*Do you have multiple materialized views chained on top of each other? Are you seeing small amounts of lag?*\
Tip: avoid intermediary materialized views where not necessary. Each chained materialized view incurs a small amount of processing lag from the previous one. <TODO add more guidance on avoiding chained mat views>

To troubleshoot and fix a lagging materialized view or index, follow the steps in the [dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

Some other things to consider:
*  If you've gone through the dataflow troubleshooting and do not want to make any changes to your query, another option to consider is [sizing up your cluster](/sql/create-cluster/#size).
* You can also consider changing your [isolation level](/get-started/isolation-level/), depending on the consistency guarantees that you need.
* You can also check whether you're using a [transaction](#Transactions) and follow the guidance there.

##### Transactions
Copy from https://materialize.com/docs/manage/troubleshooting/#transactions

#### Lagging or stalled source
TODO: similar to materialized views, checking the workflow graph for detection.
Redirect to [troubleshooting sources](/transform-data/source-troubleshooting) page.

#### Slow query execution
Query execution time is largely dependent on the amount of on-the-fly work that needs to be done in order to compute the result. There are a number of ways to reduce that.

##### Indexing and query optimization
If you haven’t already, you should consider an [index](/sql/create-index/) for your query, and read through the [optimizations](/transform-data/optimization) guide on how to optimize query performance in Materialize.

If you already created an index, you can confirm that the query is actually using the index. The tool for this is an [`EXPLAIN` plan](/sql/explain-plan/). If you run `EXPLAIN` for your query, and the index was correctly used, you should see the index you created in the section "Used Indexes". If you're not seeing the index in the explain plan, some things to troubleshoot:
* Are you running the query in the same cluster that you created the index? You must do so in order for the index to be used.
* Does the index's indexed expression (key) match up with how you're querying the data?

##### Computation-intensive queries
Another thing to be aware of, in Materialize, aggregations like `COUNT(*)` and limit constraints like `LIMIT 1` perform computation across the full underlying dataset being queried, so can be resource intensive. TODO: tips of what to do instead?


#### Other things to consider
##### Client-side latency
Copy from https://materialize.com/docs/manage/troubleshooting/#client-side-latency 

##### Result size
Smaller results lead to less time spent transmitting data over the network.
Todo: how to see your result size, maybe using activity log's time to last row and size

##### Cluster CPU
Another think to look at is the how busy cluster you're issueing queries on is. A busy cluster means your query might be blocked by some other processing going on. If you issue a lot of resource-intensive queries at once, that can spike the CPU.

The measure of cluster busyness is CPU. You can see the CPU by going to https://console.materialize.com/, clicking the "clusters" tab, and clicking into the cluster. You can also query the CPU via SQL:
```
SELECT cru.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization cru
LEFT JOIN mz_catalog.mz_cluster_replicas cr ON cru.replica_id = cr.id
LEFT JOIN mz_catalog.mz_clusters c ON cr.cluster_id = c.id
WHERE c.name = <CLUSTER_NAME>;
```


## Why is my query not responding?