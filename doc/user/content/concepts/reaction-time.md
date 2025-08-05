---
title: Reaction Time, Freshness, and Query Latency
description: "Learn about indexes in Materialize."
menu:
  main:
    parent: concepts
    weight: 1
    identifier: 'concepts-reaction-time'
---

In operational data systems, the performance and responsiveness of queries depend not only on how fast a query runs, but also on how current the underlying data is. This page introduces three foundational concepts for evaluating and understanding system responsiveness in Materialize:

* **Freshness**: the time it takes for a change in an upstream system to become visible in the results of a query.
* **Query latency**: the time it takes to compute and return the result of a SQL query once the data is available in the system.
* **Reaction time**: the total delay from data change to observable result.

Together, these concepts form the basis for understanding how Materialize enables timely, accurate insights across operational and analytical workloads.

---

## Freshness

**Freshness** measures the time it takes for a change in an upstream system to become visible in the results of a query. In other words, it captures the end-to-end latency between when data is produced and when it becomes part of the transformed, queryable state.

| System         | Performance  | Explanation                |
| -------------- | ------------ | -------------------------- |
| OLTP Database  | Excellent    | Freshness is effectively zero. Queries run directly against the source of truth, and changes are visible immediately. |
| Data Warehouse | Poor (stale) | Freshness is often poor due to scheduled batch ingestion. Changes may take minutes to hours to propagate.                  |
| Materialize    | Excellent    | Freshness is low, typically within milliseconds to a few seconds, due to continuous ingestion and incremental view maintenance.                  |

### Monitoring Freshness

You can monitor data freshness in Materialize by querying wallclock lag measurements from the [`mz_internal.mz_wallclock_global_lag`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag) system catalog view.
Wallclock lag indicates how far behind real-world wall-clock time your data objects are, helping you understand freshness across your materialized views, indexes, and sources.

```sql
SELECT object_id, lag
FROM mz_internal.mz_wallclock_global_lag;
```

---

## Query Latency

**Query latency** refers to the time it takes to compute and return the result of a SQL query once the data is available in the system. It is affected by the system's execution model, indexing strategies, and the complexity of the query itself.

| System         | Performance  | Explanation                |
| -------------- | ------------ | -------------------------- |
| OLTP Database  | Poor (slow)  | Optimized for transactional workloads and point lookups. Complex analytical queries involving joins, filters, and aggregations tend to exhibit poor query latency. |
| Data Warehouse | Excellent | Designed for analytical processing, and generally provide excellent query latency even for complex queries over large datasets. |
| Materialize    | Excellent    | Maintains low query latency by incrementally updating and indexing the results of complex views. Queries that read from indexed views typically return results in milliseconds. |

---

## Reaction Time

**Reaction time** is defined as the sum of freshness and query latency. It captures the total time from when a data change occurs upstream to when a downstream consumer can query and act on that change.

```
reaction time = freshness + query latency
```

This is the most comprehensive measure of system responsiveness and is particularly relevant for applications that depend on timely and accurate decision-making.

| System         | Reaction Time |
| -------------- | ------------- |
| OLTP Database  | High          |
| Data Warehouse | High          |
| Materialize    | Low           |


## Example

Consider an e-commerce application that needs to monitor order fulfillment rates in real time. This requires both timely access to new orders and the ability to compute aggregates across multiple related tables.

Let’s compare how this plays out across three systems:

| **System**        | **Data Freshness**                                                                                                              | **Query Latency**                                                                                                                                      |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| **OLTP System**   | The order and fulfillment data is always current, as queries run directly against the transactional system.                   | Computing fulfillment rates involves joins and aggregations over multiple tables, which transactional databases are not optimized for. Queries may be slow or resource-intensive. |
| **Data Warehouse**| The data is typically ingested in batches, so it may lag behind by minutes or hours. Freshness depends on the ETL schedule.   | Analytical queries, including aggregations and joins, are well-optimized and typically return quickly.                                                |
| **Materialize**   | Updates stream in continuously from the operational database. Materialize incrementally maintains the fulfillment rate.       | Because the computation is performed ahead of time and maintained in an indexed view, queries return promptly—even for complex logic.                 |

## Design Implications

Optimizing reaction time is essential for building systems that depend on timely decision-making, accurate reporting, and responsive user experiences. Materialize enables this by ensuring:

* **Low freshness lag**: Data changes are ingested and transformed in near real time.
* **Low query latency**: Results are precomputed and maintained through indexed views.
* **Minimal operational complexity**: Users define transformations using standard SQL. Materialize handles the complexity of incremental view maintenance internally.

This architecture removes the traditional trade-off between fast queries and fresh data. Unlike OLTP systems and data warehouses, which optimize for one at the expense of the other, Materialize provides both simultaneously.

---

## Summary

| Concept       | Definition                                    | How Materialize Optimizes It                     |
| ------------- | --------------------------------------------- | ------------------------------------------------ |
| Freshness     | Time from upstream change to queryability     | Streaming ingestion + incremental transformation |
| Query Latency | Time to execute and return results of a query | Indexes + real-time maintained views             |
| Reaction Time | Total time from data change to insight        | Combines low freshness and low query latency     |

Materialize is built to minimize all three. The result is a system that delivers fast, consistent answers over fresh data, enabling use cases that were previously too costly or complex to implement.
