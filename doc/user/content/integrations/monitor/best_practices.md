---
title: "Monitoring best practices"
description: "Review some important considerations for your Materialize resources"
menu:
  main:
    parent: ""
    name: 
---

This page highlights some of the steps you can take to diagnose and prevent
`oomkilled` errors in Materialize.

Understanding your Materialize environment is crucial to maintaining a healthy
workflow. There are some potential issues to keep in mind when you are using
Materialize and we offer guidance on how to handle these issues and how to
monitor your system to prevent them in the future.

## Memory

Materialize performs many computations in memory. The computations occur across
your cluster and replicas by using the RAM allocated when the cluster is
created. In-memory processing is faster than traditional disk data access, but
still requires careful use-case consideration.

One symptom of memory issues is the Out of Memory (OOM) error. OOM errors occur
when operational costs have exceeded the allocated memory available to the
cluster. 

Several factors contribute to out of memory errors:

- Undersizing cluster replicas
- Incorrectly indexing
- Creating and hydrating several objects at once
- Increase in data throughput

Let's review some of these factors and how to avoid memory spikes.

## Right-sizing

The majority of memory errors can be avoided with sufficiently sized [cluster replicas](https://materialize.com/docs/sql/create-cluster-replica/#sizes).
That can differ based on your usage and needs, but one way to determine sizing
is to review the currently available replica sizes in the `mz_internal` system
catalog.

```sql
materialize=> SELECT processes, credits_per_hour, SUM(memory_bytes) / (1024.0 * 1024.0 * 1024.0) AS memory_gb, size FROM mz_internal.mz_cluster_replica_sizes GROUP BY 1, credits_per_hour, size ORDER BY credits_per_hour;

processes | credits_per_hour | memory_gb |  size
-----------+------------------+-----------+---------
 1         |             0.25 |         4 | 3xsmall
 1         |              0.5 |         8 | 2xsmall
 1         |                1 |        16 | xsmall
 1         |                2 |        48 | small
 1         |                4 |       112 | medium
 1         |                8 |       235 | large
 1         |               16 |       470 | xlarge
 2         |               32 |       470 | 2xlarge
 4         |               64 |       470 | 3xlarge
 8         |              128 |       470 | 4xlarge
 16        |              256 |       470 | 5xlarge
 32        |              512 |       470 | 6xlarge
(12 rows)
```

The query above represents the replica sizes and the corresponding compute
resources they have access to. Based on your average data output, you can use
this table to determine the correct size for your replicas.

The [`mz_interal` system catalog](https://materialize.com/docs/sql/system-catalog/mz_internal/) returns metadata about Materialize itself and
the values and structure are subject to change.

## Incorrect indexing

Indexes maintain query results in memory. Future queries can then access
the query results faster than recomputing the query.

In smaller-sized clusters and sources, you are limited by the amount of memory you can use
for an index.

Consider restructuring data into smaller replicas or sources across your region.

## Object creation

At some point, you will need to add or recreate objects in your cluster. If you
need to create several new objects or are using infrastructure management tools
like dbt or Terraform to create Materialize objects, you may hit a memory limit
upon creation.

Once you create an object, it goes through **data hydration** which means your
data begins to queue in the object in order to "catch up" with the latest
records. The hydration process is memory intensive because Materialize retrieves
and processes a large backlog of data.

To ensure your data hydration process does not consume all the available memory
in your cluster, consider the following recommendations:

[//]: # "TODO(tr0njavolta) "Some recommendations"

## Increased data

An increase of data throughput in your Materialize region may impact your memory
usage because you initially sized your cluster with a specific throughput in
mind.

## Observability conventions

We've provided some guies to help you add Materialize to your current monitoring
solution:

[//]: # "TODO(tr0njavolta) "Add links"
- Datadog
- Grafana
