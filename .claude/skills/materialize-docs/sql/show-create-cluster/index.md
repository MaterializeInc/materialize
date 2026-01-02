---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-cluster/
complexity: advanced
description: '`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE
- SHOW CREATE CLUSTER
product_area: Indexes
status: stable
title: SHOW CREATE CLUSTER
---

# SHOW CREATE CLUSTER

## Purpose
`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.



`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.

## Syntax

This section covers syntax.

```sql
SHOW CREATE CLUSTER <cluster_name>
```text

For available cluster names, see [`SHOW CLUSTERS`](/sql/show-clusters).

## Examples

This section covers examples.

```sql
SHOW CREATE CLUSTER c;
```text

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 c                | CREATE CLUSTER "c" (INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = '100cc', SCHEDULE = MANUAL)
```

## Privileges

There are no privileges required to execute this statement.


## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`CREATE CLUSTER`](../create-cluster)

