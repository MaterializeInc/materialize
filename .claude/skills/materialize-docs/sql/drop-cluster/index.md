---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-cluster/
complexity: intermediate
description: '`DROP CLUSTER` removes an existing cluster from Materialize.'
doc_type: reference
keywords:
- DROP THEM
- IF EXISTS
- DROP CLUSTER
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP CLUSTER
---

# DROP CLUSTER

## Purpose
`DROP CLUSTER` removes an existing cluster from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP CLUSTER` removes an existing cluster from Materialize.



`DROP CLUSTER` removes an existing cluster from Materialize. If there are indexes or materialized views depending on the cluster, you must explicitly drop them first, or use the `CASCADE` option.

## Syntax

This section covers syntax.

```mzsql
DROP CLUSTER [IF EXISTS] <cluster_name> [CASCADE|RESTRICT];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional.  If specified, do not return an error if the specified cluster does not exist.
`<cluster_name>` | The cluster you want to drop. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
**CASCADE** | Optional. If specified, remove the cluster and its dependent objects.
**RESTRICT** | Optional. Do not drop the cluster if it has dependencies. _(Default)_

## Examples

This section covers examples.

### Dropping a cluster with no dependencies

To drop an existing cluster, run:

```mzsql
DROP CLUSTER auction_house;
```text

To avoid issuing an error if the specified cluster does not exist, use the `IF EXISTS` option:

```mzsql
DROP CLUSTER IF EXISTS auction_house;
```bash

### Dropping a cluster with dependencies

If the cluster has dependencies, Materialize will throw an error similar to:

```mzsql
DROP CLUSTER auction_house;
```text

```nofmt
ERROR:  cannot drop cluster with active indexes or materialized views
```text

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP CLUSTER auction_house CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped cluster.


## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`DROP OWNED`](../drop-owned)

