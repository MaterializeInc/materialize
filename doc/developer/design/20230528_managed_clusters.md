# Managed clusters

Epic: [#19547](https://github.com/MaterializeInc/materialize/issues/19547)

## Context

Materialize's API today requires that users control a clusters's size and
replication factor by manually managing its replicas. The following is a quick
tour of the SQL commands used for these operations.

To create a cluster, users must explicitly specify the replica set:

```sql
-- Create a cluster with two replicas of different sizes.
CREATE CLUSTER c REPLICAS (
    r1 (SIZE 'small'),
    r2 (SIZE 'medium')
);
```

To decrease the replication factor of a cluster, they must choose which replica
to discard:

```sql
-- Decrease the replication factor of `c` to 1.
DROP CLUSTER REPLICA c.r2;
```

To increase the replication factor, they must create the appropriate number of
new replicas:

```sql
-- Increase the replication factor of `c` to 3.
CREATE CLUSTER REPLICA c.r2 SIZE 'small';
CREATE CLUSTER REPLICA c.r3 SIZE 'small';
```

To resize a cluster, they must create new replicas at the new size with the
desired replication factor:

```sql
-- Change the size of `c` from `small` to `medium`.

-- Create new bigger replicas.
CREATE CLUSTER REPLICA c.r1_big SIZE 'medium';
CREATE CLUSTER REPLICA c.r2_big SIZE 'medium';
CREATE CLUSTER REPLICA c.r3_big SIZE 'medium';

-- Drop old smaller replicas.
DROP CLUSTER REPLICA c.r1;
DROP CLUSTER REPLICA c.r2;
DROP CLUSTER REPLICA c.r3;
```

We've received overwhelming feedback from education, field engineering, and end
users that manual management of replicas is confusing and/or frustrating.
Questions have included:

  * Is the name of each replica significant?
  * If replicas should always be named `r1`, `r2`, ..., why do I need to specify
    their names?
  * Does it ever make sense to have a cluster with differently sized replicas?
  * How can you have a cluster with only one replica? That's not how the English
    word "replica" works.
  * How can you have a cluster with zero replicas?
  * Why can't I type `ALTER CLUSTER ... SET (SIZE)`?

The key source of friction is that users think in terms of the fundamental
properties of a cluster—its **size** and its **replication factor**—but today's
API requires that users imperatively manage replicas, rather than declaratively
specifying the cluster's size and replication factor.

We've always intended to build a declarative API on top of the existing API. The
original [cluster replica design] from April 2022 sketched this declarative API
that would allow users to create a cluster with three replicas that
automatically scaled up and down in response to load:

```sql
CREATE CLUSTER foo REPLICATION FACTOR 3, MIN SIZE 'small', MAX SIZE 'xlarge'
```

As described, this API requires "[dynamic cluster scheduling]", which is
difficult and has many unknowns, so we intentionally chose to not work on this
in the past year.

The need for improvement has recently become more urgent, as the designers of
both our [Terraform provider] and our [web console] have considered layering on
a better cluster management experience in their respective tools. But the right
place to solve this is in the database itself, so that we don't need to write
bespoke logic in each tool that is downstream of the SQL API, and so that users
who use the SQL API directly get the improved experience too.

So, this design document proposes a new course: a new cluster management API
that solves most of the ergonomic issues with the existing API but does not
require the full generality of dynamic cluster scheduling.

## Goals

* Users can manage a cluster's size and replication factor without thinking
  about individual replicas.
* We can implement the design in weeks, not quarters.

## Non-Goals

* Automatically resizing clusters without downtime.
* Automatically resizing clusters in response to load.
* Automatically rebalancing replicas across availability zones.

## Overview

We'll add support for the following declarative cluster management commands to
Materialize:

```sql
-- Create a managed cluster with one small replica.
CREATE CLUSTER foo SIZE 'small';

-- Size up the cluster. IN THE FUTURE this will gracefully
-- change the size without downtime. AT PRESENT, this will cause
-- downtime for as long as it takes the new replica(s) to rehydrate.
--
-- If you NEED graceful resizing, convert this cluster to an unmanaged
-- cluster and handle dropping/creating replicas yourself.
ALTER CLUSTER foo SET (SIZE 'medium');

-- Add two new replicas automatically.
ALTER CLUSTER foo SET (REPLICATION FACTOR 3);

-- Turn off the cluster for the night.
ALTER CLUSTER foo SET (REPLICATION FACTOR 0);

-- You can also create a cluster with multiple replicas from the get go.
CREATE CLUSTER foo2 SIZE 'small', REPLICATION FACTOR 2;
```

## Detailed description

### Managed clusters

We'll introduce the concept of a **managed** cluster. A managed cluster is one
with a declared size and replication factor, where Materialize is responsible
for ensuring the replica set matches the declared size and replication factor.
The replicas of a managed cluster are visible in the system catalog, but cannot
be directly modified by users.

Existing clusters, where users control the replica set manually via
`{CREATE|DROP} CLUSTER REPLICA`, will be deemed **unmanaged** clusters.

### `CREATE CLUSTER`

The `CREATE CLUSTER` statement will learn three new options:

  * `SIZE`, a string, which specifies the desired size of the replicas in the
    cluster.
  * `MANAGED`, a boolean, which specifies whether the cluster is managed or
    unmanaged.
  * `REPLICATION FACTOR`, an integer, which specifies the desired number of
    replicas.

In addition to this, the `CREATE CLUSTER` statement will learn specialized
configuration parameters for replicas to control introspection, the
arrangement idle merge effort, and availability zone.

Users must specify either the `SIZE` or `REPLICAS` option when creating a
cluster. The two options may not be specified simultaneously.

The `MANAGED` option must be true if `SIZE` is specified or false if `REPLICAS`
is specified. It takes on the appropriate default if unspecified. We expect that
this option will never be explicitly specified in practice, but we accept it in
order to be consistent with `ALTER CLUSTER`.

The `REPLICATION FACTOR` option may only be specified if `MANAGED` is true. If unspecified, it defaults to `1`.

For managed clusters, Materialize will automatically create replicas named
`r1`, `r2`, ..., `rN`, where `N` is the desired replication factor, all of
the size specified by the `SIZE` option.

**Note:** The documentation should call out that the replica naming scheme may
change in future versions of Materialize.

For unmanaged clusters, `CREATE CLUSTER`'s behavior remains unchanged.

**Note:** Managed clusters can not be used to host sources and sinks, or can be
used as linked clusters.

### `ALTER CLUSTER`

#### Changing size and replication factor

We'll extend the `ALTER CLUSTER` statement to support altering the `SIZE` and
`REPLICATION FACTOR` of a cluster using the usual syntax for `ALTER ... SET`
and `ALTER ... RESET` commands:

```sql
-- Change the size of a cluster to `medium`.
ALTER CLUSTER c SET (SIZE = 'medium');
-- Change the replication factor of a cluster to 2.
ALTER CLUSTER c SET (REPLICATION FACTOR = 2);
-- Simultaneously change the size and replication factor of a cluster.
ALTER CLUSTER c SET (SIZE = 'medium', REPLICATION FACTOR = 2);
-- Change the replication factor of a cluster to the default value of 1.
ALTER CLUSTER c RESET (REPLICATION FACTOR);
```

When the command updates the cluster's size, Materialize will immediately drop
all existing replicas of the cluster and recreate them with the same names with
the new size.

> **Warning**
>
> In the initial implementation, changing a cluster's size will result in
> downtime while the new replicas rehydrate. This may be surprising to users.
>
> We hope to change the behavior in a future release so that no downtime is
> incurred, but this will require [dynamic cluster scheduling].

When the command increases the cluster's replication factor, Materialize will
immediately create as many new replicas as necessary to meet the desired
replication factor. It will name the replicas such that the resulting replica
set adheres to the `r1`, `r2`, ..., `rN` naming scheme.

When the command decreases the cluster's replication factor, Materialize will
immediately drop as many replicas as necessary to meet the desired replication
factor. It will choose replicas to drop so that the resulting replica set
adheres to the `r1`, `r2`, ..., `rN` naming scheme.

Users may only modify these options for managed clusters. Attempting to set the
size or replication factor for an unmanaged cluster will result in an error like
the following:

```
> CREATE CLUSTER c REPLICAS ();
> ALTER CLUSTER c SET (SIZE = 'medium');
ERROR: cannot set SIZE option for unmanaged cluster "c"
HINT: Manually manage replicas using CREATE CLUSTER REPLICA and DROP CLUSTER REPLICA,
or convert the cluster to a managed cluster by running ALTER CLUSTER c SET (MANAGED).
```

#### Converting an unmanaged cluster to a managed cluster

`ALTER CLUSTER` will support enabling the `MANAGED` option for a cluster to
convert an unmanaged cluster to a managed cluster:

```sql
ALTER CLUSTER c SET (MANAGED = true);
ALTER CLUSTER c SET (MANAGED); -- Shorthand for the above
```

Conversion is possible if and only if all of the following conditions are met:

  * All replicas of the cluster have the same size `S`.
  * The replicas are named `r1`, `r2`, ..., `rN`, with no gaps in the numbering.
    (Zero replicas trivially matches this pattern with `N=0`.)

The `SIZE` option of the cluster will be set to `S` and the
`REPLICATION FACTOR` will be set to `N`.

Because the conditions for conversion are strict

```sql
> CREATE CLUSTER c REPLICAS (
  r1 (SIZE 'small'),
  r2 (SIZE 'medium')
);
> ALTER CLUSTER c SET (MANAGED);
ERROR: cannot convert cluster "c" to a managed cluster
DETAIL: Replicas do not all have the same size.

> CREATE CLUSTER c REPLICAS (
  foo (SIZE 'small'),
  bar (SIZE 'small'),
  r3 (SIZE 'small')
);
> ALTER CLUSTER c SET (MANAGED);
ERROR: cannot convert cluster "c" to a managed cluster
DETAIL: The following replicas do not match required naming pattern `r1`, `r2`, ...
  foo
  bar

> CREATE CLUSTER c REPLICAS (
  r1 (SIZE 'small'),
  r3 (SIZE 'medium')
);
> ALTER CLUSTER c SET (MANAGED);
ERROR: cannot convert cluster "c" to a managed cluster
DETAIL: The replicas do not match required naming pattern `r1`, `r2`, .... The
first missing replica is "r2".
```

#### Converting a managed cluster to an unmanaged cluster

`ALTER CLUSTER` will support disabling the `MANAGED` option for a cluster to
convert a managed cluster to an unmanaged cluster:

```sql
ALTER CLUSTER c SET (MANAGED = false);
```

There are no constraints on the conversion. All managed clusters can be
converted to unmanaged clusters.

The conversion preserves the existing replicas of the cluster. Users can then
create or drop replicas of the cluster using `{CREATE|DROP} CLUSTER REPLICA`.

#### `{CREATE|DROP} CLUSTER REPLICA`

The `CREATE CLUSTER REPLICA` and `DROP CLUSTER REPLICA` statements will be
restricted to operations on replicas of an unmanaged cluster.

Materialize will produce an error message like the following when attempting
to manually manipulate replicas of a managed cluster:

```
> CREATE CLUSTER c SIZE 'small';
> DROP CLUSTER REPLICA c1.r1;
ERROR: cannot drop replica of managed cluster "c"
HINT: Use ALTER CLUSTER to change the cluster's size and replication factor, or
convert the cluster to an unmanaged cluster by running ALTER CLUSTER c SET (MANAGED = false).
```

#### `ALTER CLUSTER [REPLICA] ... RENAME TO`

`ALTER CLUSTER` and `ALTER CLUSTER REPLICA` will learn to support renaming
clusters and cluster replicas, respectively:

```sql
-- Rename cluster c1 to c2.
ALTER CLUSTER c1 RENAME TO c2;

-- Rename replica r1 of cluster c1 to r2.
ALTER CLUSTER REPLICA c1.r1 RENAME TO r2;
```

`ALTER CLUSTER REPLICA` will not permit renaming a replica of a managed cluster.

#### `mz_clusters`

`mz_clusters` will grow three new columns:

Field                | Type               | Meaning
---------------------|--------------------|--------
`managed`            | `bool`             | Whether the cluster has automatically managed replicas.
`size`               | `text`             | If the cluster is managed, the desired size of the cluster's replicas. If the cluster is unmanaged, `NULL`.
`replication_factor` | `bigint`           | If the cluster is managed, the desired number of replicas of the cluster. If the cluster is unmanaged, `NULL`.

## Alternatives

We could wait to improve the API until we've built [dynamic cluster scheduling].

## Open questions

* Is it acceptable that `ALTER CLUSTER ... SET (SIZE = ...)` causes downtime?

* Should we infer the `MANAGED` option based on whether `SIZE` is set? If so,
  users would convert to a managed cluster by running
  `ALTER CLUSTER unmanaged SET (SIZE = ...)`, and convert to an unmanaged
  cluster by running `ALTER CLUSTER managed RESET (SIZE)`.

  I personally prefer the explictness of
  `ALTER CLUSTER ... SET (MANAGED = ...)` .


[cluster replica design]: https://github.com/MaterializeInc/materialize/blob/d7101d4b952f9eb4f1185fdcceacd64c4d151de5/doc/developer/design/20220413_cluster_replica.md#future-work
[dynamic cluster scheduling]: https://github.com/MaterializeInc/materialize/issues/13870
[terraform provider]: https://github.com/MaterializeInc/terraform-provider-materialize/issues/145
[web console]: https://materializeinc.slack.com/archives/CU7ELJ6E9/p1683568811596419?thread_ts=1683565139.938549&cid=CU7ELJ6E9
