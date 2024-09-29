# Moving source and sinks

Issue: [#17417](https://github.com/MaterializeInc/database-issues/issues/5050)

## Context

Materialize's API today allows users to specify a target cluster for sources,
sinks and materialized views on creation, but does not allow for changing the
cluster at a later time. To change the cluster, users have to drop and recreate
the object.

This is problematic:
* Sources almost always have downstream dependencies that need to be recreated,
  too.
* Sinks require users to recreate the topic in the downstream Kafka broker.
* Materialized views can have downstream dependencies.

## Goals

* Users can change the cluster on which a source, sink, or materialized view
  runs.

## Non-Goals

* Mixing compute and storage workload on the same cluster.

## Overview

We'll add support for the following declarative management commands to
Materialize:

```sql
-- Move a source to cluster `clstr`:
ALTER SOURCE src SET CLUSTER clstr;

-- Move a sink to cluster `c2`:
ALTER SINK snk SET CLUSTER c2;

-- Move a materialized view to cluster `prod`:
ALTER MATERIALIZED VIEW my_view SET CLUSTER prod;
```

This corresponds to the `CREATE` syntax where the user specifies the cluster
with an `CLUSTER` parameter.

Note that this is _intentionally_ lacking parens
(`ALTER SOURCE src SET (CLUSTER = clstr);`).
This is for consistency with the analogous PostgreSQL command
`ALTER TABLE ... SET {SCHEMA|TABLESPACE} {schema|tblspc}`.
The rule is that only parameters configured in the `WITH` block go inside the
parens; parameters that have dedicated syntax, like schemas, tablespaces, and
clusters, get dedicated `ALTER ... SET` syntax too.

## Detailed description

The `ALTER SOURCE`, `ALTER SINK` and `ALTER MATERIALIZED VIEW` statements will
learn a new option `SET CLUSTER clstr`:
* The cluster name `clstr` must match an existing cluster.
* The usual constraints apply. For sources and sinks, the cluster must either be
  empty or only host sources and sinks. The replication factor must be 0 or 1.
  For materialized views, the cluster must not host sources or sinks.
* Clusters can be managed or unmanaged.

When the command updates the cluster, Materialize will immediately drop the
existing source, sink, or materialized view and recreate it in the target
cluster.

> **Warning**
>
> In the initial implementation, changing the cluster associated will result in
> downtime while the object recreates. During this time, the source, sink, or
> materialized view will not produce new data.
>
> We hope to change the behavior in a future release so that no downtime is
> incurred, but this will require smarter scheduling.

Users may not schedule compute objects together with sources and sinks.
Attempting to schedule incompatible objects will result in an error like the
following:

```sql
ALTER SOURCE src SET CLUSTER compute_cluster;
ERROR: cannot alter source cluster to cluster containing indexes or materialized views
```

Users may not schedule sources or sinks on clusters with an incompatible
replication factor. Attempting to do so will result in an error like the
following:

```sql
ALTER SOURCE src SET CLUSTER cluster;
ERROR: cannot alter source cluster to cluster with more than one replica
```

Users cannot move system objects to different clusters, and users cannot move
user objects to system clusters.

### Linked clusters

Sources and sinks can have linked clusters, which are clusters owned by the
source or sink. Altering clusters of sources and sinks will cause any previously
linked cluster to be dropped.

We do not support moving a source or sink back to a linked cluster. If there is
the need to move sources or sinks back to a linked cluster, we could use the
following syntax:

```sql
ALTER SOURCE src RESET CLUSTER
```

## Alternatives

* We could wait to improve the API until we have unified clusters.

* We could support moving a source or sink to a linked cluster when either a size
  value was previously specified, or explicitly set on the `ALTER` command.

### `SET IN CLUSTER`

We rejected the following alternative syntax using `IN`:
```sql
ALTER SOURCE src IN CLUSTER clstr;
```

This is to avoid potential future conflict with setting parameters that
explicitly require `IN`.
