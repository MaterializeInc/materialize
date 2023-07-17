# Moving source and sinks

Issue: [#17417](https://github.com/MaterializeInc/materialize/issues/17417)

## Context

Materialize's API today allows users to specify a target cluster for sources
and sinks on creation, but does not allow for changing the cluster at a later
time. To change the cluster, users have to drop and recreate the object.

This is problematic:
* Sources almost always have downstream dependencies that need to be recreated,
  too.
* Sinks require users to recreate the topic in the downstream Kafka broker.

## Goals

* Users chan change the cluster on which a source or sink runs.

## Non-Goals

* Mixing compute and storage workload on the same cluster.

## Overview

We'll add support for the following declarative source and sink management
commands to Materialize:

```sql
-- Move a source to cluster `clstr`:
ALTER SOURCE src SET CLUSTER clstr;

-- Move a sink to cluster `c2`:
ALTER SINK snk SET CLUSTER c2;
```

Note that this is _intentionally_ lacking parens
(`ALTER SOURCE src SET (CLUSTER = clstr)`;).
This is for consistency with the analogous PostgreSQL command
`ALTER TABLE ... SET {SCHEMA|TABLESPACE} {schema|tblspc}`.
The rule is that only parameters configured in the `WITH` block go inside the
parens; parameters that have dedicated syntax, like schemas, tablespaces, and
clusters, get dedicated `ALTER ... SET` syntax too.

## Detailed description

The `ALTER SOURCE` and `ALTER SINK` statements will learn a new option `SET CLUSTER clstr`:
* The cluster name `clstr` must match an existing cluster.
* The usual constraints apply. At the moment, the cluster must either be empty
  or only host sources and sinks. The replication factor must be 0 or 1.
* Clusters can be managed or unmanaged.

When the command updates the cluster, Materialize will immediately drop the
existing source or sink and recreate it in the target cluster.

> **Warning**
> 
> In the initial implementation, changing the cluster associated will result in
> downtime while the source or sink recreate.
> 
> We hope to change the behavior in a future release so that no downtime is
> incurred, but this will require smarter source and sink scheduling.

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
ERROR: cannot source source cluster to cluster with more than one replica 
```

## Alternatives

We could wait to improve the API until we have unified clusters.

## Open questions

* Is the syntax accepted?
