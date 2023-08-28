# Unbilled replicas

- [#20317](https://github.com/MaterializeInc/materialize/issues/20317)

## Context

Support requires the ability to dynamically add resources to a user's environment
without incurring substantial costs for the user. For example, when support
notices that after a version upgrade, Materialize needs additional compute
resources, support should be able to add such resources for a limited amount of
time without requiring explicit approval and complicated reimbursement for the
customer.

We extend Materialize with a concept of unbilled replicas, which server compute
traffic but are not charged to a customer's account.

To achieve this, we extend clusters and their replicas with concept of replica sets.
A replica set defines properties shared by a partition of replicas within a cluster.
We allow internal users to create replica sets with specific billing information to add free or cost-reduced resouces to user environemnts, without changing the structure of a user's deplyoment.

## Goals

We introduce unbilled clusters by the following means:
* Introduce replica sets.
* Add a billing property to replica sets.
* Record replica set properties in audit events.

## Non-Goals

We do not plan to do the following:
* Allow customers general access to replica sets.

## Overview

We introduce the concept of replica sets, which capture partitions
of replicas sharing configuration aspects. Replica sets can co-exist, and a default
replica set captures the current configuration of a cluster. Each replica is a member of a single
replica set.

At this time, we only support replica sets for internal users, but do not
give customers the permission to directly interact with replica sets. This
is to give us time to validate the feature, and decide on reserved names.

## Detailed description

We extend clusters to support multiple replica configurations, and name each a replica set.
A replica set describes the properties of a partition of replicas within a cluster.
A cluster can have several replica sets, and each replica is part of a single replica set.

Multiple replica set within a cluster can co-exist simultaneously, and can have different parameters.
We do not change that all replicas in a cluster execute equivalent commands and participate equally in producing responses.

```sql
-- Creating a managed replica set without billing
CREATE REPLICA SET cluster.support WITH (SIZE '3xlarge', BILLED AS 'unbilled');
-- Drop the replica set, releasing any associated resource.
DROP REPLICA SET cluster.support;

-- Alternatively, create an unmanaged replica set without billing.
CREATE REPLICA SET cluster.support WITH (MANAGED = FALSE, BILLED AS 'unbilled');
-- Create a replica in the replica set.
CREATE CLUSTER REPLICA cluster.r1 SIZE '3xlarge' IN REPLICA SET support;
-- Drop the replica set, also dropping replica `r1`.
DROP REPLICA SET cluster.support CASCADE;
```

### Replica sets and replicas

At the moment, clusters only contain replicas.
With the introduction of replica sets, we change this property to allow different kinds of objects as part of a cluster.
Replicas and replica sets share a common cluster namespace, so their names need to be unique within a cluster, but can be reused across clusters.

At this time, users do not need to work with replica sets if they don't choose to.
Each cluster has an implicit replica set (a.k.a, the `NULL` replica set?) that they interact with using the managed or unmanaged cluster SQL API.

The following example demonstrates using the implicit replica set:
```sql
CREATE CLUSTER clname SIZE '3xlarge';
```

### Resources within replica sets

Replicas are part of a replica sets, and their lifetime is tied to that of the replica set:

```sql
-- Cluster `cl` has an implicit replica set with one replica `r1`
DROP REPLICA SET cl.default;
ERROR: Replica set cl.default has objects
HINT: Drop objects first, or specify CASCADE.

DROP REPLICA SET cl.default CASCADE;
```

### Managed and unmanaged replica sets

We can mix managed and unmanaged replica sets:

```sql
-- Create the a cluster with an implicit managed replica set, which include replicas `cl.r1`, `cl.r2`:
CREATE CLUSTER cl SIZE '3xsmall', REPLICATION FACTOR 2;
-- Create the `p1` replica set, which includes replica `cl.p1_r1`:
CREATE REPLICA SET cl.p1 REPLICAS (p1_r1 SIZE 'large');
```

This requires a change to the names of replicas in managed clusters. Instead of
only using the names `r1..rN`, we could use the replica set name, or `default`, as a
prefix.

The syntax to drop replicas stays the same:

```sql
-- Drop replica a replica:
DROP CLUSTER REPLICA cl.r1;
```

### Altering replica sets

* Similar to altering clusters
  ```sql
  ALTER REPLICA SET cl.p1 SET (REPLICATION FACTOR 0);
  ```
* Altering replica set name: Rename the replica set, rename replicas for managed replica sets
  ```sql
  ALTER REPLICA SET cl.p1 RENAME TO p2;
  ```
* Should we allow moving replicas between replica sets?
  ```sql
  ALTER CLUSTER REPLICA cl.r1 IN REPLICA SET support;
  ERROR: Cannot move billed replica to unbilled replica set.
  ```
* Can we use RBAC to limit what users can do with unbilled replica sets?
  * Owner can modify
  * Owner can change owner
  * Cluster owner can drop

### System catalog

We need to present replica sets in the system catalog.
We extend the `mz_cluster_replicas` table with a `replica_set_id` column that names the replica set ID, or `NULL` if the replica is part of the implicit default replica set.

We add a `mz_replica_sets` table that has the following columns:
* `cluster_id`: The ID of the cluster the replica set belongs to.
* `replica_set_id`: The ID of the replica set within the cluster namespace.
* `managed`: Whether the replica set is a managed. `true` if managed, `false` if unmanaged.
* `size`: If `managed`, the size of replicas. `NULL` otherwise.
* `replication_factor`:  If `managed`, the replica replication factor. `NULL` otherwise.
* Introspection settings, which need to be defined. They are currently not exposed in the catalog.

An open question is whether this data should be presented as columns or a JSON blob.

### Stash

The stash currently stores replicas as the only items within replicas.
We need to change this such that replicas are a variant of items, with replica sets being the alternative.

### Billing

Materialize records when a user creates and drops cluster replicas. We need to
log unbilled replicas differently, either indicating in the audit log that
someone else covers their cost, or not logging events for unbilled replicas in
the audit log.

Not logging cluster replica events has several disadvantages, most importantly
that we lack an audit trail of events. For this reason alone, we'd like to
include unbilled replicas in the audit log, but clearly marked as such.

```sql
-- Create an unbilled replica set
CREATE REPLICA SET cl.support BILLED AS 'free', SIZE 'xlarge';
```

The `BILLED` setting is reserved for Materialize and cannot be used by users.

## Alternatives

### Cluster replica properties

Instead of introducing replica sets, we could encode the information directly on
replicas. This has the benefit that we don't have to introduce a novel concept,
but it doesn't integrate well with managed clusters.

To avoid the interaction with managed clusters, we could allow unbilled replicas
only for unmanaged clusters where there are no constraints across replicas.

### Specific billing information

Instead of adding a boolean property indicating whether a replica is billed to a
customer, we could add a proper string indicating _who_ should be billed for a
replica. Even if the customer isn't billed, we'd want to track the spending for
accounting purposes.

In the future, this could allow customers to be more specific as to who should
be billed based on different billing profiles.

## Open questions

* Are we convinced that replica sets are worth adding?
* Replica sets would be interesting for the following features:
  * Customer-specific scheduling of replica sets to switch between business hours
    and off-hours replica configurations.
  * Namespacing for clusters during graceful reconfigurations.
  * Namespacing for clusters during automatic load-based reconfiguration.
  * Blue/green deployments where sets replicas as a whole could be moved.
