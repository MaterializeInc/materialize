# Unbilled replicas

- [#20317)](https://github.com/MaterializeInc/materialize/issues/20317)

## Context

Support requires the ability to dynamically add resources to a user's environment
without incurring substantial costs for the user. For example, when support
notices that after a version upgrade, Materialize needs additional compute
resources, support should be able to add such resources for a limited amount of
time without requiring explicit approval and complicated reimbursement for the
customer.

We extend Materialize with a concept of unbilled replicas, which server compute
traffic but are not charged to a customer's account.

## Goals

We introduce unbilled clusters by the following means:
* Introduce cluster profiles.
* Add a billing property to cluster profiles.
* Record cluster profile properties in audit events.

## Non-Goals

We do not plan to do the following:
* Allow customers general access to cluster profiles.

## Overview

We introduce the notion of cluster profiles, which capture groups
of replica sharing configuration aspects. Profiles can co-exist, and a default
profile captures the current configuration.

At this time, we only support cluster profiles for internal users, but do not
give customers the permission to directly interact with cluster profiles. This
is to give us time to validate the feature, and decide on reserved names.

## Detailed description

We extend clusters to support multiple replica configurations, and name each a profile.
A cluster profile describes the properties of a set of replicas within a cluster.
A cluster can have several profiles, and each replica is part of a single profile.


```sql
-- Creating a managed cluster profile without billing
CREATE CLUSTER PROFILE cluster.support WITH (SIZE '3xlarge', BILLED = FALSE);
-- Drop the profile, releasing any associated resource.
DROP CLUSTER PROFILE cluster.support;

-- Alternatively, create an unmanaged profile without billing.
CREATE CLUSTER PROFILE cluster.support WITH (MANAGED = FALSE, BILLED = FALSE);
-- Create a replica in the profile, identifying the profile in the name.
CREATE CLUSTER REPLICA cluster.support.r1 SIZE '3xlarge';
-- Drop the profile
DROP CLUSTER PROFILE cluster.support CASCADE;
```

### Default profile

When a user doesn't specify a profile, they interact with the default profile:

```sql
CREATE CLUSTER clname SIZE '3xlarge';
-- Equivalent to:
CREATE CLUSTER clname;
CREATE CLUSTER PROFILE clname.default SIZE '3xlarge';
```

### Resources within profiles

Replicas are part of a profile, and their lifetime is tied to that of the profile:

```sql
-- Cluster `cl` has a profile `default` with one replica `r1`
DROP CLUSTER PROFILE cl.default;
ERROR: Cluster profile cl.default has objects
HINT: Drop objects first, or specify CASCADE.

DROP CLUSTER PROFILE cl.default CASCADE;
```

### Managed and unmanaged profiles

We can mix managed and unmanaged profiles:

```sql
-- Create cluster without profiles
CREATE CLUSTER cl;
-- Create `default` profile:
CREATE CLUSTER PROFILE cl.default SIZE '3xsmall', REPLICATION FACTOR 2;
-- Create `p1` profile:
CREATE CLUSTER PROFILE cl.p1 REPLICAS (r1 SIZE 'large');
```

This is equivalent to creating the default profile in-line:

```sql
-- Create default profile as part of the cluster:
CREATE CLUSTER cl SIZE '3xsmall', REPLICATION FACTOR 2;
-- Create `p1` profile:
CREATE CLUSTER PROFILE cl.p1 REPLICAS (r1 SIZE 'large');
```


We need to extend the syntax to dropping replicas of unmanaged profiles:

```sql
-- Drop replica from the default profile:
DROP CLUSTER REPLICA cl.r1;
-- Drop replica from specific profile:
DROP CLUSTER REPLICA cl.p1.r1;
```

Name resolution could either default to the `default` profile, or, if there is
only one profile, target the single profile.

### Billing

Materialize records when a user creates and drops cluster replicas. We need to
log unbilled replicas differently, either indicating in the audit log that
someone else covers their cost, or not logging events for unbilled replicas in
the audit log.

Not logging cluster replica events has several disadvantages, most importantly
that we lack an audit trail of events. For this reason alone, we'd like to
include unbilled replicas in the audit log, but clearly marked as such.

```sql
-- Create an unbilled profile
CREATE CLUSTER PROFILE cl.support BILLED = false, SIZE 'xlarge';
```

The `BILLED` setting is reserved for Materialize and cannot be used by users.

## Alternatives

### Cluster replica properties

Instead of introducing profiles, we could encode the information directly on
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

* Are we convinced that a cluster profiles feature is worth adding?
* Cluster profiles would be interesting for the following features:
  * Customer-specific scheduling of profiles to switch between business hours
    and off-hours replica configurations.
  * Namespacing for clusters during graceful reconfigurations.
  * Namespacing for clusters during automatic load-based reconfiguration.
  * Blue/green deployments where sets replicas as a whole could be moved.
