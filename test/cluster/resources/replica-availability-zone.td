# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for the public mz_cluster_replicas.availability_zone column.
#
# That column is a BuiltinMaterializedView derived from the durable
# ClusterReplica JSON. The durable Managed location stores an
# `availability_zones` list, not a single zone. For an unmanaged cluster the
# list is the user's single AVAILABILITY ZONE pin (a 0- or 1-element list). For
# a managed cluster it is the provisioned AVAILABILITY ZONES pool. The public
# column must surface only the former and stay NULL for the latter, so the view
# joins the owning cluster to learn its managed-ness.
#
# environmentd is booted with a configured AZ pool (--availability-zone), which
# is what lets an AVAILABILITY ZONE pin be accepted at all.

# Unmanaged cluster with an AVAILABILITY ZONE pin on its replica.
> CREATE CLUSTER c_unmanaged_az REPLICAS (
    r1 (SIZE 'scale=1,workers=1', AVAILABILITY ZONE 'us-east-1b')
  )

# The pin is surfaced verbatim.
> SELECT r.name, r.availability_zone
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON r.cluster_id = c.id
  WHERE c.name = 'c_unmanaged_az'
r1 us-east-1b

# Managed cluster with an AVAILABILITY ZONES pool.
> CREATE CLUSTER c_managed_az
    SIZE = 'scale=1,workers=1',
    REPLICATION FACTOR = 1,
    AVAILABILITY ZONES = ('us-east-1a', 'us-east-1c')

# The pool lives on the cluster, not the replica. A non-NULL pool proves the
# managed replica was provisioned with a non-empty AZ list, so the NULL below is
# the managed-ness branch at work, not an empty list.
> SELECT availability_zones IS NOT NULL FROM mz_clusters WHERE name = 'c_managed_az'
true

# A managed cluster's replica is provisioned from the pool, but the public
# replica column stays NULL.
> SELECT r.name, r.availability_zone IS NULL
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON r.cluster_id = c.id
  WHERE c.name = 'c_managed_az'
r1 true
