# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Re-assert mz_cluster_replicas.availability_zone after an environmentd restart.
# The replica locations are recomputed from durable state on boot, so this
# exercises the rebuild-from-durable concretization path too. See
# replica-availability-zone.td for the objects under test.

> SELECT r.name, r.availability_zone
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON r.cluster_id = c.id
  WHERE c.name = 'c_unmanaged_az'
r1 us-east-1b

> SELECT r.name, r.availability_zone
  FROM mz_cluster_replicas r
  JOIN mz_clusters c ON r.cluster_id = c.id
  WHERE c.name = 'c_managed_az'
r1 us-east-1a,us-east-1c
