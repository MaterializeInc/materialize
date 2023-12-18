# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_cc_cluster_sizes = false

# Clean up cluster manually, since testdrive does not automatically clean up
# clusters.
> DROP CLUSTER IF EXISTS c;

# Cannot create clusters with cc cluster size naming schemes
! CREATE CLUSTER c SIZE '1cc';
contains:use of 'cc' cluster sizes is not supported

! CREATE CLUSTER c SIZE '512C';
contains:use of 'cc' cluster sizes is not supported

# Nor can we create an unmanaged replica directly
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true

! CREATE CLUSTER c REPLICAS (r1 (SIZE '1cc'))
contains:use of 'cc' cluster sizes is not supported

# The existing cluster names are fine
> CREATE CLUSTER c SIZE '1';

# But ensure we cannot ALTER our way to a cc name either
! ALTER CLUSTER c SET (SIZE '1cc');
contains:use of 'cc' cluster sizes is not supported

> DROP CLUSTER IF EXISTS c;

# Now flip the flag and test that we can create clusters
# with this naming scheme. Note that here the commands
# still fail (haven't hooked up any of these size names
# for testing), but crucially the error isn't due to the
# feature flag gating
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_cc_cluster_sizes = true

! CREATE CLUSTER c SIZE '1cc';
contains:unknown cluster replica size 1cc

! CREATE CLUSTER c SIZE '1C';
contains:unknown cluster replica size 1C
