# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET allowed_cluster_replica_sizes = 'scale=1,workers=1,nodisk';
ALTER SYSTEM SET disk_cluster_replicas_default = false;

# Cannot create clusters with cc cluster size naming schemes
! CREATE CLUSTER c SIZE '1cc';
contains:unknown cluster replica size 1cc

! CREATE CLUSTER c SIZE '512C';
contains:unknown cluster replica size 512C

# Nor can we create an unmanaged replica directly
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

! CREATE CLUSTER c REPLICAS (r1 (SIZE '1cc'))
contains:unknown cluster replica size 1cc

# The existing cluster names are fine
> CREATE CLUSTER c SIZE 'scale=1,workers=1,nodisk';

# But ensure we cannot ALTER our way to a cc name either
! ALTER CLUSTER c SET (SIZE '1cc');
contains:unknown cluster replica size 1cc

> DROP CLUSTER c

# Now flip the flag and test that we can create clusters
# with this naming scheme.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET allowed_cluster_replica_sizes;

> CREATE CLUSTER c SIZE '1cc';
> SELECT disk FROM mz_clusters WHERE name = 'c'
true
> DROP CLUSTER c

> CREATE CLUSTER c SIZE '1C';
> SELECT disk FROM mz_clusters WHERE name = 'c'
true
> DROP CLUSTER c

# Create a cluster with a legacy size with disk enabled.
> CREATE CLUSTER c SIZE 'scale=1,workers=1,nodisk', DISK = true
> SELECT disk FROM mz_clusters WHERE name = 'c'
true

# Altering to a cc size with disk explicitly toggled is not allowed.
! ALTER CLUSTER c SET (SIZE = '1cc', DISK = true)
contains: DISK option not supported for modern cluster sizes because disk is always enabled
! ALTER CLUSTER c SET (SIZE = '1cc', DISK = false)
contains: DISK option not supported for modern cluster sizes because disk is always enabled

# But it's fine as long as the ALTER command doesn't mention disk explicitly,
# even though the cluster's initial creation specified disk explicitly. The
# DISK value is just forced to true.
> ALTER CLUSTER c SET (SIZE = '1cc')
> SELECT disk FROM mz_clusters WHERE name = 'c'
true
> DROP CLUSTER c

# Same test as before, except the legacy size cluster has disk explicitly
# disabled.
> CREATE CLUSTER c SIZE 'scale=1,workers=1,nodisk', DISK = false
> ALTER CLUSTER c SET (SIZE = '1cc')
> SELECT disk FROM mz_clusters WHERE name = 'c'
true
> DROP CLUSTER c

# Same test as before, except the legacy size cluster has no disk explicitly
# configured.
> CREATE CLUSTER c SIZE = 'scale=1,workers=1,nodisk'
> SELECT disk FROM mz_clusters WHERE name = 'c'
false
> ALTER CLUSTER c SET (SIZE = '1cc')
> SELECT disk FROM mz_clusters WHERE name = 'c'
true

# Cannot explicitly alter DISK option for new sizes.
! ALTER CLUSTER c SET (DISK = false)
contains: DISK option not supported for modern cluster sizes because disk is always enabled
! ALTER CLUSTER c SET (DISK = true)
contains: DISK option not supported for modern cluster sizes because disk is always enabled

# But it's okay if you're going back to a legacy size.
> ALTER CLUSTER c SET (DISK = true, SIZE = 'scale=1,workers=1,nodisk')
> SELECT disk FROM mz_clusters WHERE name = 'c'
true
> DROP CLUSTER c

# Ensure that altering from a legacy size to a legacy size does not enable disk.
> CREATE CLUSTER c SIZE = 'scale=1,workers=1,nodisk'
> SELECT disk FROM mz_clusters WHERE name = 'c'
false
> ALTER CLUSTER c SET (SIZE = 'scale=1,workers=2,nodisk')
> SELECT disk FROM mz_clusters WHERE name = 'c'
false
> DROP CLUSTER c

# Ensure that disk isn't configurable for the new sizes (as it's force enabled).

> CREATE CLUSTER c SIZE '1cc', DISK = true;

> DROP CLUSTER c;

! CREATE CLUSTER c SIZE '1cc', DISK = false;
contains: DISK option disabled is not supported for non-legacy cluster sizes because disk is always enabled

> CREATE CLUSTER c REPLICAS (r1 (SIZE '1cc'))
> CREATE CLUSTER REPLICA c.r2 SIZE '1cc';
> CREATE CLUSTER REPLICA c.r3 SIZE '1C';

! CREATE CLUSTER REPLICA c.r SIZE '1cc', DISK = true;
contains: DISK option not supported for non-legacy cluster sizes because disk is always enabled

! CREATE CLUSTER REPLICA c.r SIZE '1cc', DISK = false;
contains: DISK option not supported for non-legacy cluster sizes because disk is always enabled
