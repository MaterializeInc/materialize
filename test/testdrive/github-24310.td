# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CLUSTER sources (SIZE '1', REPLICATION FACTOR = 0);

> CREATE SOURCE counter IN CLUSTER sources FROM LOAD GENERATOR COUNTER (UP TO 100);

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
CREATE CLUSTER REPLICA sources.unbilled SIZE '2', BILLED AS 'free', INTERNAL;

! ALTER CLUSTER sources SET (REPLICATION FACTOR = 1);
contains:cannot create more than one replica of a cluster containing sources or sinks
detail:Currently have 1 replica (1 internal); command would result in 2

# Work has been scheduled
> SELECT max(counter) > 3 FROM counter;
true

$ postgres-execute connection=mz_system
DROP CLUSTER REPLICA sources.unbilled;

> ALTER CLUSTER sources SET (REPLICATION FACTOR = 1);

# Reset
> ALTER CLUSTER sources SET (REPLICATION FACTOR = 0);

$ postgres-execute connection=mz_system
CREATE CLUSTER REPLICA sources.unbilled SIZE '2', BILLED AS 'free', INTERNAL;

! CREATE CLUSTER REPLICA sources.r1 SIZE '1';
contains:cannot create more than one replica of a cluster containing sources or sinks
detail:Currently have 1 replica (1 internal); command would result in 2

$ postgres-execute connection=mz_system
DROP CLUSTER REPLICA sources.unbilled;

! CREATE CLUSTER REPLICA sources.r1 SIZE '1'
contains:cannot modify managed cluster sources
