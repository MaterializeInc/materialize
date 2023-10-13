# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
DROP CLUSTER default CASCADE

> SELECT 0
0

> SELECT 1 as id
1

> SELECT 1 from (select mz_now(), now())
1

# filtering isn't supported as a simple query today
! SELECT * FROM (VALUES (1), (2)) where column1 > 1
contains:unknown cluster 'default'

! SELECT generate_series(0, 10)
contains:unknown cluster 'default'

# to make it easier to run the test again, let's recreate the default cluster
$ postgres-execute connection=mz_system
CREATE CLUSTER default REPLICAS ()
