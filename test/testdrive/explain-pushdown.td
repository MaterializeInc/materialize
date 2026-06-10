# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=(\d+) replacement=<number>

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_explain_pushdown = true

> CREATE CLUSTER sources (SIZE 'scale=1,workers=1', REPLICATION FACTOR 0);

> CREATE SOURCE counter IN CLUSTER sources FROM LOAD GENERATOR COUNTER (UP TO 100);
> CREATE TABLE counter_tbl FROM SOURCE counter;

# We expect the explain to time out, so no point waiting for very long...
$ set-sql-timeout duration=5s force=true

! EXPLAIN FILTER PUSHDOWN FOR SELECT count(*) FROM counter_tbl WHERE counter = 1;
timeout

$ set-sql-timeout duration=default force=true

# Check that we can still handle normal queries
> SELECT 1 + 1;
<number>

# If we scale the source cluster up, our query should eventually complete
> ALTER CLUSTER sources SET (REPLICATION FACTOR 1);

> EXPLAIN FILTER PUSHDOWN FOR SELECT count(*) FROM counter_tbl WHERE counter = 1;
materialize.public.counter_tbl <number> <number> <number> <number>
