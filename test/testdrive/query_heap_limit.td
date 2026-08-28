# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the `query_heap_limit` session variable.
#
# The limit is compared against the dataflow's heap usage summed over the whole replica, so this
# runs on a cluster with an explicit multi-worker size rather than the composition's default. A
# per-worker or worker-count-scaled comparison passes at one worker and fails here.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_compute_heap_size_limit = true

> CREATE CLUSTER heap_limit SIZE 'scale=1,workers=2'

> SET cluster = heap_limit

> CREATE TABLE t1 (a int4, b text)

> INSERT INTO t1 SELECT * FROM generate_series(1, 10000), repeat('a', 100)

# A limit far below any dataflow's footprint must trip.

> SET query_heap_limit = '1kB'

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1)

# The first batch is produced from the initial timestamp, before the watchdog has observed the
# dataflow's size, so it arrives normally.
> FETCH 1 c
<TIMESTAMP> 1 10000

! FETCH 1 c
contains:Dataflow limit exceeded

> ROLLBACK

# The same limit on the `SELECT` path, which the controller resolves by cancelling the peek rather
# than by failing a subscribe.

! SELECT count(*) FROM t1, t1 AS t2 WHERE t1.a = t2.a
contains:Dataflow limit exceeded

> ROLLBACK

# A limit far above it must not.

> SET query_heap_limit = '512MB'

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1)

> FETCH 1 c
<TIMESTAMP> 1 10000

> FETCH 1 FROM c WITH (TIMEOUT = '1s')

> ROLLBACK

# Both spellings of "no limit" must clear it.

> SET query_heap_limit = '1kB'

> RESET query_heap_limit

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1)

> FETCH 1 c
<TIMESTAMP> 1 10000

> FETCH 1 FROM c WITH (TIMEOUT = '1s')

> ROLLBACK

> SET query_heap_limit = 0

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(a) FROM t1)

> FETCH 1 c
<TIMESTAMP> 1 10000

> FETCH 1 FROM c WITH (TIMEOUT = '1s')

> ROLLBACK

> RESET query_heap_limit

> SET cluster = quickstart

> DROP CLUSTER heap_limit CASCADE

> DROP TABLE t1
