# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the `max_query_heap_size` session variable.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

# The watchdog that enforces the limit is rendered when a replica initializes its logging dataflow,
# so the flag has to be set before the clusters below are created.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_compute_heap_size_limit = true

# Two workers, so that the per-dataflow subtotals are exchanged and summed across workers rather
# than read off a single one.
#
# The introspection interval bounds how quickly the watchdog can observe a dataflow, and the two
# clusters want opposite things from it. A subscribe delivers its snapshot from the `as_of` the
# coordinator picked before the dataflow existed, so a long interval makes "snapshot first, error
# second" deterministic. A peek's dataflow lives only as long as the peek, so a short interval is
# what gives the watchdog a chance to see it at all.

> CREATE CLUSTER heap_limit_subscribe (SIZE 'scale=1,workers=2', INTROSPECTION INTERVAL '5s')

> CREATE CLUSTER heap_limit_peek (SIZE 'scale=1,workers=2', INTROSPECTION INTERVAL '10ms')

> CREATE TABLE t (a int4, b text)

> INSERT INTO t SELECT generate_series, repeat('a', 100) FROM generate_series(1, 50000)

> SHOW max_query_heap_size
""

> SET cluster = heap_limit_subscribe

# Without a limit the subscribe keeps running.

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a))

> FETCH 1 c
<TIMESTAMP> 1 50000

> FETCH 1 c WITH (TIMEOUT = '2s')

> ROLLBACK

# A limit the dataflow stays well below does not disturb it either.

> SET max_query_heap_size = '512MB'

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a))

> FETCH 1 c
<TIMESTAMP> 1 50000

> FETCH 1 c WITH (TIMEOUT = '2s')

> ROLLBACK

# A limit the dataflow cannot meet fails the subscribe. The grouped subquery arranges a row per
# key, which puts it well above the limit set here.

> SET max_query_heap_size = '1kB'

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a))

> FETCH 1 c
<TIMESTAMP> 1 50000

# The timeout bounds a regression: without it a fetch that never errors blocks the whole run rather
# than failing it.
! FETCH 1 c WITH (TIMEOUT = '60s')
contains:query exceeded the max_query_heap_size limit

> ROLLBACK

# Zero reads as no limit, so the same subscribe runs again.

> SET max_query_heap_size = 0

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a))

> FETCH 1 c
<TIMESTAMP> 1 50000

> FETCH 1 c WITH (TIMEOUT = '2s')

> ROLLBACK

> RESET max_query_heap_size

> SHOW max_query_heap_size
""

# The same limit applies to a slow-path peek, which renders a dataflow of its own and fails through
# the peek response path rather than the subscribe one.
#
# NOTE: unlike the subscribe above, this is not ordering-proof. Whichever of the watchdog's report
# and the replica's own peek response reaches the controller first wins, because the first one
# retires the peek. The 10ms introspection interval on this cluster buys a wide margin over a
# reduce across 50k rows, but if it ever proves too narrow, the lever is a larger or deliberately
# slower input rather than a shorter interval.

> SET cluster = heap_limit_peek

> SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a)
50000

> SET max_query_heap_size = '1kB'

! SELECT count(*) FROM (SELECT a, max(b) FROM t GROUP BY a)
contains:query exceeded the max_query_heap_size limit

# A fast-path peek reads an existing arrangement rather than rendering a dataflow, so it carries no
# limit and is unaffected.

> CREATE INDEX t_a ON t (a)

> SELECT a FROM t WHERE a = 1
1

> RESET max_query_heap_size

> RESET cluster

> DROP TABLE t CASCADE

> DROP CLUSTER heap_limit_subscribe

> DROP CLUSTER heap_limit_peek

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enable_compute_heap_size_limit
