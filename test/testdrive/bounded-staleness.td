# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Bounded staleness isolation — happy-path coverage.

> CREATE TABLE bs_t (a int);
> INSERT INTO bs_t VALUES (1), (2), (3);

# Read once under default (strict serializable) isolation so the read frontier
# advances past the insert before we switch to bounded staleness. Under bounded
# staleness, the SELECT does not block on input frontiers, so we must observe
# the write here first.
> SELECT count(*) FROM bs_t
3

> SET transaction_isolation = 'bounded staleness 5s'

> SELECT count(*) FROM bs_t
3

> SELECT sum(a) FROM bs_t
6

# Round-trip via SHOW.
> SHOW transaction_isolation
"bounded staleness 5s"

> RESET transaction_isolation

> DROP TABLE bs_t

# --- error path: bounded staleness exceeded -----------------------------------
#
# Build an MV on a dedicated cluster, then drop the cluster's replication factor
# to 0 so the MV's upper frontier stops advancing. After waiting past the
# staleness bound, a SELECT under bounded staleness must error.

> CREATE CLUSTER bs_cluster SIZE 'scale=1,workers=1'

> CREATE TABLE bs_t (a int)

> INSERT INTO bs_t VALUES (1), (2), (3)

> CREATE MATERIALIZED VIEW bs_mv IN CLUSTER bs_cluster AS
  SELECT count(*) FROM bs_t

# Read under strict serializable so the frontier advances past the data.
> SET transaction_isolation = 'strict serializable'
> SELECT * FROM bs_mv
3
> RESET transaction_isolation

> ALTER CLUSTER bs_cluster SET (REPLICATION FACTOR 0)

> SELECT mz_unsafe.mz_sleep(2)
<null>

> SET transaction_isolation = 'bounded staleness 500ms'

! SELECT * FROM bs_mv
contains: cannot serve query under bounded staleness

> RESET transaction_isolation

> DROP MATERIALIZED VIEW bs_mv

> DROP TABLE bs_t

> DROP CLUSTER bs_cluster
