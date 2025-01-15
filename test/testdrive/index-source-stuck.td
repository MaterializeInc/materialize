# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP CLUSTER IF EXISTS cluster1 CASCADE;
> DROP CLUSTER IF EXISTS cluster2 CASCADE;
> CREATE CLUSTER cluster1 REPLICAS (replica1 (SIZE '1'));
> CREATE CLUSTER cluster2 REPLICAS (replica1 (SIZE '1'));
> CREATE SOURCE src IN CLUSTER cluster1 FROM LOAD GENERATOR counter (UP TO 100);
> CREATE MATERIALIZED VIEW mv IN CLUSTER cluster2 AS SELECT * FROM src;
> SET cluster = cluster2;

# Prevent us from getting stuck, see database-issues#8300
> SELECT min(counter) FROM src
1
> SELECT min(counter) FROM mv
1

# Dropping the replica will make the load generator be out of date from now on
> DROP CLUSTER REPLICA cluster1.replica1;

# Wait a bit to let Mz realize that
> SELECT mz_unsafe.mz_sleep(3)
<null>

# Strict serializable is expected to fail, but serializable isolation should still be able to return (out of date) results
> SET transaction_isolation = serializable;

# Should return instantly since we have selected serializable isolation
> SELECT min(counter) FROM src;
1

# Should return instantly, even inside of a transaction
# > BEGIN
# > SELECT * FROM src;
# > COMMIT

# Should return instantly since it only uses the materialized view and we have selected serializable isolation
> SELECT min(counter) FROM mv;
1

# Should return instantly, even inside of a transaction
# TODO(def-): Enable once incident 78 is fixed:
# > BEGIN
# > SELECT * FROM mv;
# > COMMIT

> CREATE INDEX IN CLUSTER cluster2 ON mv (counter);

# Check that index is actually used
? EXPLAIN SELECT min(counter) FROM mv;
Explained Query:
  With
    cte l0 =
      Reduce aggregates=[min(#0)]
        ReadIndex on=mv mv_counter_idx=[*** full scan ***]
  Return
    Union
      Get l0
      Map (null)
        Union
          Negate
            Project ()
              Get l0
          Constant
            - ()

Used Indexes:
  - materialize.public.mv_counter_idx (*** full scan ***)

Target cluster: cluster2

> SELECT 1
1
# Should return instantly since it only uses the index and we have selected serializable isolation
> SELECT min(counter) FROM mv;
1

# TODO(def-): Enable once incident 78 is fixed:
# > BEGIN
# > SELECT * FROM mv;
# > COMMIT

> DROP CLUSTER IF EXISTS cluster1 CASCADE;
> DROP CLUSTER IF EXISTS cluster2 CASCADE;
