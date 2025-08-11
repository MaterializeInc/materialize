# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test confirms that dataflow operators are cleaned up when they are not
# needed anymore.
#
# This test relies on testdrive's automatic retries, since it queries
# introspection sources that take a while to update.

# Introspection subscribes add noise to the introspection sources, so disable them.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_introspection_subscribes = false

# Create a clean replica to inspect dataflow state on.

> DROP CLUSTER IF EXISTS test

> CREATE CLUSTER test REPLICAS (r1 (SIZE 'scale=1,workers=1', INTROSPECTION INTERVAL '10ms'))
> SET cluster = test
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false

# Verify that an index dataflow is cleaned up when the index is dropped.

> CREATE TABLE t (a int)
> CREATE INDEX test_index ON t (a)
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
true
> DROP INDEX test_index
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false

# Verify that an index dataflow is cleaned up when its input advances to the
# empty frontier.
# To make sure that we don't query mz_dataflow_operators before the dataflow
# was created, we query the index once and then wait for a bit as well.

> CREATE VIEW constant_view AS SELECT generate_series(1, 1000) AS a
> CREATE INDEX test_index ON constant_view (a)
> SELECT count(*) FROM constant_view
1000
> SELECT mz_unsafe.mz_sleep(1)
<null>
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false
> DROP INDEX test_index
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false

# Verify that an MV dataflow is cleaned up when the MV is dropped.

> CREATE MATERIALIZED VIEW test_mv AS SELECT a FROM t
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
true
> DROP MATERIALIZED VIEW test_mv
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false

# Verify that an MV dataflow is cleaned up when its input advances to the
# empty frontier.
# To make sure that we don't query mz_dataflow_operators before the dataflow
# was created, we query the MV once and then wait for a bit as well.

> CREATE MATERIALIZED VIEW test_mv AS SELECT generate_series(1, 1000)
> SELECT count(*) FROM test_mv
1000
> SELECT mz_unsafe.mz_sleep(1)
<null>
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false
> DROP MATERIALIZED VIEW test_mv
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflow_operators
false

# Regression test for https://github.com/MaterializeInc/database-issues/issues/4712

> CREATE CLUSTER lgtpch_cluster_cleanup SIZE 'scale=1,workers=1';

> CREATE SOURCE lgtpch IN CLUSTER lgtpch_cluster_cleanup FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.1, UP TO 100);

> CREATE TABLE customer FROM SOURCE lgtpch (REFERENCE customer);
> CREATE TABLE lineitem FROM SOURCE lgtpch (REFERENCE lineitem);
> CREATE TABLE nation FROM SOURCE lgtpch (REFERENCE nation);
> CREATE TABLE orders FROM SOURCE lgtpch (REFERENCE orders);
> CREATE TABLE part FROM SOURCE lgtpch (REFERENCE part);
> CREATE TABLE partsupp FROM SOURCE lgtpch (REFERENCE partsupp);
> CREATE TABLE region FROM SOURCE lgtpch (REFERENCE region);
> CREATE TABLE supplier FROM SOURCE lgtpch (REFERENCE supplier);

> CREATE MATERIALIZED VIEW q14 AS
  SELECT
    100.00 * sum(case
      when p_type like 'PROMO%'
        then l_extendedprice * (1 - l_discount)
      else 0
    end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
  FROM
    lineitem,
    part
  WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' month
> SELECT count(*) > 0 FROM q14
true
> SELECT mz_unsafe.mz_sleep(1)
<null>
> SELECT count(*) FROM mz_introspection.mz_dataflow_operators
0
> DROP MATERIALIZED VIEW q14
> SELECT count(*)  FROM mz_introspection.mz_dataflow_operators
0
