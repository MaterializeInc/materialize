# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests specialized arrangement functionality. Note that this test requires
# the retry behavior of testdrive as it accesses introspection sources.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_specialized_arrangements = true

> CREATE CLUSTER test SIZE '2', REPLICATION FACTOR 2;

> SET cluster = test;

> SET cluster_replica = r1;

# Test that single-column indexes with fixed-length 8-byte types use specialized arrangements.
> CREATE TABLE t_boolean (a boolean NOT NULL);

> INSERT INTO t_boolean VALUES (true);

> CREATE DEFAULT INDEX ON t_boolean;

> CREATE TABLE t_smallint (a smallint NOT NULL);

> INSERT INTO t_smallint VALUES (42);

> CREATE DEFAULT INDEX ON t_smallint;

> CREATE TABLE t_int (a int NOT NULL);

> INSERT INTO t_int VALUES (42);

> CREATE DEFAULT INDEX ON t_int;

> CREATE TABLE t_bigint (a bigint NOT NULL);

> INSERT INTO t_bigint VALUES (42);

> CREATE DEFAULT INDEX ON t_bigint;

> CREATE TABLE t_uint2 (a uint2 NOT NULL);

> INSERT INTO t_uint2 VALUES (42);

> CREATE DEFAULT INDEX ON t_uint2;

> CREATE TABLE t_uint4 (a uint4 NOT NULL);

> INSERT INTO t_uint4 VALUES (42);

> CREATE DEFAULT INDEX ON t_uint4;

> CREATE TABLE t_uint8 (a uint8 NOT NULL);

> INSERT INTO t_uint8 VALUES (42);

> CREATE DEFAULT INDEX ON t_uint8;

> CREATE TABLE t_real (a real NOT NULL);

> INSERT INTO t_real VALUES (42);

> CREATE DEFAULT INDEX ON t_real;

> CREATE TABLE t_double (a double NOT NULL);

> INSERT INTO t_double VALUES (42);

> CREATE DEFAULT INDEX ON t_double;

> CREATE TABLE t_date (a date NOT NULL);

> INSERT INTO t_date VALUES (DATE '1942-04-02');

> CREATE DEFAULT INDEX ON t_date;

> CREATE TABLE t_time (a time NOT NULL);

> INSERT INTO t_time VALUES (TIME '02:42:42');

> CREATE DEFAULT INDEX ON t_time;

> CREATE TABLE t_timestamp (a timestamp NOT NULL);

> INSERT INTO t_timestamp VALUES (TIMESTAMP '1942-04-02 04:02:42');

> CREATE DEFAULT INDEX ON t_timestamp;

> CREATE TABLE t_timestamptz (a timestamp with time zone NOT NULL);

> INSERT INTO t_timestamptz VALUES (TIMESTAMP '1942-04-02 04:02:42+04');

> CREATE DEFAULT INDEX ON t_timestamptz;

> CREATE TABLE t_interval (a interval NOT NULL);

> INSERT INTO t_interval VALUES (INTERVAL '42' MINUTES);

> CREATE DEFAULT INDEX ON t_interval;

> CREATE TABLE t_uuid (a uuid NOT NULL);

> INSERT INTO t_uuid VALUES (UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');

> CREATE DEFAULT INDEX ON t_uuid;

> CREATE TABLE t_oid (a oid NOT NULL);

> INSERT INTO t_oid VALUES (42);

> CREATE DEFAULT INDEX ON t_oid;

> CREATE TABLE t_mz_timestamp (a mz_timestamp NOT NULL);

> INSERT INTO t_mz_timestamp VALUES (1696841933694);

> CREATE DEFAULT INDEX ON t_mz_timestamp;

> CREATE TABLE t_mz_aclitem_nullable (a mz_aclitem);

# Unfortunately, we cannot mint an mz_aclitem from thin air.
> INSERT INTO t_mz_aclitem_nullable VALUES (NULL);

> CREATE DEFAULT INDEX ON t_mz_aclitem_nullable;

> CREATE TABLE t_aclitem_nullable (a aclitem);

# Unfortunately, we cannot mint an aclitem from thin air.
> INSERT INTO t_aclitem_nullable VALUES (NULL);

> CREATE DEFAULT INDEX ON t_aclitem_nullable;

# Out of the types above, uuid, interval, timestamp, timestamptz, mz_aclitem, and aclitem
# are all too large to fit a 9-byte fixed-length representation. So, we get 13 instead of
# 19 specialized arrangements in total.
> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[9-byte]%';
13

# Test query patterns. Note that we focus on bigint as a representative, but the same patterns
# apply to other types as well.
> CREATE TABLE t_bigint_nullable(a bigint);

> INSERT INTO t_bigint_nullable VALUES (42), (NULL);

> INSERT INTO t_bigint VALUES (43);

# Illustrates a fast-path peek.
? EXPLAIN PHYSICAL PLAN FOR
  SELECT * FROM t_bigint WHERE a = 42;
Explained Query (fast path):
  Project (#0)
    ReadIndex on=materialize.public.t_bigint t_bigint_primary_idx=[lookup value=(42)]

Used Indexes:
  - materialize.public.t_bigint_primary_idx (lookup)

> SELECT * FROM t_bigint WHERE a = 42;
42

# Similar to the above, but with a full scan.
> SELECT * FROM t_bigint WHERE a <> 43;
42

# Illustrates joins among non-nullable vs. nullable columns of the same type.
? EXPLAIN PHYSICAL PLAN FOR
  CREATE MATERIALIZED VIEW null_nonnull_join AS
  SELECT * FROM t_bigint_nullable JOIN t_bigint USING (a);
materialize.public.null_nonnull_join:
  Join::Linear
    linear_stage[0]
      lookup={ relation=0, key=[#0] }
      stream={ key=[#0], thinning=() }
    source={ relation=1, key=[#0] }
    ArrangeBy
      raw=true
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[bigint]
      Get::Collection materialize.public.t_bigint_nullable
        raw=true
    Get::PassArrangements materialize.public.t_bigint
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[bigint]

Source materialize.public.t_bigint_nullable
  filter=((#0) IS NOT NULL)

Used Indexes:
  - materialize.public.t_bigint_primary_idx (differential join)

> CREATE MATERIALIZED VIEW null_nonnull_join AS
  SELECT * FROM t_bigint_nullable JOIN t_bigint USING (a);

> SELECT * FROM null_nonnull_join;
42

> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[9-byte]%';
14

# Illustrates joins across types.
? EXPLAIN PHYSICAL PLAN FOR
  CREATE MATERIALIZED VIEW cross_type_join AS
  SELECT * FROM t_bigint_nullable JOIN t_real USING (a);
materialize.public.cross_type_join:
  Join::Linear
    linear_stage[0]
      closure
        project=(#1)
      lookup={ relation=0, key=[bigint_to_real(#0)] }
      stream={ key=[#0], thinning=() }
    source={ relation=1, key=[#0] }
    ArrangeBy
      raw=true
      arrangements[0]={ key=[bigint_to_real(#0)], permutation={#0: #1}, thinning=(#0) }
      types=[bigint]
      Get::Collection materialize.public.t_bigint_nullable
        raw=true
    Get::PassArrangements materialize.public.t_real
      raw=false
      arrangements[0]={ key=[#0], permutation=id, thinning=() }
      types=[real]

Source materialize.public.t_bigint_nullable
  filter=((#0) IS NOT NULL)

Used Indexes:
  - materialize.public.t_real_primary_idx (differential join)

> CREATE MATERIALIZED VIEW cross_type_join AS
  SELECT * FROM t_bigint_nullable JOIN t_real USING (a);

> SELECT * FROM cross_type_join;
42

> SELECT COUNT(*)
  FROM mz_internal.mz_dataflow_operator_dataflows
  WHERE name LIKE '%ArrangeBy%[9-byte]%';
15

> RESET cluster;

> RESET cluster_replica;

> DROP CLUSTER test CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_specialized_arrangements = false
