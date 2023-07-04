# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test monotonicity analyses which derive from ENVELOPE NONE sources.
# Note that these only test the implementation for monotonic sources,
# they do not test that the analysis doesn't have false positives on
# non-monotonic sources.

# TODO: Once we have support for more "LOAD GENERATOR"-based monotonic
# sources these tests can be migrated to `monotonic.slt`.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=non-dbz-data

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=1
{"a": 1, "b": 2}
{"a": 1, "b": 1048576}
{"a": 2, "b": 3}
{"a": 2, "b": 4}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE non_dbz_data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> CREATE MATERIALIZED VIEW monotonic_min AS SELECT a, min(b) FROM non_dbz_data group by a

> SELECT * FROM monotonic_min
a min
---
1 2
2 3

> CREATE MATERIALIZED VIEW monotonic_max AS SELECT a, max(b) FROM non_dbz_data group by a

> SELECT * FROM monotonic_max
a max
---
1 1048576
2 4

# Smoke test for aggregate fusion
> CREATE MATERIALIZED VIEW monotonic_fused AS SELECT a, min(b), max(b + 1)  FROM non_dbz_data group by a

> SELECT * FROM monotonic_fused
a min max
---
1 2 1048577
2 3 5

# Propagating monotonicity analysis through indexed views
# TODO: After https://github.com/MaterializeInc/materialize/pull/13238 is merged, modify these tests to dig into
# the plans with `jq` and just check for `MonotonicTopK` being present.

> CREATE VIEW i1 AS SELECT b FROM non_dbz_data
> CREATE DEFAULT INDEX ON i1

> CREATE VIEW v2 AS SELECT * FROM i1 ORDER BY b LIMIT 3

$ set-regex match=(.User.:\s\d+|\(u\d+\)) replacement=UID

? EXPLAIN PHYSICAL PLAN FOR VIEW v2
materialize.public.v2:
  TopK::MonotonicTopK order_by=[#0 asc nulls_last] limit=3
    ArrangeBy
      input_key=[#0]
      raw=true
      Get::PassArrangements materialize.public.i1
        raw=false
        arrangements[0]={ key=[#0], permutation=id, thinning=() }

Used Indexes:
  - materialize.public.i1_primary_idx

> CREATE SOURCE non_dbz_data_indexed
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> CREATE DEFAULT INDEX ON non_dbz_data_indexed

# Propagating monotonicity analysis from an indexed source

> CREATE VIEW v3 AS SELECT * FROM non_dbz_data_indexed ORDER BY b LIMIT 3

? EXPLAIN PHYSICAL PLAN FOR VIEW v3
materialize.public.v3:
  TopK::MonotonicTopK order_by=[#1 asc nulls_last] limit=3
    ArrangeBy
      input_key=[#0, #1]
      raw=true
      Get::PassArrangements materialize.public.non_dbz_data_indexed
        raw=false
        arrangements[0]={ key=[#0, #1], permutation=id, thinning=() }

Used Indexes:
  - materialize.public.non_dbz_data_indexed_primary_idx

> CREATE VIEW i4 AS SELECT b+1 as c FROM i1;
> CREATE DEFAULT INDEX ON i4;

# Propagating monotonicity analysis through 2 indexed views (i1 and i4)

> CREATE VIEW v5 AS SELECT * from i4 ORDER BY c LIMIT 2;

? EXPLAIN PHYSICAL PLAN FOR VIEW v5;
materialize.public.v5:
  TopK::MonotonicTopK order_by=[#0 asc nulls_last] limit=2
    ArrangeBy
      input_key=[#0]
      raw=true
      Get::PassArrangements materialize.public.i4
        raw=false
        arrangements[0]={ key=[#0], permutation=id, thinning=() }

Used Indexes:
  - materialize.public.i4_primary_idx

> CREATE VIEW i6 AS SELECT c FROM v5;
> CREATE DEFAULT INDEX ON i6;

# Non-monotonic indexed view -- v7 can't use a monotonic TopK plan, because i6 and v5 are not monotonic

> CREATE VIEW v7 AS SELECT * from i6 ORDER BY c LIMIT 2;

? EXPLAIN PHYSICAL PLAN FOR VIEW v7;
materialize.public.v7:
  TopK::Basic order_by=[#0 asc nulls_last] limit=2
    ArrangeBy
      input_key=[#0]
      raw=true
      Get::PassArrangements materialize.public.i6
        raw=false
        arrangements[0]={ key=[#0], permutation=id, thinning=() }

Used Indexes:
  - materialize.public.i6_primary_idx

> CREATE VIEW i8 AS SELECT * from (SELECT * FROM i1 UNION ALL SELECT * FROM i1);
> CREATE DEFAULT INDEX ON i8;

> CREATE VIEW i9 AS SELECT * from (SELECT * FROM i1 UNION ALL SELECT * FROM i8);
> CREATE DEFAULT INDEX ON i9;

> CREATE VIEW v10 as SELECT b FROM i9 ORDER BY b LIMIT 2;;

# Propagating monotonicity analysis in a complex situation: (i1, i1) -> i8; (i1, i8) -> i9

? EXPLAIN PHYSICAL PLAN FOR VIEW v10;
materialize.public.v10:
  TopK::MonotonicTopK order_by=[#0 asc nulls_last] limit=2
    ArrangeBy
      input_key=[#0]
      raw=true
      Get::PassArrangements materialize.public.i9
        raw=false
        arrangements[0]={ key=[#0], permutation=id, thinning=() }

Used Indexes:
  - materialize.public.i9_primary_idx

> CREATE MATERIALIZED VIEW m1 AS SELECT b FROM non_dbz_data;

> CREATE VIEW v11 AS SELECT * FROM m1 ORDER BY b LIMIT 3;

# Propagating monotonicity analysis through materialized views

? EXPLAIN PHYSICAL PLAN FOR VIEW v11;
materialize.public.v11:
  TopK::MonotonicTopK order_by=[#0 asc nulls_last] limit=3
    Get::PassArrangements materialize.public.m1
      raw=true
