# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that all materialized views dealing with the same source
# see the same records at the same timestamp.
#
# As per Ruchir's idea, this is being tested by running queries like
# SELECT some_view EXCEPT ALL select_some_other_view
# and making sure they return no rows
#
# Unfortunately this failed to reproduce the original problem, so the
# test is being pushed here in the hope that it will somehow end up
# being useful in the future.
#

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
       {"name": "a", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data partitions=16

$ kafka-ingest format=avro topic=data partition=0 schema=${schema}
{"a": 10}
{"a": 5}

$ kafka-ingest format=avro topic=data partition=1 schema=${schema}
{"a": 1}
{"a": 7}
{"a": 6}

$ kafka-ingest format=avro topic=data partition=2 schema=${schema}
{"a": 10}
{"a": 1}
{"a": 7}
{"a": 8}

$ kafka-ingest format=avro topic=data partition=3 schema=${schema}
{"a": 6}

> CREATE SOURCE direct_source1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  LEGACYWITH (timestamp_frequency_ms = 90)
  FORMAT AVRO USING SCHEMA '${schema}';

> CREATE SOURCE direct_source2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  LEGACYWITH (timestamp_frequency_ms = 100)
  FORMAT AVRO USING SCHEMA '${schema}';

$ kafka-ingest format=avro topic=data partition=4 schema=${schema}
{"a": 5}

$ kafka-ingest format=avro topic=data partition=5 schema=${schema}
{"a": 1}
{"a": 10}
{"a": 2}
{"a": 5}

$ kafka-ingest format=avro topic=data partition=6 schema=${schema}
{"a": 9}
{"a": 4}

$ kafka-ingest format=avro topic=data partition=7 schema=${schema}
{"a": 5}
{"a": 8}
{"a": 1}

> CREATE MATERIALIZED VIEW direct_view1a AS SELECT a + 0 AS a FROM direct_source1;

# Sleep so that the views are not all created at the same time

> SELECT mz_internal.mz_sleep(2);
<null>

> CREATE MATERIALIZED VIEW direct_view1b AS SELECT a + 0 AS a FROM direct_source1;

> CREATE MATERIALIZED VIEW derived_view1a AS SELECT a + 0 AS a FROM direct_view1a;

> SELECT mz_internal.mz_sleep(2);
<null>

> CREATE MATERIALIZED VIEW derived_view1b AS SELECT a + 0 AS a FROM direct_view1b;

> CREATE MATERIALIZED VIEW join_view1 AS SELECT a1.a + 0 AS a FROM direct_source1 AS a1, direct_view1a;

> CREATE MATERIALIZED VIEW join_view2 AS SELECT a1.a + 0 AS a FROM direct_view1a AS a1, direct_view1b;

> CREATE MATERIALIZED VIEW join_view3 AS SELECT a2.a + 0 AS a FROM direct_view1a, derived_view1b AS a2;

$ kafka-ingest format=avro topic=data partition=8 schema=${schema}
{"a": 2}
{"a": 8}
{"a": 5}
{"a": 9}

$ kafka-ingest format=avro topic=data partition=9 schema=${schema}
{"a": 6}

$ kafka-ingest format=avro topic=data partition=10 schema=${schema}
{"a": 4}

$ kafka-ingest format=avro topic=data partition=11 schema=${schema}
{"a": 7}
{"a": 2}
{"a": 10}
{"a": 1}

> CREATE MATERIALIZED VIEW check_v1 AS SELECT a + 0 AS a FROM direct_source1 EXCEPT ALL SELECT a - 0 AS a FROM direct_source1;

> CREATE MATERIALIZED VIEW check_v2 AS SELECT a + 0 AS a FROM direct_view1a EXCEPT ALL SELECT a - 0 AS a FROM direct_view1a;

> CREATE MATERIALIZED VIEW check_v3 AS SELECT a + 0 AS a FROM direct_view1a EXCEPT ALL SELECT a - 0 AS a FROM direct_view1b;

> CREATE MATERIALIZED VIEW check_v4 AS SELECT a + 0 AS a FROM derived_view1a EXCEPT ALL SELECT a - 0 AS a FROM derived_view1a;

> CREATE MATERIALIZED VIEW check_v5 AS SELECT a + 0 AS a FROM derived_view1a EXCEPT ALL SELECT a - 0 AS a FROM derived_view1b;

> CREATE MATERIALIZED VIEW check_v6 AS SELECT a + 0 AS a FROM join_view1 EXCEPT ALL SELECT a - 0 AS a FROM join_view2;

> CREATE MATERIALIZED VIEW check_v7 AS SELECT a + 0 AS a FROM join_view2 EXCEPT ALL SELECT a - 0 AS a FROM join_view3;

$ kafka-ingest format=avro topic=data partition=12 schema=${schema}
{"a": 31}
{"a": 32}

$ kafka-ingest format=avro topic=data partition=13 schema=${schema}
{"a": 33}
{"a": 34}
{"a": 35}

$ kafka-ingest format=avro topic=data partition=14 schema=${schema}
{"a": 36}
{"a": 37}

$ kafka-ingest format=avro topic=data partition=15 schema=${schema}
{"a": 38}
{"a": 39}
{"a": 40}

# Make sure that none of the 'EXCEPT ALL' views above has ever produced any records.

> SELECT COUNT(*) FROM mz_records_per_dataflow_global WHERE name LIKE '%check_v%' AND records > 0;
0
