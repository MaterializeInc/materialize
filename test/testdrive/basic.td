# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT 1
1

> VALUES (1)
1

> CREATE VIEW select_constant as SELECT true

> SELECT * from select_constant;
true

> DROP VIEW select_constant;

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": null}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

! CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE 'data-schema.json'
  ENVELOPE DEBEZIUM
No such file or directory

$ file-write path=data-schema.json
\${schema}

> CREATE SOURCE data2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data
Source   Create Source
------------------
materialize.public.data  "CREATE SOURCE \"materialize\".\"public\".\"data\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA '{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] }   ] }' ENVELOPE DEBEZIUM"

! SELECT * FROM data
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> CREATE VIEW data_view as SELECT * from data

! SELECT * FROM data_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

#TODO (materialize#1725) This test consistently in fails on buildkite but not locally
#> CREATE SINK not_mat_sink FROM data_view INTO 'kafka://${testdrive.kafka-addr}/data-view-sink' WITH (schema_registry_url = '${testdrive.schema-registry-url}')

> CREATE MATERIALIZED VIEW test1 AS
  SELECT b, sum(a) FROM data GROUP BY b;

> SELECT * FROM test1
b  sum
------


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> SHOW FULL VIEWS
name        type     queryable   materialized
------------------------------------------------
data_view   USER     false       false
test1       USER     true        true

> SHOW MATERIALIZED VIEWS
test1

> SHOW SOURCES FROM mz_catalog
mz_arrangement_sharing
mz_arrangement_sizes
mz_catalog_names
mz_dataflow_channels
mz_dataflow_operator_addresses
mz_dataflow_operators
mz_materialization_dependencies
mz_materialization_frontiers
mz_materializations
mz_peek_active
mz_peek_durations
mz_scheduling_elapsed
mz_scheduling_histogram
mz_scheduling_parks
mz_view_foreign_keys
mz_view_keys

> SHOW VIEWS FROM mz_catalog
mz_addresses_with_unit_length
mz_dataflow_names
mz_dataflow_operator_dataflows
mz_perf_arrangement_records
mz_perf_dependency_frontiers
mz_perf_peek_durations_aggregates
mz_perf_peek_durations_bucket
mz_perf_peek_durations_core
mz_records_per_dataflow
mz_records_per_dataflow_global
mz_records_per_dataflow_operator

> SELECT * FROM test1;
b  sum
------
1  6
2  1

> SHOW COLUMNS FROM test1;
Field Nullable Type
-------------------
b     NO       int8
sum   YES      int8

> SHOW MATERIALIZED SOURCES FROM mz_catalog LIKE '%peek%';
mz_peek_active
mz_peek_durations

> SHOW VIEWS FROM mz_catalog LIKE '%peek%';
mz_perf_peek_durations_aggregates
mz_perf_peek_durations_bucket
mz_perf_peek_durations_core

> SHOW VIEWS LIKE '%data%';
data_view

> SHOW CREATE VIEW test1
View                      Create View
----------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.test1  "CREATE VIEW \"materialize\".\"public\".\"test1\" AS SELECT \"b\", sum(\"a\") FROM \"materialize\".\"public\".\"data\" GROUP BY \"b\""

# materialized view can be built on a not-materialized view
> CREATE MATERIALIZED VIEW test2 AS
  SELECT b, 1 + sum(a + 1) FROM data_view GROUP BY b;

> SELECT * FROM test2
b  ?column?
-----------
1  10
2  3

# materialize data_view
> CREATE INDEX data_view_idx on data_view(a)

> SELECT * FROM data_view
a b
---
1 1
2 1
3 1
1 2

> CREATE VIEW test3 AS
  SELECT b, min(a) FROM data_view GROUP BY b;

> SELECT * FROM test3
b  min
------
1  1
2  1

> CREATE MATERIALIZED VIEW test4 AS
  SELECT b, max(a) FROM data_view GROUP BY b;

> SELECT * FROM test4
b  max
------
1  3
2  1

#unmaterialize data view
> DROP INDEX data_view_idx;

#can continue to select from view that depends on the unmaterialized view
> SELECT * FROM test4
b  max
------
1  3
2  1

> SELECT * FROM test4 where b = 2
b  max
------
2  1

# cannot select from unmaterialized view
! SELECT * from data_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

# can create sink from unmaterialized view
> CREATE SINK not_mat_sink2 FROM data_view
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-view2-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# can create materialized view from unmaterialized view
> CREATE MATERIALIZED VIEW test5 AS
  SELECT b, max(a) AS c FROM data_view GROUP BY b;

> SELECT * FROM test5
b  c
------
1  3
2  1

> SELECT c+b from test5
c+b
------
4
3

> CREATE INDEX idx1 ON test5(c)

! SELECT * FROM idx1
catalog item 'materialize.public.idx1' is an index and so cannot be depended upon

! SELECT * from idx1
catalog item 'materialize.public.idx1' is an index and so cannot be depended upon

#if there exists a second primary index, dropping one primary index will not unmaterialize the view
> DROP INDEX test5_primary_idx;

> SELECT * from test5;
b  c
------
1  3
2  1

> SELECT c-b from test5;
c-b
------
2
-1

#unmaterialize test5
> DROP INDEX idx1;

! SELECT * from test5
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

# test that materialized views can be even if it requires multiple layers of recursing through the AST
# to find a source
> CREATE MATERIALIZED VIEW test6 AS SELECT (-c + 2*b) AS d FROM test5;

> SELECT * from test6;
d
----
-1
3

# dependencies have not re-materialized as a result of creating a dependent materialized view
! SELECT * from test5
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

! SELECT * from data_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

# rematerialize data_view creating an index on it
> CREATE INDEX data_view_idx on data_view(a)

> SELECT * from data_view
a b
---
1 1
2 1
3 1
1 2

#existing materialized dependencies can be selected from as normal
> SELECT * from test6;
d
----
-1
3

#dependencies can be selected from again if they do not depend on any other raw source
> SELECT * from test5
b  c
------
1  3
2  1

# Create a suboptimal second index on the same column in data_view
> CREATE INDEX data_view_idx2 on data_view(a)

> SELECT * from data_view
a b
---
1 1
2 1
3 1
1 2

> SELECT * from test6;
d
----
-1
3

> SELECT * from test5
b  c
------
1  3
2  1

#delete the first copy of the same index and ensure everything selects as normal
> DROP INDEX data_view_idx;

> SELECT * from data_view
a b
---
1 1
2 1
3 1
1 2

> SELECT * from test6;
d
----
-1
3

> SELECT * from test5
b  c
------
1  3
2  1

# Materialized sources tests

$ set mat={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=mat schema=${mat} timestamp=1
{"before": null, "after": null}

> CREATE MATERIALIZED SOURCE mat_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-mat-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${mat}'
  ENVELOPE DEBEZIUM

> SELECT * from mat_data;

$ kafka-ingest format=avro topic=mat schema=${schema} timestamp=42
{"before": null, "after": {"a": -1, "b": 0}}
{"before": null, "after": {"a": -1, "b": 1}}
{"before": null, "after": {"a": 3, "b": 4}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=mat schema=${schema} timestamp=43
{"before": null, "after": null}

> SELECT * from mat_data;
a  b
----
-1 0
-1 1
3  4
1  2

# if there exists another index, dropping the primary index will not unmaterialize the source

> CREATE INDEX mat_data_idx2 ON mat_data(a);

> DROP INDEX mat_data_primary_idx;

> SELECT a+b from mat_data;
a+b
----
-1
0
7
3

# Can create both materialized and unmaterialized views from materialized source
> CREATE MATERIALIZED VIEW test7 as SELECT count(*) from mat_data;

> SELECT * from test7;
count
-----
4

> CREATE VIEW test8 as SELECT -b as c, -a as d from mat_data;

> SELECT * from test8;
c  d
-----
0  1
-1 1
-4 -3
-2 -1

# unmaterialize source
> DROP INDEX mat_data_idx2;

! SELECT * from mat_data;
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> SELECT * from test7;
count
-----
4

! SELECT * from test8;
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

$ kafka-ingest format=avro topic=mat schema=${schema} timestamp=44
{"before": null, "after": {"a": -3, "b": 0}}
{"before": null, "after": {"a": -1, "b": 0}}
{"before": null, "after": {"a": 0, "b": 4}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=mat schema=${schema} timestamp=45
{"before": null, "after": null}

# rematerialize source
> CREATE INDEX mat_data_idx3 on mat_data(b);

> SELECT * from mat_data;
a  b
----
-1 0
-1 1
3  4
1  2
-3 0
-1 0
0  4
1  2

> SELECT * from test7;
count
-----
8

> SELECT * from test8;
a  b
----
0  1
-1 1
-4 -3
-2 -1
0  3
0  1
-4 0
-2 -1

# N.B. it is important to test sinks that depend on sources directly vs. sinks
# that depend on views, as the code paths are different.

# Depends on source.
> CREATE SINK mat_source_sink FROM mat_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'mat-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK source_sink FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# Depends on view.
> CREATE SINK view_sink FROM test5
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'test1-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> DROP SINK source_sink

> DROP SINK view_sink

> DROP SINK mat_source_sink