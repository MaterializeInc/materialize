# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> SELECT 1
1

> VALUES (1)
1

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

> CREATE SOURCE data FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

> SHOW CREATE SOURCE data
Source   Create Source
------------------
data  "kafka://${testdrive.kafka-addr-resolved}/testdrive-data-${testdrive.seed}"

! PEEK data
data is not a view

> CREATE VIEW data_view as SELECT * from data

> PEEK data_view
a b
---


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> PEEK data_view
a b
---
1 1
2 1
3 1
1 2


> SHOW VIEWS;
data_view
mz_arrangement_sharing
mz_arrangement_sizes
mz_catalog_names
mz_dataflow_channels
mz_dataflow_operator_addresses
mz_dataflow_operators
mz_materialization_dependencies
mz_materializations
mz_peek_active
mz_peek_durations
mz_scheduling_elapsed
mz_scheduling_histogram
mz_scheduling_parks
mz_view_foreign_keys
mz_view_frontiers
mz_view_keys

> CREATE MATERIALIZED VIEW test1 AS
  SELECT b, sum(a) FROM data GROUP BY b;

> PEEK test1;
b  sum
------
1  6
2  1

> SHOW COLUMNS FROM test1;
Field Nullable Type
-------------------
b     NO       int8
sum   YES      int8

> SHOW VIEWS LIKE '%peek%';
mz_peek_active
mz_peek_durations

> SHOW VIEWS LIKE '%data%';
data_view
mz_dataflow_channels
mz_dataflow_operator_addresses
mz_dataflow_operators

> SHOW CREATE VIEW test1
View   Create View
------------------
test1  "CREATE VIEW test1 AS SELECT b, sum(a) FROM data GROUP BY b"

> CREATE MATERIALIZED VIEW test2 AS
  SELECT b, 1 + sum(a + 1) FROM data GROUP BY b;

> PEEK test2
b  ?column?
-----------
1  10
2  3

> CREATE MATERIALIZED VIEW test3 AS
  SELECT b, min(a) FROM data GROUP BY b;

> PEEK test3
b  min
------
1  1
2  1

> CREATE MATERIALIZED VIEW test4 AS
  SELECT b, max(a) FROM data GROUP BY b;

> PEEK test4
b  max
------
1  3
2  1

> CREATE MATERIALIZED VIEW test5 AS
  SELECT b, max(a) AS c FROM data GROUP BY b;

> PEEK test5
b  c
------
1  3
2  1

> CREATE INDEX idx1 ON test5(c)

! PEEK idx1
catalog item 'idx1' is an index and so cannot be depended upon

! SELECT * from idx1
catalog item 'idx1' is an index and so cannot be depended upon

# TODO@jldlaughlin: Investigate below failures. Write more source tests.
# N.B. it is important to test sinks that depend on sources directly vs. sinks
# that depend on views, as the code paths are different.

#> CREATE SINK data_sink FROM data INTO 'kafka://${testdrive.kafka-addr}/data-sink'
#> CREATE SINK test1_sink FROM data INTO 'kafka://${testdrive.kafka-addr}/test1-sink'
