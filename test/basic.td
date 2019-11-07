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

> PEEK data
a b
---

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> PEEK data
a b
---
1 1
2 1
3 1
1 2

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

> SHOW TABLES;
data
logs_addresses
logs_arrangement
logs_channels
logs_dataflow_dependency
logs_dataflows
logs_elapsed
logs_frontiers
logs_histogram
logs_operates
logs_parks
logs_peek_durations
logs_peeks
logs_sharing

> SHOW TABLES LIKE '%peeks%';
logs_peeks

> SHOW TABLES LIKE '%data%';
data
logs_dataflow_dependency
logs_dataflows

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
  SELECT b, max(a) as c FROM data GROUP BY b;

> PEEK test5
b  c
------
1  3
2  1

> CREATE INDEX idx1 ON test5(c)

! PEEK idx1
unsupported SQL statement: PEEK index
