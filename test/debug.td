# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# This is not meant to be merged into the repo. This is a test file meant to show
# why basic.td is failing.

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

> CREATE VIEW view as SELECT * from data

> PEEK view
a  b
----

> CREATE VIEW same_source_same_time as SELECT a + b as rowsum from data;

> PEEK same_source_same_time
rowsum
----

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

> CREATE VIEW same_source_before_new_data_flush as SELECT a, max(b) FROM data GROUP BY a;

> PEEK view
a  b
----

> PEEK same_source_same_time
rowsum
----


$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW same_source_after_new_data_flush AS
  SELECT b, sum(a) FROM data GROUP BY b;

> PEEK view
a  b
----

> PEEK same_source_same_time
rowsum
----
2
3
4
3

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=44
{"before": null, "after": null}

> PEEK view
a  b
----


> PEEK same_source_same_time
rowsum
----
2
3
4
3

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=45
{"before": null, "after": {"a": 3, "b": 4}}
{"before": null, "after": {"a": 9, "b": 5}}
{"before": null, "after": {"a": 2, "b": 6}}
{"before": null, "after": {"a": 3, "b": 5}}

> PEEK view
a  b
----

> PEEK same_source_same_time
rowsum
----
2
3
4
3

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=46
{"before": null, "after": null}

> PEEK view
a  b
----

> PEEK same_source_same_time
rowsum
----
2
3
4
3
7
14
8
8

