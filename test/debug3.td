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

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW same_source_after_new_data_flush AS
  SELECT b, sum(a) FROM data GROUP BY b;

> PEEK view
a  b
----
1  1
2  1
3  1
1  2

! PEEK same_source_after_new_data_flush
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=44
{"before": null, "after": null}

> PEEK view
a  b
----
1  1
2  1
3  1
1  2

! PEEK same_source_after_new_data_flush;
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=45
{"before": null, "after": {"a": 3, "b": 4}}
{"before": null, "after": {"a": 9, "b": 5}}
{"before": null, "after": {"a": 2, "b": 6}}
{"before": null, "after": {"a": 3, "b": 5}}

> PEEK view
a  b
----
1  1
2  1
3  1
1  2

! PEEK same_source_after_new_data_flush;
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=46
{"before": null, "after": null}

> PEEK view
a  b
----
1  1
2  1
3  1
1  2
3  4
9  5
2  6
3  5

> PEEK same_source_after_new_data_flush;
b  sum
------
1  6
2  1
4  3
5  12
6  2

