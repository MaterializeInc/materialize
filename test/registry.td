# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set schema_v1={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "RowBefore",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      {
        "name": "after",
        "type": [
          {
            "name": "RowAfter",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      }
    ]
  }

$ set schema_v2={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "RowBefore",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long", "default": 42}
            ]
          },
          "null"
        ]
      },
      {
        "name": "after",
        "type": [
          {
            "name": "RowAfter",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long", "default": 42}
            ]
          },
          "null"
        ]
      }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema_v1} publish=true timestamp=1
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=data schema=${schema_v1} publish=true timestamp=2
{"before": null, "after": null}

> CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> PEEK data_v1
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema_v2} publish=true timestamp=3
{"before": null, "after": {"a": 2, "b": -1}}

$ kafka-ingest format=avro topic=data schema=${schema_v2} publish=true timestamp=4
{"before": null, "after": null}

> CREATE SOURCE data_v2 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> PEEK data_v1
a
---
1
2

> PEEK data_v2
a b
----
1 42
2 -1

$ kafka-ingest format=avro topic=my_topic schema=${schema_v2} publish=true timestamp=5
{"before": null, "after": {"a": 2, "b": -1}}

$ kafka-ingest format=avro topic=my_topic schema=${schema_v2} publish=true timestamp=6
{"before": null, "after": null}

> CREATE SOURCES LIKE '%testdrive-my_topic%' FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---
testdrive-my_topic-${testdrive.seed}

> CREATE SOURCES FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---
testdrive-data-${testdrive.seed}

> CREATE SOURCES FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---

