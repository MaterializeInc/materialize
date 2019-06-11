# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set schema_v1={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "a", "type": "long"}
    ]
  }

$ set schema_v2={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "long", "default": 42}
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema_v1} publish=true
{"a": 1}

> CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/data'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> PEEK data_v1
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema_v2} publish=true
{"a": 2, "b": -1}

> CREATE SOURCE data_v2 FROM 'kafka://${testdrive.kafka-addr}/data'
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
