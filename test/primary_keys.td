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

$ set key_schema_missing={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "b", "type": "long"}
    ]
    }

$ set key_schema_wrong_type={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "a", "type": "int"}
    ]
    }

$ set valid_key_schema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "a", "type": "long"}
    ]
    }

$ kafka-ingest format=avro topic=missing-data schema=${schema_v1} key_schema=${key_schema_missing} publish=true timestamp=1
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=missing-data schema=${schema_v1} key_schema=${key_schema_missing} publish=true timestamp=2
{"before": null, "after": null}

! CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/testdrive-missing-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}';
Value schema missing primary key column: b

$ kafka-ingest format=avro topic=mismatched-data schema=${schema_v1} key_schema=${key_schema_wrong_type} publish=true timestamp=3
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=mismatched-data schema=${schema_v1} key_schema=${key_schema_wrong_type} publish=true timestamp=4
{"before": null, "after": null}

! CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/testdrive-mismatched-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}';
key and value column types do not match: key ColumnType { name: Some("a"), nullable: false, scalar_type: Int32 } vs. value ColumnType { name: Some("a"), nullable: false, scalar_type: Int64

$ kafka-ingest format=avro topic=data schema=${schema_v1} key_schema=${valid_key_schema} publish=true timestamp=5
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=data schema=${schema_v1} key_schema=${valid_key_schema} publish=true timestamp=6
{"before": null, "after": null}

> CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> PEEK data_v1
a
---
1
