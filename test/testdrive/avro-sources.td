# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for Avro sources without using the Confluent Schema Registry.

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
              {"name": "b", "type": "long"},
              {
                "name": "json",
                "type": {
                  "connect.name": "io.debezium.data.Json",
                  "type": "string"
                }
              }
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1, "json": "null"}}
{"before": null, "after": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}"}}
{"before": null, "after": {"a": -1, "b": 7, "json": "[1, 2, 3]"}}

# Create a source using an inline schema.

> CREATE MATERIALIZED SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_inline
Source   Create Source
------------------
materialize.public.data_schema_inline  "CREATE SOURCE materialize.public.data_schema_inline FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             }           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] }   ] }\' ENVELOPE DEBEZIUM"

> SELECT * FROM data_schema_inline
a  b  json
----------
1  1  null
2  3  "{\"hello\":\"world\"}"
-1  7 "[1.0,2.0,3.0]"

# Create a source using a file schema. This should fail if the named schema file
# does not exist.

! CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE 'data-schema.json'
  ENVELOPE DEBEZIUM
No such file or directory

$ file-append path=data-schema.json
\${schema}

> CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_file
Source   Create Source
------------------
materialize.public.data_schema_file  "CREATE SOURCE materialize.public.data_schema_file FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             }           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] }   ] }\n\' ENVELOPE DEBEZIUM"

> SELECT * FROM data_schema_file
a  b  json
----------
1  1  null
2  3  "{\"hello\":\"world\"}"
-1  7 "[1.0,2.0,3.0]"

# Test an Avro source without a Debezium envelope.

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
{"a": 2, "b": 3}

> CREATE MATERIALIZED SOURCE non_dbz_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data
a b
---
1 2
2 3
