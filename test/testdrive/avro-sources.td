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
              },
              {
                "name": "c",
                "type": {
                  "type": "enum",
                  "name": "Bool",
                  "symbols": ["True", "False", "FileNotFound"]
                }
              },
              {"name": "d", "type": "Bool"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "io.debezium.connector.mysql",
          "fields": [
            {
              "name": "file",
              "type": "string"
            },
            {
              "name": "pos",
              "type": "long"
            },
            {
              "name": "row",
              "type": "int"
            },
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "boolean",
                  "connect.default": false
                },
                "null"
              ],
              "default": false
            }
          ],
          "connect.name": "io.debezium.connector.mysql.Source"
        }
      }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1, "json": "null", "c": "True", "d": "False"}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound"}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": -1, "b": 7, "json": "[1, 2, 3]", "c": "FileNotFound", "d": "True"}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": false}}

# Create a source using an inline schema.

> CREATE MATERIALIZED SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_inline
Source   Create Source
------------------
materialize.public.data_schema_inline "CREATE SOURCE \"materialize\".\"public\".\"data_schema_inline\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     }   ] }\' ENVELOPE DEBEZIUM"

> SELECT * FROM data_schema_inline
a  b  json                     c            d
--------------------------------------------------------
1  1  null                     True         False
2  3  "{\"hello\":\"world\"}"  False        FileNotFound
-1  7 "[1.0,2.0,3.0]"          FileNotFound True

> CREATE MATERIALIZED SOURCE fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (start_offset=2)
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM fast_forwarded
a  b  json            c            d
---------------------------------------
-1  7 "[1.0,2.0,3.0]" FileNotFound True

# Check that repeated Debezium messages are skipped.
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound"}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": 42, "b": 19, "json": "[4, 5, 6]", "c": "FileNotFound", "d": "True"}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": false}}

> SELECT * FROM data_schema_inline
a  b  json                     c            d
--------------------------------------------------------
1  1  null                     True         False
2  3  "{\"hello\":\"world\"}"  False        FileNotFound
-1  7 "[1.0,2.0,3.0]"          FileNotFound True
42 19 "[4.0,5.0,6.0]"          FileNotFound True

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
materialize.public.data_schema_file  "CREATE SOURCE \"materialize\".\"public\".\"data_schema_file\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     }   ] }\n\' ENVELOPE DEBEZIUM"

> SELECT * FROM data_schema_file
a  b  json                     c            d
--------------------------------------------------------
1  1  null                     True         False
2  3  "{\"hello\":\"world\"}"  False        FileNotFound
-1  7 "[1.0,2.0,3.0]"          FileNotFound True
42 19 "[4.0,5.0,6.0]"          FileNotFound True

# Test an Avro source without a Debezium envelope.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"},
      {"name": "u", "type": ["long", "null", "string"]},
      {"name": "n", "type": ["long", "null"]}
    ]
  }

$ kafka-create-topic topic=non-dbz-data

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=1
{"a": 1, "b": 2, "u": 5, "n": 6}
{"a": 2, "b": 3, "u": null, "n": null}
{"a": 3, "b": 4, "u": "hello", "n": 7}

> CREATE MATERIALIZED SOURCE non_dbz_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data
a b u1 u2 n
---
1 2 5 <null> 6
2 3 <null> <null> <null>
3 4 <null> "hello" 7


# Source with new-style three-valued "snapshot".
$ set new-dbz-schema={
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
      { "name": "after", "type": ["row", "null"] },
      {
        "name": "source",
        "type": {
          "type": "record",
          "name": "Source",
          "namespace": "io.debezium.connector.mysql",
          "fields": [
            {
              "name": "snapshot",
              "type": [
                {
                  "type": "string",
                  "connect.version": 1,
                  "connect.parameters": {
                    "allowed": "true,last,false"
                  },
                  "connect.default": "false",
                  "connect.name": "io.debezium.data.Enum"
                },
                "null"
              ],
              "default": "false"
            },
            {
              "name": "file",
              "type": "string"
            },
            {
              "name": "pos",
              "type": "long"
            },
            {
              "name": "row",
              "type": "int"
            }
          ],
          "connect.name": "io.debezium.connector.mysql.Source"
        }
      }
    ]
  }

$ kafka-create-topic topic=new-dbz-data

# We don't do anything sensible yet for snapshot "true" or "last", so just test that those are ingested.

$ kafka-ingest format=avro topic=new-dbz-data schema=${new-dbz-schema} timestamp=1
{"before": null, "after": {"a": 9, "b": 10}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": "true"}}
{"before": null, "after": {"a": 11, "b": 11}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": "last"}}
{"before": null, "after": {"a": 14, "b": 6}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": null}}
{"before": null, "after": {"a": 1, "b": 1}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": 2, "b": 3}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": -1, "b": 7}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": "false"}}
{"before": null, "after": {"a": -1, "b": 7}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": "false"}}

> CREATE MATERIALIZED SOURCE new_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-new-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${new-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM new_dbz
a b
---
9 10
11 11
14 6
2 3
-1 7
