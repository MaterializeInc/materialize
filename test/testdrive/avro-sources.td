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
              {"name": "d", "type": "Bool"},
              {"name": "e", "type": ["null",{
                "type": "record",
                "name": "nested_data_1",
                "fields": [
                    {"name": "n1_a", "type": "long"},
                    {"name": "n1_b", "type": ["null", "double", {
                        "type": "record",
                        "name": "nested_data_2",
                        "fields": [
                          {"name": "n2_a", "type": "long"},
                          {"name": "n2_b", "type": "int"}
                        ]
                      }]
                    }
                  ]
                }]
              },
              {"name": "f", "type": ["null", "nested_data_2"]}
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
{"before": null, "after": {"a": 1, "b": 1, "json": "null", "c": "True", "d": "False", "e": {"n1_a": 42, "n1_b": 86.5}, "f": null}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound", "e": {"n1_a": 43, "n1_b":{"n2_a": 44, "n2_b": -1}}, "f": {"n2_a": 45, "n2_b": -2}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": -1, "b": 7, "json": "[1, 2, 3]", "c": "FileNotFound", "d": "True", "e": null, "f": null}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": false}}

# Create a source using an inline schema.

> CREATE MATERIALIZED SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_inline
Source   "Create Source"
------------------------
materialize.public.data_schema_inline "CREATE SOURCE \"materialize\".\"public\".\"data_schema_inline\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"},             {\"name\": \"e\", \"type\": [\"null\",{               \"type\": \"record\",               \"name\": \"nested_data_1\",               \"fields\": [                   {\"name\": \"n1_a\", \"type\": \"long\"},                   {\"name\": \"n1_b\", \"type\": [\"null\", \"double\", {                       \"type\": \"record\",                       \"name\": \"nested_data_2\",                       \"fields\": [                         {\"name\": \"n2_a\", \"type\": \"long\"},                         {\"name\": \"n2_b\", \"type\": \"int\"}                       ]                     }]                   }                 ]               }]             },             {\"name\": \"f\", \"type\": [\"null\", \"nested_data_2\"]}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     }   ] }\' ENVELOPE DEBEZIUM"

> SELECT a, b, json, c, d, e::text, f::text FROM data_schema_inline
a  b  json                     c            d             e                   f
------------------------------------------------------------------------------------
1  1  null                     True         False         "(42,86.5,)"        <null>
2  3  "{\"hello\":\"world\"}"  False        FileNotFound  "(43,,\"(44,-1)\")" (45,-2)
-1  7 "[1.0,2.0,3.0]"          FileNotFound True          <null>              <null>

> CREATE MATERIALIZED SOURCE fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (start_offset=2)
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT a, b, json, c, d FROM fast_forwarded
a  b  json            c            d
---------------------------------------
-1  7 "[1.0,2.0,3.0]" FileNotFound True

# Check that repeated Debezium messages are skipped.
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound", "e": null, "f": null}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": false}}
{"before": null, "after": {"a": 42, "b": 19, "json": "[4, 5, 6]", "c": "FileNotFound", "d": "True", "e": null, "f": null}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": false}}

> SELECT a, b, json, c, d, e::text, f::text FROM data_schema_inline
a  b  json                     c            d             e                   f
------------------------------------------------------------------------------------
1  1  null                     True         False         "(42,86.5,)"        <null>
2  3  "{\"hello\":\"world\"}"  False        FileNotFound  "(43,,\"(44,-1)\")" (45,-2)
-1  7 "[1.0,2.0,3.0]"          FileNotFound True          <null>              <null>
42 19 "[4.0,5.0,6.0]"          FileNotFound True          <null>              <null>

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
Source   "Create Source"
------------------------
materialize.public.data_schema_file  "CREATE SOURCE \"materialize\".\"public\".\"data_schema_file\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA \'{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"},             {\"name\": \"e\", \"type\": [\"null\",{               \"type\": \"record\",               \"name\": \"nested_data_1\",               \"fields\": [                   {\"name\": \"n1_a\", \"type\": \"long\"},                   {\"name\": \"n1_b\", \"type\": [\"null\", \"double\", {                       \"type\": \"record\",                       \"name\": \"nested_data_2\",                       \"fields\": [                         {\"name\": \"n2_a\", \"type\": \"long\"},                         {\"name\": \"n2_b\", \"type\": \"int\"}                       ]                     }]                   }                 ]               }]             },             {\"name\": \"f\", \"type\": [\"null\", \"nested_data_2\"]}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     }   ] }\n\' ENVELOPE DEBEZIUM"

> SELECT a, b, json, c, d FROM data_schema_file
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

# Test that deduplication=ordered does the same thing as the default.

> CREATE MATERIALIZED SOURCE new_dbz2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-new-dbz-data-${testdrive.seed}'
  WITH (deduplication = 'ordered')
  FORMAT AVRO USING SCHEMA '${new-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM new_dbz2
a b
---
9 10
11 11
14 6
2 3
-1 7

# Test that deduplication=full accepts mis-ordered Debezium data.

$ kafka-create-topic topic=misordered-dbz-data

$ kafka-ingest format=avro topic=misordered-dbz-data schema=${new-dbz-schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": 2, "b": 3}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": -1, "b": 7}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": "false"}}
{"before": null, "after": {"a": 2, "b": 3}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": 1, "b": 1}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": "false"}}
{"before": null, "after": {"a": 3, "b": 4}, "source": {"file": "binlog2", "pos": 2, "row": 0, "snapshot": "false"}}

> CREATE MATERIALIZED SOURCE misordered_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-misordered-dbz-data-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING SCHEMA '${new-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM misordered_dbz
a b
---
1 1
2 3
-1 7
3 4

! CREATE SOURCE recurisve
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'ignored'
  FORMAT AVRO USING SCHEMA '{"type":"record","name":"a","fields":[{"name":"f","type":["a","null"]}]}'
validating avro value schema: Recursively defined type in schema: .a

$ set key-schema={"type": "string"}
$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "a", "type": "string"}]}

$ kafka-create-topic topic=non-subset-key

$ kafka-ingest format=avro topic=non-subset-key key-format=avro key-schema=${key-schema} schema=${value-schema} publish=true
"asdf" {"a": "asdf"}

> CREATE MATERIALIZED SOURCE non_subset_key
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-subset-key-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

> SELECT * FROM non_subset_key
a
---
"asdf"

# Test that Postgres-style sources can be ingested.
$ set pg-dbz-schema={
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
          "namespace": "whatever",
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
              "name": "lsn",
              "type": ["long", "null"]
            }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=pg-dbz-data

# The third record will be skipped, since `lsn` has gone backwards.
$ kafka-ingest format=avro topic=pg-dbz-data schema=${pg-dbz-schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1}, "source": {"lsn": 1, "snapshot": "false"}}
{"before": null, "after": {"a": 2, "b": 3}, "source": {"lsn": 2, "snapshot": "false"}}
{"before": null, "after": {"a": -1, "b": 7}, "source": {"lsn": 0, "snapshot": "false"}}

> CREATE MATERIALIZED SOURCE pg_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-pg-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${pg-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM pg_dbz
a b
---
1 1
2 3
