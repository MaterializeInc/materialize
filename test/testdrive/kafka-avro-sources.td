# Copyright Materialize, Inc. and contributors. All rights reserved.
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
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "Transaction",
          "namespace": "whatever",
          "fields": [
            {
              "name": "total_order",
              "type": ["long", "null"]
            },
            {
              "name": "id",
              "type": "string"
            }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=data partitions=1

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 1, "b": 1, "json": "null", "c": "True", "d": "False", "e": {"nested_data_1": {"n1_a": 42, "n1_b": {"double": 86.5}}}, "f": null}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound", "e": {"nested_data_1": {"n1_a": 43, "n1_b":{"nested_data_2": {"n2_a": 44, "n2_b": -1}}}}, "f": {"nested_data_2": {"n2_a": 45, "n2_b": -2}}}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": -1, "b": 7, "json": "[1, 2, 3]", "c": "FileNotFound", "d": "True", "e": null, "f": null}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}

# We should refuse to create a source with invalid WITH options
! CREATE MATERIALIZED SOURCE invalid_with_option
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      badoption = true,
      deduplication = 'full_in_range',
      deduplication_pad_start = '2020-09-13 10:00:00',
      deduplication_start = '2020-09-13 12:26:00',
      deduplication_end = '2020-09-13 13:00:00'
  )
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM
contains:unexpected parameters for CREATE SOURCE: badoption

> SHOW SOURCES
name
----


# Create a source using an inline schema.

> CREATE MATERIALIZED SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (deduplication = 'ordered')
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_inline
Source   "Create Source"
------------------------
materialize.public.data_schema_inline "CREATE SOURCE \"materialize\".\"public\".\"data_schema_inline\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' WITH (\"deduplication\" = 'ordered') FORMAT AVRO USING SCHEMA '{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"},             {\"name\": \"e\", \"type\": [\"null\",{               \"type\": \"record\",               \"name\": \"nested_data_1\",               \"fields\": [                   {\"name\": \"n1_a\", \"type\": \"long\"},                   {\"name\": \"n1_b\", \"type\": [\"null\", \"double\", {                       \"type\": \"record\",                       \"name\": \"nested_data_2\",                       \"fields\": [                         {\"name\": \"n2_a\", \"type\": \"long\"},                         {\"name\": \"n2_b\", \"type\": \"int\"}                       ]                     }]                   }                 ]               }]             },             {\"name\": \"f\", \"type\": [\"null\", \"nested_data_2\"]}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     },     {       \"name\": \"transaction\",       \"type\": {         \"type\": \"record\",         \"name\": \"Transaction\",         \"namespace\": \"whatever\",         \"fields\": [           {             \"name\": \"total_order\",             \"type\": [\"long\", \"null\"]           },           {             \"name\": \"id\",             \"type\": \"string\"           }         ]       }     }   ] }' ENVELOPE DEBEZIUM"

> SELECT a, b, json, c, d, e::text, f::text FROM data_schema_inline
a  b  json                     c            d             e                   f
------------------------------------------------------------------------------------
1  1  null                     True         False         "(42,86.5,)"        <null>
2  3  "{\"hello\":\"world\"}"  False        FileNotFound  "(43,,\"(44,-1)\")" (45,-2)
-1  7 "[1,2,3]"                FileNotFound True          <null>              <null>

> CREATE MATERIALIZED SOURCE fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (start_offset=2)
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> SELECT a, b, json, c, d FROM fast_forwarded
a  b  json            c            d
---------------------------------------
-1  7 "[1,2,3]"       FileNotFound True

# Check that repeated Debezium messages are skipped.
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"row": {"a": 2, "b": 3, "json": "{\"hello\": \"world\"}", "c": "False", "d": "FileNotFound", "e": null, "f": null}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}
{"before": null, "after": {"row": {"a": 42, "b": 19, "json": "[4, 5, 6]", "c": "FileNotFound", "d": "True", "e": null, "f": null}}, "source": {"file": "binlog2", "pos": 1, "row": 1, "snapshot": {"boolean": false}}, "transaction": {"total_order": null, "id": "1"}}

> SELECT a, b, json, c, d, e::text, f::text FROM data_schema_inline
a  b  json                     c            d             e                   f
------------------------------------------------------------------------------------
1  1  null                     True         False         "(42,86.5,)"        <null>
2  3  "{\"hello\":\"world\"}"  False        FileNotFound  "(43,,\"(44,-1)\")" (45,-2)
-1  7 "[1,2,3]"                FileNotFound True          <null>              <null>
42 19 "[4,5,6]"                FileNotFound True          <null>              <null>

# Create a source using a file schema. This should fail if the named schema file
# does not exist.

! CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE 'data-schema.json'
  ENVELOPE DEBEZIUM
contains:No such file or directory

$ file-append path=data-schema.json
\${schema}

> CREATE MATERIALIZED SOURCE data_schema_file
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA FILE '${testdrive.temp-dir}/data-schema.json'
  ENVELOPE DEBEZIUM

> SHOW CREATE SOURCE data_schema_file
Source   "Create Source"
------------------------
materialize.public.data_schema_file "CREATE SOURCE \"materialize\".\"public\".\"data_schema_file\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' FORMAT AVRO USING SCHEMA '{   \"type\": \"record\",   \"name\": \"envelope\",   \"fields\": [     {       \"name\": \"before\",       \"type\": [         {           \"name\": \"row\",           \"type\": \"record\",           \"fields\": [             {\"name\": \"a\", \"type\": \"long\"},             {\"name\": \"b\", \"type\": \"long\"},             {               \"name\": \"json\",               \"type\": {                 \"connect.name\": \"io.debezium.data.Json\",                 \"type\": \"string\"               }             },             {               \"name\": \"c\",               \"type\": {                 \"type\": \"enum\",                 \"name\": \"Bool\",                 \"symbols\": [\"True\", \"False\", \"FileNotFound\"]               }             },             {\"name\": \"d\", \"type\": \"Bool\"},             {\"name\": \"e\", \"type\": [\"null\",{               \"type\": \"record\",               \"name\": \"nested_data_1\",               \"fields\": [                   {\"name\": \"n1_a\", \"type\": \"long\"},                   {\"name\": \"n1_b\", \"type\": [\"null\", \"double\", {                       \"type\": \"record\",                       \"name\": \"nested_data_2\",                       \"fields\": [                         {\"name\": \"n2_a\", \"type\": \"long\"},                         {\"name\": \"n2_b\", \"type\": \"int\"}                       ]                     }]                   }                 ]               }]             },             {\"name\": \"f\", \"type\": [\"null\", \"nested_data_2\"]}           ]         },         \"null\"       ]     },     { \"name\": \"after\", \"type\": [\"row\", \"null\"] },     {       \"name\": \"source\",       \"type\": {         \"type\": \"record\",         \"name\": \"Source\",         \"namespace\": \"io.debezium.connector.mysql\",         \"fields\": [           {             \"name\": \"file\",             \"type\": \"string\"           },           {             \"name\": \"pos\",             \"type\": \"long\"           },           {             \"name\": \"row\",             \"type\": \"int\"           },           {             \"name\": \"snapshot\",             \"type\": [               {                 \"type\": \"boolean\",                 \"connect.default\": false               },               \"null\"             ],             \"default\": false           }         ],         \"connect.name\": \"io.debezium.connector.mysql.Source\"       }     },     {       \"name\": \"transaction\",       \"type\": {         \"type\": \"record\",         \"name\": \"Transaction\",         \"namespace\": \"whatever\",         \"fields\": [           {             \"name\": \"total_order\",             \"type\": [\"long\", \"null\"]           },           {             \"name\": \"id\",             \"type\": \"string\"           }         ]       }     }   ] }\n' WITH (\"confluent_wire_format\" = true) ENVELOPE DEBEZIUM"

> SELECT a, b, json, c, d FROM data_schema_file
a  b  json                     c            d
--------------------------------------------------------
1  1  null                     True         False
2  3  "{\"hello\":\"world\"}"  False        FileNotFound
-1  7 "[1,2,3]"                FileNotFound True
42 19 "[4,5,6]"                FileNotFound True

# Test an Avro source without a Debezium envelope.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=non-dbz-data partitions=1

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

# test INCLUDE metadata

! CREATE MATERIALIZED SOURCE non_dbz_data_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  INCLUDE TOPIC
  ENVELOPE NONE
contains:INCLUDE TOPIC not yet supported

> CREATE MATERIALIZED SOURCE non_dbz_data_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  INCLUDE PARTITION, OFFSET
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_metadata
a b partition offset
--------------------
1 2 0         1
2 3 0         2

> CREATE MATERIALIZED SOURCE non_dbz_data_metadata_named
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  INCLUDE PARTITION as part, OFFSET as mzo
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_metadata_named
a b  part  mzo
--------------
1 2  0     1
2 3  0     2

# Test an Avro source without a Debezium envelope starting at specified partition offsets.

$ kafka-create-topic topic=non-dbz-data-multi-partition partitions=2

$ kafka-ingest format=avro topic=non-dbz-data-multi-partition schema=${non-dbz-schema} timestamp=1 partition=1
{"a": 4, "b": 1}

$ kafka-ingest format=avro topic=non-dbz-data-multi-partition schema=${non-dbz-schema} timestamp=1 partition=0
{"a": 1, "b": 2}

> CREATE MATERIALIZED SOURCE non_dbz_data_multi_partition
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-multi-partition-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_multi_partition
a  b
-----
4  1

> CREATE MATERIALIZED SOURCE non_dbz_data_multi_partition_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-multi-partition-${testdrive.seed}'
  WITH (start_offset=[0,0])
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_multi_partition_2
a  b
-----
1  2
4  1

> CREATE MATERIALIZED SOURCE non_dbz_data_multi_partition_fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-multi-partition-${testdrive.seed}'
  WITH (start_offset=[0,1])
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_multi_partition_fast_forwarded
a  b
----
1  2

> CREATE MATERIALIZED SOURCE non_dbz_data_multi_partition_fast_forwarded_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-multi-partition-${testdrive.seed}'
  WITH (start_offset=[1,0])
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_multi_partition_fast_forwarded_2
a  b
----
4  1

# Test an Avro source without a Debezium envelope with specified offsets and varying partition numbers.

$ kafka-create-topic topic=non-dbz-data-varying-partition partitions=1

$ kafka-ingest format=avro topic=non-dbz-data-varying-partition schema=${non-dbz-schema} timestamp=1 partition=0
{"a": 5, "b": 6}

> CREATE MATERIALIZED SOURCE non_dbz_data_varying_partition
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-varying-partition-${testdrive.seed}'
  WITH (start_offset=1, topic_metadata_refresh_interval_ms=10)
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_varying_partition

# Erroneously adds start_offsets for non-existent partitions.
> CREATE MATERIALIZED SOURCE non_dbz_data_varying_partition_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-varying-partition-${testdrive.seed}'
  WITH (start_offset=[0,1], topic_metadata_refresh_interval_ms=10)
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> SELECT * FROM non_dbz_data_varying_partition_2
a  b
----
5  6

$ kafka-add-partitions topic=non-dbz-data-varying-partition total-partitions=2

# Reading data that's ingested to a new partition takes longer than the default timeout.
$ set-sql-timeout duration=180s

$ kafka-ingest format=avro topic=non-dbz-data-varying-partition schema=${non-dbz-schema} timestamp=1 partition=1
{"a": 7, "b": 8}
{"a": 9, "b": 10}

# Because the start offset for any new partitions will be 0, the first record sent to the new
# partition will be included.
> SELECT * FROM non_dbz_data_varying_partition
a  b
-----
7  8
9  10

# Because the start offsets erronously included an offset for partition 1 (which didn't exist at the time),
# the first record ingested into partition 1 will be ignored.
> SELECT * FROM non_dbz_data_varying_partition_2
a  b
-----
5  6
9  10

> CREATE MATERIALIZED SOURCE non_dbz_data_varying_partition_3
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-varying-partition-${testdrive.seed}'
  WITH (start_offset=[1,1], topic_metadata_refresh_interval_ms=10)
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

$ kafka-add-partitions topic=non-dbz-data-varying-partition total-partitions=3

$ kafka-ingest format=avro topic=non-dbz-data-varying-partition schema=${non-dbz-schema} timestamp=1 partition=2
{"a": 11, "b": 12}

# Because the start offset for any new partitions will be 0, the first record sent to the new
# partition will be included.
> SELECT * FROM non_dbz_data_varying_partition_3
a  b
-----
9  10
11 12

$ set-sql-timeout duration=default

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
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "Transaction",
          "namespace": "whatever",
          "fields": [
            {
              "name": "total_order",
              "type": ["long", "null"]
            }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=new-dbz-data partitions=1

# We don't do anything sensible yet for snapshot "true" or "last", so just test that those are ingested.

$ kafka-ingest format=avro topic=new-dbz-data schema=${new-dbz-schema} timestamp=1
{"before": null, "after": {"row":{"a": 9, "b": 10}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 11, "b": 11}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"string": "last"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 14, "b": 6}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": null}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 0, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": -1, "b": 7}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": -1, "b": 7}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

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

$ set new-dbz-key-schema={"type": "record", "name": "row", "fields": [{"name": "a", "type": "long"}]}

$ kafka-ingest format=avro key-format=avro topic=misordered-dbz-data schema=${new-dbz-schema} key-schema=${new-dbz-key-schema} timestamp=1 publish=true
{"a": 5} {"before": null, "after": {"row":{"a": 5, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}
{"a": 5} {"before": null, "after": {"row":{"a": 5, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}
{"a": 1} {"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 2} {"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": -1} {"before": null, "after": {"row":{"a": -1, "b": 7}}, "source": {"file": "binlog", "pos": 1, "row": 1, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 2} {"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 1} {"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"file": "binlog", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"a": 3} {"before": null, "after": {"row":{"a": 3, "b": 4}}, "source": {"file": "binlog2", "pos": 2, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

> CREATE MATERIALIZED SOURCE misordered_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-misordered-dbz-data-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM misordered_dbz
a b
---
1 1
2 3
-1 7
3 4
5 3
#
# Test that deduplication=full bails on invalid keys

$ kafka-create-topic topic=invalid-keyed-dbz-data

$ set wrong-dbz-key-schema={"type": "record", "name": "row", "fields": [{"name": "c", "type": "long"}]}

$ kafka-ingest format=avro key-format=avro topic=invalid-keyed-dbz-data schema=${new-dbz-schema} key-schema=${wrong-dbz-key-schema} timestamp=1 publish=true
{"c": 5} {"before": null, "after": {"row":{"a": 5, "b": 3}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "true"}}, "transaction": {"total_order": null}}

! CREATE MATERIALIZED SOURCE invalid_keyed_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-invalid-keyed-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains:Value schema missing primary key column: c

! CREATE MATERIALIZED SOURCE invalid_keyed_dbz_with_dedup
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-invalid-keyed-dbz-data-${testdrive.seed}'
  WITH (deduplication = 'full')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains:Value schema missing primary key column: c

! CREATE MATERIALIZED SOURCE invalid_keyed_dbz_with_range_dedup
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-invalid-keyed-dbz-data-${testdrive.seed}'
  WITH (
      deduplication = 'full_in_range',
      deduplication_start = '2020-09-13 12:26:00',
      deduplication_end = '2020-09-13 13:00:00'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains:Value schema missing primary key column: c

# Test that deduplication=full_in_range accepts mis-ordered debezium data in the range

$ kafka-create-topic topic=misordered-dbz-in-range-data

# timestamps:
# * 1599990000000 = 2020-09-13 09:40 -- more than an hour before the duplicate items
# * 1599999600000 = 2020-09-13 12:20 -- in the hard-coded padding window
# * 1600000000000 = 2020-09-13 12:26 -- duplicate items
# * 1600005000000 = 2020-09-13 13:50 -- no longer possibly duplicate

$ kafka-ingest format=avro topic=misordered-dbz-in-range-data schema=${new-dbz-schema} timestamp=1599990000000
{"before": null, "after": {"row":{"a": 1, "b": 0}}, "source": {"file": "binlog", "pos": 1, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

# padding window
$ kafka-ingest format=avro topic=misordered-dbz-in-range-data schema=${new-dbz-schema} timestamp=1599999600000
{"before": null, "after": {"row":{"a": 4, "b": 0}}, "source": {"file": "binlog", "pos": 4, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

$ kafka-ingest format=avro topic=misordered-dbz-in-range-data schema=${new-dbz-schema} timestamp=1600000000000
{"before": null, "after": {"row":{"a": 3, "b": 1}}, "source": {"file": "binlog", "pos": 3, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 4, "b": 0}}, "source": {"file": "binlog", "pos": 4, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

# full dedupe catches reversed, invalid data
$ kafka-ingest format=avro topic=misordered-dbz-in-range-data schema=${new-dbz-schema} timestamp=1600000000000
{"before": null, "after": {"row":{"a": 6, "b": 0}}, "source": {"file": "binlog", "pos": 6, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 5, "b": 0}}, "source": {"file": "binlog", "pos": 5, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

# return to just binlog-based deduplication, item 7 will be stripped because it is out of order!
$ kafka-ingest format=avro topic=misordered-dbz-in-range-data schema=${new-dbz-schema} timestamp=1600005000000
{"before": null, "after": {"row":{"a": 8, "b": 0}}, "source": {"file": "binlog", "pos": 8, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"row":{"a": 7, "b": 0}}, "source": {"file": "binlog", "pos": 7, "row": 0, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

> CREATE MATERIALIZED SOURCE misordered_dbz_in_range
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-misordered-dbz-in-range-data-${testdrive.seed}'
  WITH (
      deduplication = 'full_in_range',
      deduplication_start = '2020-09-13 12:26:00',
      deduplication_end = '2020-09-13 13:00:00'
  )
  FORMAT AVRO USING SCHEMA '${new-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM misordered_dbz_in_range
a b
---
1 0
4 0
3 1
6 0
5 0
8 0

# same thing, but with an explicit pad start
> CREATE MATERIALIZED SOURCE misordered_dbz_in_range_with_start
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-misordered-dbz-in-range-data-${testdrive.seed}'
  WITH (
      deduplication = 'full_in_range',
      deduplication_pad_start = '2020-09-13 10:00:00',
      deduplication_start = '2020-09-13 12:26:00',
      deduplication_end = '2020-09-13 13:00:00'
  )
  FORMAT AVRO USING SCHEMA '${new-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM misordered_dbz_in_range_with_start
a b
---
1 0
4 0
3 1
6 0
5 0
8 0

! CREATE SOURCE recursive
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'ignored'
  FORMAT AVRO USING SCHEMA '{"type":"record","name":"a","fields":[{"name":"f","type":["a","null"]}]}'
contains:validating avro schema: Recursive types are not supported: .a

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
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "TransactionMetadata",
          "fields": [
            {
              "name": "total_order",
              "type": "long"
            }
          ]
        }
      }
    ]
  }

$ kafka-create-topic topic=pg-dbz-data partitions=1

# The third and fourth records will be skipped, since `(lsn, total_order)` has gone backwards.
$ kafka-ingest format=avro topic=pg-dbz-data schema=${pg-dbz-schema} timestamp=1
{"before": null, "after": {"row":{"a": 1, "b": 1}}, "source": {"lsn": {"long": 1}, "snapshot": {"string": "false"}}, "transaction": {"total_order": 0}}
{"before": null, "after": {"row":{"a": 2, "b": 3}}, "source": {"lsn": {"long": 2}, "snapshot": {"string": "false"}}, "transaction": {"total_order": 1}}
{"before": null, "after": {"row":{"a": -1, "b": 7}}, "source": {"lsn": {"long": 0}, "snapshot": {"string": "false"}}, "transaction": {"total_order": 0}}
{"before": null, "after": {"row":{"a": 4, "b": 5}}, "source": {"lsn": {"long": 2}, "snapshot": {"string": "false"}}, "transaction": {"total_order": 0}}

> CREATE MATERIALIZED SOURCE pg_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-pg-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${pg-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM pg_dbz
a b
---
1 1
2 3

# Test that SQL Server-style sources can be ingested.
$ set ms-dbz-schema={
    "connect.name": "com.materialize.test.Envelope",
    "fields": [
      {
        "default": null,
        "name": "before",
        "type": [
          "null",
          {
            "connect.name": "com.materialize.test.Value",
            "fields": [
              {
                "name": "a",
                "type": "int"
              },
              {
                "name": "b",
                "type": "int"
              }
            ],
            "name": "Value",
            "type": "record"
          }
        ]
      },
      {
        "default": null,
        "name": "after",
        "type": [
          "null",
          "Value"
        ]
      },
      {
        "name": "source",
        "type": {
          "connect.name": "io.debezium.connector.sqlserver.Source",
          "fields": [
            {
              "default": "false",
              "name": "snapshot",
              "type": [
                {
                  "connect.default": "false",
                  "connect.name": "io.debezium.data.Enum",
                  "connect.parameters": {
                    "allowed": "true,last,false"
                  },
                  "connect.version": 1,
                  "type": "string"
                },
                "null"
              ]
            },
            {
              "default": null,
              "name": "change_lsn",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "default": null,
              "name": "sequence",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "default": null,
              "name": "event_serial_no",
              "type": [
                "null",
                "long"
              ]
            }
          ],
          "name": "Source",
          "namespace": "io.debezium.connector.sqlserver",
          "type": "record"
        }
      },
      {
        "name": "transaction",
        "type": {
          "type": "record",
          "name": "Transaction",
          "namespace": "whatever",
          "fields": [
            {
              "name": "total_order",
              "type": ["long", "null"]
            }
          ]
        }
      }
    ],
    "name": "Envelope",
    "namespace": "com.materialize.test",
    "type": "record"
  }

$ kafka-create-topic topic=ms-dbz-data partitions=1

# The third record will be skipped, since `lsn` has gone backwards.
$ kafka-ingest format=avro topic=ms-dbz-data schema=${ms-dbz-schema} timestamp=1
{"before": null, "after": {"Value":{"a": 1, "b": 1}}, "source": {"change_lsn": {"string": "00000025:00000728:001b"}, "sequence": null, "event_serial_no": {"long": 1}, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"Value":{"a": 2, "b": 3}}, "source": {"change_lsn": {"string": "00000025:00000728:001c"}, "sequence": null, "event_serial_no": {"long": 1}, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}
{"before": null, "after": {"Value":{"a": -1, "b": 7}}, "source": {"change_lsn": {"string": "00000025:00000728:001a"}, "sequence": null, "event_serial_no": {"long": 1}, "snapshot": {"string": "false"}}, "transaction": {"total_order": null}}

> CREATE MATERIALIZED SOURCE ms_dbz
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-ms-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${ms-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM ms_dbz
a b
---
1 1
2 3

> CREATE MATERIALIZED SOURCE ms_dbz_uncommitted
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-ms-dbz-data-${testdrive.seed}'
  WITH (isolation_level='read_uncommitted')
  FORMAT AVRO USING SCHEMA '${ms-dbz-schema}'
  ENVELOPE DEBEZIUM

> SELECT * FROM ms_dbz_uncommitted
a b
---
1 1
2 3
