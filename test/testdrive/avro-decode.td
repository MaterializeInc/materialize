# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test complex reordered/resolved schemas.
# The schemas are mostly a copy and paste from test_complex_resolutions
# in mod avro. The code duplication is warranted because making that
# test exercise the new decode/deserialize API would
# invert the dependency structure, requiring mod avro to depend
# on core materialize.

$ set writer-schema={
            "name": "some_record",
            "type": "record",
            "fields": [
                {
                    "name": "f5",
                    "type": "long"
                },
                {
                    "name": "f4",
                    "type": [
                        "long",
                        {
                            "name": "variant1",
                            "type": "fixed",
                            "size": 1
                        },
                        "null",
                        {
                            "name": "variant2",
                            "type": "fixed",
                            "size": 2
                        }
                    ]
                },
                {
                    "name": "f3",
                    "type": "long"
                },
                {
                    "name": "f2",
                    "type": {
                        "type": "enum",
                        "symbols": ["Clubs", "Diamonds", "Hearts", "Spades"],
                        "name": "Suit"
                    }
                },
                {
                    "name": "f1",
                    "type": [
                        {
                            "name": "variant3",
                            "type": "fixed",
                            "size": 3
                        },
                        "long",
                        {
                            "name": "variant4",
                            "type": "fixed",
                            "size": 4
                        }
                    ]
                },
                {
                    "name": "f6",
                    "type": {
                        "type": "map",
                        "values": "long"
                    }
                }
            ]
        }

$ set reader-schema={
          "name": "some_record",
          "type": "record",
          "fields": [
              {
                  "name": "f0",
                  "type": {
                      "type": "record",
                      "name": "f0_value",
                      "fields": [
                          {
                              "name": "f0_0",
                              "type": "long"
                          },
                          {
                              "name": "f0_1",
                              "type": [
                                  {
                                      "type": "enum",
                                      "symbols": ["foo", "bar", "blah"],
                                      "name": "some_enum"
                                  },
                                  "null"
                              ]
                          }
                      ]
                  },
                  "default": {"f0_1": "bar", "f0_0": 7777}
              },
              {
                  "name": "f1",
                  "type": [
                      {
                          "name": "variant4",
                          "type": "fixed",
                          "size": 4
                      },
                      {
                          "name": "variant3",
                          "type": "fixed",
                          "size": 3
                      },
                      "long"
                  ]
              },
              {
                  "name": "f2",
                  "type": {
                      "type": "enum",
                      "symbols": ["Hearts", "Spades", "Diamonds", "Clubs", "Jokers"],
                      "name": "Suit",
                      "default": "Diamonds"
                  }
              },
              {
                  "name": "f5",
                  "type": [
                      {
                          "name": "extra_variant",
                          "type": "fixed",
                          "size": 10
                      },
                      "long"
                    ]
              },
              {
                  "name": "f6",
                  "type": {
                      "type": "map",
                      "values": "long"
                  }
              }
          ]
      }

$ kafka-create-topic topic=avro-data

$ kafka-ingest format=avro topic=avro-data schema=${writer-schema} publish=true timestamp=1
{ "f5": 1234, "f4": {"variant1": [0]}, "f3": 2345, "f2": "Diamonds", "f1": {"variant4": [0, 1, 2, 3]}, "f6": {"hello": 123, "another": 2144} }

$ kafka-ingest format=avro topic=avro-data schema=${reader-schema} publish=true timestamp=1
{ "f0": {"f0_0": 9999, "f0_1": null}, "f1": {"long": 3456}, "f2": "Jokers", "f5": {"extra_variant": [0,1,2,3,4,5,6,7,8,9]}, "f6": {"key": 8372} }

> CREATE SOURCE avro_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> SELECT f0::text, f11, f12, f13, f2, f51, f52
  FROM avro_data
f0 f11 f12 f13 f2 f51 f52
---
(7777,bar) \x00\x01\x02\x03 <null> <null> Diamonds <null> 1234
(9999,) <null> <null> 3456 Jokers \x00\x01\x02\x03\x04\x05\x06\x07\x08\t <null>

# Testdrive prepares statements before they are executed.
# Because maps are a non-standard Postgres type, we can't SELECT them directly.
# TODO@jldlaughlin: Update this test case when we have a binary encoding for maps.
> SELECT f6 -> 'hello' FROM avro_data
<null>
123

$ kafka-create-topic topic=avro-data-no-registry

$ kafka-ingest format=avro topic=avro-data-no-registry schema=${reader-schema} publish=true confluent-wire-format=false timestamp=1
{ "f0": {"f0_0": 9999, "f0_1": null}, "f1": {"long": 3456}, "f2": "Jokers", "f5": {"extra_variant": [0,1,2,3,4,5,6,7,8,9]}, "f6": {"key": 8372} }

> CREATE SOURCE avro_data_no_registry
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-data-no-registry-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${reader-schema}' WITH (CONFLUENT WIRE FORMAT = false)

> SELECT f2
  FROM avro_data_no_registry
Jokers

# Test decoding of corrupted messages
$ kafka-create-topic topic=avro-corrupted-values

$ kafka-ingest format=bytes topic=avro-corrupted-values timestamp=1
garbage

> CREATE SOURCE avro_corrupted_values
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-corrupted-values-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${reader-schema}'

! SELECT f2 FROM avro_corrupted_values
contains:Decode error: Text: avro deserialization error: wrong Confluent-style avro serialization magic: expected 0, got 1

# Test decoding of corrupted messages without magic byte
$ kafka-create-topic topic=avro-corrupted-values2

$ kafka-ingest format=bytes topic=avro-corrupted-values2 timestamp=1
garbage

> CREATE SOURCE avro_corrupted_values2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-corrupted-values2-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${reader-schema}' WITH (CONFLUENT WIRE FORMAT = false)

! SELECT f2 FROM avro_corrupted_values2
contains:Decode error: Decoding error: Expected non-negative integer, got -49
