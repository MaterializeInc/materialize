# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Regression test for parse_fixed decimal precision check overflow.
# For fixed-size decimals with size >= 9 bytes, the computation
# 2_usize.pow(8*size-1) overflows usize, causing a panic in debug
# builds or silently losing the decimal logical type in release builds.
# This test uses a 16-byte fixed decimal (decimal128, precision=38).

$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "a",
        "type": {
          "type": "fixed",
          "name": "decimal128",
          "size": 16,
          "logicalType": "decimal",
          "precision": 38,
          "scale": 2
        }
      }
    ]
  }

$ kafka-create-topic topic=avro-decimal-fixed-large

# Value 12345.67 => unscaled 1234567 => 16-byte big-endian: 0x00...0012D687
$ kafka-ingest format=avro topic=avro-decimal-fixed-large schema=${schema}
{"a": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 214, 135]}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE avro_decimal_fixed_large
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-decimal-fixed-large-${testdrive.seed}')

> CREATE TABLE avro_decimal_fixed_large_tbl FROM SOURCE avro_decimal_fixed_large (REFERENCE "testdrive-avro-decimal-fixed-large-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
> COMMIT

> SELECT * FROM avro_decimal_fixed_large_tbl
12345.67
