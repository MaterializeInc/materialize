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

# Regression test: fixed-decimal encoding must left-pad unscaled bytes to the
# exact fixed size. Without padding, the encoder writes fewer bytes than
# fixed_size, causing UnexpectedEof on decode.

$ set padding-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "a",
        "type": {
          "type": "fixed",
          "name": "dec4",
          "size": 4,
          "logicalType": "decimal",
          "precision": 9,
          "scale": 2
        }
      }
    ]
  }

$ kafka-create-topic topic=avro-decimal-fixed-padding

# Value 2.56: unscaled=256=0x0100, only 2 bytes.
# Encoder must left-pad to [0, 0, 1, 0] for the 4-byte fixed.
$ kafka-ingest format=avro topic=avro-decimal-fixed-padding schema=${padding-schema}
{"a": [1, 0]}

> BEGIN

> CREATE SOURCE avro_decimal_fixed_padding
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-decimal-fixed-padding-${testdrive.seed}')

> CREATE TABLE avro_decimal_fixed_padding_tbl FROM SOURCE avro_decimal_fixed_padding (REFERENCE "testdrive-avro-decimal-fixed-padding-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${padding-schema}'

> COMMIT

> SELECT * FROM avro_decimal_fixed_padding_tbl
2.56

# Both 17-byte coefficients exceed 39 digits and fit in NUMERIC at scale 2.

$ set signflip-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "a",
        "type": {
          "type": "fixed",
          "name": "decimal17",
          "size": 17,
          "logicalType": "decimal",
          "precision": 39,
          "scale": 2
        }
      }
    ]
  }

$ kafka-create-topic topic=avro-decimal-signflip

$ kafka-ingest format=avro topic=avro-decimal-signflip schema=${signflip-schema}
{"a": [226, 156, 214, 14, 60, 163, 91, 64, 84, 70, 10, 159, 0, 0, 0, 0, 0]}
{"a": [226, 156, 214, 14, 60, 163, 91, 64, 84, 70, 10, 158, 255, 255, 255, 216, 240]}

> BEGIN
> CREATE SOURCE avro_decimal_signflip
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-decimal-signflip-${testdrive.seed}')

> CREATE TABLE avro_decimal_signflip_tbl FROM SOURCE avro_decimal_signflip (REFERENCE "testdrive-avro-decimal-signflip-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${signflip-schema}'
> COMMIT

> SELECT a = '-1e38'::numeric, a = '-1e38'::numeric - 100 FROM avro_decimal_signflip_tbl
false true
true false

$ set signflip-bytes-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {
        "name": "a",
        "type": {
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 39,
          "scale": 2
        }
      }
    ]
  }

$ kafka-create-topic topic=avro-decimal-signflip-bytes

$ kafka-ingest format=avro topic=avro-decimal-signflip-bytes schema=${signflip-bytes-schema}
{"a": [226, 156, 214, 14, 60, 163, 91, 64, 84, 70, 10, 159, 0, 0, 0, 0, 0]}

> BEGIN
> CREATE SOURCE avro_decimal_signflip_bytes
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-decimal-signflip-bytes-${testdrive.seed}')

> CREATE TABLE avro_decimal_signflip_bytes_tbl FROM SOURCE avro_decimal_signflip_bytes (REFERENCE "testdrive-avro-decimal-signflip-bytes-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${signflip-bytes-schema}'
> COMMIT

> SELECT a = '-1e38'::numeric FROM avro_decimal_signflip_bytes_tbl
true
