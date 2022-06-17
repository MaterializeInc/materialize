# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=envelope-none-bytes

$ kafka-ingest topic=envelope-none-bytes format=bytes repeat=5000
ABC
XYZ

> CREATE MATERIALIZED SOURCE envelope_none_bytes
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-envelope-none-bytes-${testdrive.seed}'
  FORMAT BYTES
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE NONE

> SELECT COUNT(*) FROM envelope_none_bytes
10000

$ kafka-create-topic topic=envelope-none-text

$ kafka-ingest topic=envelope-none-text format=bytes repeat=5000
ABC
XYZ

> CREATE MATERIALIZED SOURCE envelope_none_text
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-envelope-none-text-${testdrive.seed}'
  FORMAT TEXT
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE NONE

> SELECT COUNT(*) FROM envelope_none_text
10000
