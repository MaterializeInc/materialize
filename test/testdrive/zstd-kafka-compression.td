# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for zstd compressed Kafka topics.

$ kafka-create-topic topic=zstd compression=zstd partitions=1

$ kafka-ingest format=bytes topic=zstd timestamp=1
hello
world

> CREATE SOURCE zstd
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-zstd-${testdrive.seed}'
  FORMAT TEXT
> SELECT text FROM zstd
hello
world

> CREATE SOURCE zstd_fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-zstd-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT TEXT
> SELECT text FROM zstd_fast_forwarded
world
