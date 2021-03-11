# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test support for compressed Kafka topics.

$ kafka-create-topic topic=gzip compression=gzip

$ kafka-ingest format=bytes topic=gzip timestamp=1
hello
world

> CREATE MATERIALIZED SOURCE gzip
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-gzip-${testdrive.seed}'
  FORMAT TEXT
> SELECT text FROM gzip
hello
world

$ kafka-create-topic topic=snappy compression=snappy

$ kafka-ingest format=bytes topic=snappy timestamp=1
hello
world

> CREATE MATERIALIZED SOURCE snappy
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-snappy-${testdrive.seed}'
  FORMAT TEXT
> SELECT text FROM snappy
hello
world

$ kafka-create-topic topic=lz4 compression=lz4

$ kafka-ingest format=bytes topic=lz4 timestamp=1
hello
world

> CREATE MATERIALIZED SOURCE lz4
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-lz4-${testdrive.seed}'
  FORMAT TEXT
> SELECT text FROM lz4
hello
world

$ kafka-create-topic topic=zstd compression=zstd

$ kafka-ingest format=bytes topic=zstd timestamp=1
hello
world

> CREATE MATERIALIZED SOURCE zstd
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-zstd-${testdrive.seed}'
  FORMAT TEXT
> SELECT text FROM zstd
hello
world

> CREATE MATERIALIZED SOURCE zstd_fast_forwarded
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-zstd-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT TEXT
> SELECT text FROM zstd_fast_forwarded
world
