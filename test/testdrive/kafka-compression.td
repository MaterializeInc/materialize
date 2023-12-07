# Copyright Materialize, Inc. and contributors. All rights reserved.
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

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE gzip
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-gzip-${testdrive.seed}')
  FORMAT TEXT
> SELECT text FROM gzip
hello
world

$ kafka-create-topic topic=snappy compression=snappy

$ kafka-ingest format=bytes topic=snappy timestamp=1
hello
world

> CREATE SOURCE snappy
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snappy-${testdrive.seed}')
  FORMAT TEXT
> SELECT text FROM snappy
hello
world

$ kafka-create-topic topic=lz4 compression=lz4

$ kafka-ingest format=bytes topic=lz4 timestamp=1
hello
world

> CREATE SOURCE lz4
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-lz4-${testdrive.seed}')
  FORMAT TEXT
> SELECT text FROM lz4
hello
world

$ kafka-create-topic topic=zstd compression=zstd partitions=1

$ kafka-ingest format=bytes topic=zstd timestamp=1
hello
world

> CREATE SOURCE zstd
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-zstd-${testdrive.seed}')
  FORMAT TEXT
> SELECT text FROM zstd
hello
world

> CREATE SOURCE zstd_fast_forwarded
  FROM KAFKA CONNECTION kafka_conn (START OFFSET=[1], TOPIC 'testdrive-zstd-${testdrive.seed}')
  FORMAT TEXT
> SELECT text FROM zstd_fast_forwarded
world

# Test compression with sinks.

> CREATE TABLE feed (a text)
> INSERT INTO feed VALUES ('hello'), ('world')

! CREATE SINK invalid_sink
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'pied-piper')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT
contains:invalid COMPRESSION TYPE: pied-piper

> CREATE SINK none_sink_implicit
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'none')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE SINK none_sink_explicit
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE SINK gzip_sink
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'gzip')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE SINK gzip_sink_spongebob_case
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'gZiP')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE SINK lz4_sink
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'lz4')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE SINK zstd_sink
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'zstd')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

# The Kafka APIs do not make it possible to assess whether the compression
# actually took place, so we settle for just validating that the data is
# readable.
$ kafka-verify-data format=json key=false sink=materialize.public.none_sink_implicit
{"a": "hello"}
{"a": "world"}
{"a": "hello"}
{"a": "world"}
{"a": "hello"}
{"a": "world"}
{"a": "hello"}
{"a": "world"}
{"a": "hello"}
{"a": "world"}
{"a": "hello"}
{"a": "world"}
