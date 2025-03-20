# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1
$ set-arg-default single-replica-cluster=quickstart

# Test support for compressed Kafka topics.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET max_clusters = 20

$ kafka-create-topic topic=gzip compression=gzip

$ kafka-ingest format=bytes topic=gzip timestamp=1
hello
world

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER gzip_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE gzip
  IN CLUSTER gzip_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-gzip-${testdrive.seed}')

> CREATE TABLE gzip_tbl FROM SOURCE gzip (REFERENCE "testdrive-gzip-${testdrive.seed}")
  FORMAT TEXT

> SELECT text FROM gzip_tbl
hello
world

$ kafka-create-topic topic=snappy compression=snappy

$ kafka-ingest format=bytes topic=snappy timestamp=1
hello
world

> CREATE CLUSTER snappy_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE snappy
  IN CLUSTER snappy_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-snappy-${testdrive.seed}')

> CREATE TABLE snappy_tbl FROM SOURCE snappy (REFERENCE "testdrive-snappy-${testdrive.seed}")
  FORMAT TEXT

> SELECT text FROM snappy_tbl
hello
world

$ kafka-create-topic topic=lz4 compression=lz4

$ kafka-ingest format=bytes topic=lz4 timestamp=1
hello
world

> CREATE CLUSTER lz4_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE lz4
  IN CLUSTER lz4_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-lz4-${testdrive.seed}')

> CREATE TABLE lz4_tbl FROM SOURCE lz4 (REFERENCE "testdrive-lz4-${testdrive.seed}")
  FORMAT TEXT

> SELECT text FROM lz4_tbl
hello
world

$ kafka-create-topic topic=zstd compression=zstd partitions=1

$ kafka-ingest format=bytes topic=zstd timestamp=1
hello
world

> CREATE CLUSTER zstd_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE zstd
  IN CLUSTER zstd_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-zstd-${testdrive.seed}')

> CREATE TABLE zstd_tbl FROM SOURCE zstd (REFERENCE "testdrive-zstd-${testdrive.seed}")
  FORMAT TEXT

> SELECT text FROM zstd_tbl
hello
world

> CREATE CLUSTER zstd_fast_forwarded_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE zstd_fast_forwarded
  IN CLUSTER zstd_fast_forwarded_cluster
  FROM KAFKA CONNECTION kafka_conn (START OFFSET=[1], TOPIC 'testdrive-zstd-${testdrive.seed}')

> CREATE TABLE zstd_fast_forwarded_tbl FROM SOURCE zstd_fast_forwarded (REFERENCE "testdrive-zstd-${testdrive.seed}")
  FORMAT TEXT

> SELECT text FROM zstd_fast_forwarded_tbl
world

# Test compression with sinks.

> CREATE TABLE feed (a text)
> INSERT INTO feed VALUES ('hello'), ('world')

! CREATE SINK invalid_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'pied-piper')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT
contains:invalid COMPRESSION TYPE: pied-piper

> CREATE CLUSTER lz4_sink_implicit_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK lz4_sink_implicit
  IN CLUSTER lz4_sink_implicit_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'none')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE CLUSTER none_sink_explicit_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK none_sink_explicit
  IN CLUSTER none_sink_explicit_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE CLUSTER gzip_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK gzip_sink
  IN CLUSTER gzip_sink_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'gzip')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE CLUSTER gzip_sink_spongebob_case_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK gzip_sink_spongebob_case
  IN CLUSTER gzip_sink_spongebob_case_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'gZiP')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE CLUSTER lz4_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK lz4_sink
  IN CLUSTER lz4_sink_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'lz4')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

> CREATE CLUSTER zstd_sink_cluster SIZE '${arg.default-storage-size}';

> CREATE SINK zstd_sink
  IN CLUSTER zstd_sink_cluster
  FROM feed
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-compression', COMPRESSION TYPE 'zstd')
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT

# The Kafka APIs do not make it possible to assess whether the compression
# actually took place, so we settle for just validating that the data is
# readable.
$ kafka-verify-data format=json key=false sink=materialize.public.lz4_sink_implicit
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
