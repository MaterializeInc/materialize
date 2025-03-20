# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

# Test that the source ingestion pipeline commits offsets back to Kafka with
# the expected group ID.

# Initial setup.

$ kafka-create-topic topic=topic partitions=1

> CREATE CONNECTION conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CLUSTER topic_cluster SIZE '${arg.default-storage-size}';

> CREATE SOURCE topic
  IN CLUSTER topic_cluster
  FROM KAFKA CONNECTION conn (
    TOPIC 'testdrive-topic-${testdrive.seed}'
  )

> CREATE TABLE topic_tbl FROM SOURCE topic (REFERENCE "testdrive-topic-${testdrive.seed}")
  FORMAT BYTES

> SELECT
    ks.topic
  FROM mz_sources s
  JOIN mz_catalog.mz_kafka_sources ks ON s.id = ks.id
  WHERE s.name = 'topic'
testdrive-topic-${testdrive.seed}
