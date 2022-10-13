# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that the source ingestion pipeline commits offsets back to Kafka with
# the expected group ID.

# Initial setup.

$ kafka-create-topic topic=topic partitions=1
$ kafka-ingest format=bytes topic=topic
one
two
three

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

# Test that the default consumer group ID is
# `materialize-$ENVIRONMENTID-$CONNECTIONID-$SOURCEID`.

> CREATE SOURCE topic
  FROM KAFKA CONNECTION conn (
    TOPIC 'testdrive-topic-${testdrive.seed}'
  )
  FORMAT BYTES

> SELECT * from topic
one
two
three

$ set-from-sql var=consumer-group-id
SELECT
  'materialize-' || mz_environment_id() || '-' || c.id || '-' || s.id
FROM mz_sources s
JOIN mz_connections c ON c.id = s.connection_id
WHERE s.name = 'topic'

$ kafka-verify-commit consumer-group-id=${consumer-group-id} topic=topic partition=0
3

> DROP SOURCE topic

# Test than an arbitrary prefix can be prepended to the consumer group.

> CREATE SOURCE topic
  FROM KAFKA CONNECTION conn (
    TOPIC 'testdrive-topic-${testdrive.seed}',
    GROUP ID PREFIX 'OVERRIDE-'
  )
  FORMAT BYTES

> SELECT * from topic
one
two
three

$ set-from-sql var=consumer-group-id
SELECT
  'OVERRIDE-materialize-' || mz_environment_id() || '-' || c.id || '-' || s.id
FROM mz_sources s
JOIN mz_connections c ON c.id = s.connection_id
WHERE s.name = 'topic'

$ kafka-verify-commit consumer-group-id=${consumer-group-id} topic=topic partition=0
3
