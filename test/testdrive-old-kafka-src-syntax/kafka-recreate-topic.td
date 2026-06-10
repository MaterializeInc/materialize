# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE CLUSTER to_recreate SIZE 'scale=1,workers=1'

# Test detection of topic deletion.

$ kafka-create-topic topic=topic0 partitions=4

> CREATE SOURCE source0 IN CLUSTER to_recreate
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic0-${testdrive.seed}')
  FORMAT TEXT ENVELOPE NONE

> SELECT * FROM source0

$ kafka-delete-topic-flaky topic=topic0

! SELECT * FROM source0
contains:topic was deleted

# Test detection of topic recreation.
#
# The Kafka source detects topic recreation based on regression of the upstream
# frontier. For the upstream frontier to regress, the new topic must have:
#  (1) fewer partitions than the old topic, or
#  (2) a lower watermark for at least one of its partitions.
# We test both cases below.

# (1) topic recreation with fewer partitions.

$ kafka-create-topic topic=topic1 partitions=4

> CREATE SOURCE source1 IN CLUSTER to_recreate
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')
  FORMAT TEXT ENVELOPE NONE

> SELECT * FROM source1

# Spin down the cluster, to prevent the source from observing the topic
# deletion before the new topic was created.
> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 0)

# Recreate the topic with fewer partitions.

$ kafka-delete-topic-flaky topic=topic1

# Even though `kafka-delete-topic` ensures that the topic no longer exists in
# the broker metadata there is still work to be done asynchronously before it's
# truly gone that must complete before we attempt to recreate it. There is no
# way to observe this work completing so the only option left is sleeping for a
# while. This is the sad state of Kafka. If this test ever becomes flaky let's
# just delete it.
# See: https://github.com/confluentinc/confluent-kafka-python/issues/541
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

$ kafka-create-topic topic=topic1 partitions=2

> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 1)

! SELECT * FROM source1
contains:topic was recreated

# (2) topic recreation with a lower watermark.

$ kafka-create-topic topic=topic2 partitions=4

$ kafka-ingest format=bytes topic=topic2
1

> CREATE SOURCE source2 IN CLUSTER to_recreate
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic2-${testdrive.seed}')
  FORMAT TEXT ENVELOPE NONE

> SELECT * FROM source2
1

# Spin down the cluster, to prevent the source from observing the topic
# deletion before the new topic was created.
> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 0)

# Recreate the topic with the same number of partitions but a lower watermark.

$ kafka-delete-topic-flaky topic=topic2

# Even though `kafka-delete-topic` ensures that the topic no longer exists in
# the broker metadata there is still work to be done asynchronously before it's
# truly gone that must complete before we attempt to recreate it. There is no
# way to observe this work completing so the only option left is sleeping for a
# while. This is the sad state of Kafka. If this test ever becomes flaky let's
# just delete it.
# See: https://github.com/confluentinc/confluent-kafka-python/issues/541
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

$ kafka-create-topic topic=topic2 partitions=4

> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 1)

! SELECT * FROM source2
contains:topic was recreated

# Ensure we don't panic after we restart due to the above finished ingestions.
$ kafka-create-topic topic=good-topic

> CREATE SOURCE good_source
  IN CLUSTER to_recreate
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-good-topic-${testdrive.seed}')
  FORMAT TEXT
  ENVELOPE NONE

> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 0)
> ALTER CLUSTER to_recreate SET (REPLICATION FACTOR 1)

$ kafka-ingest format=bytes topic=good-topic repeat=1
1

> SELECT * FROM good_source
text
----
1

# TODO: why are these paused and not stalled with errors?
> SELECT name, status, error FROM mz_internal.mz_source_statuses WHERE type != 'progress'
name            status    error
-------------------------------
good_source     running   <null>
source0         paused    <null>
source1         paused    <null>
source2         paused    <null>

# Testdrive expects all sources to end in a healthy state, so manufacture that
# by dropping sources.
> DROP CLUSTER to_recreate CASCADE;
