# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ kafka-create-topic topic=topic0 partitions=4

$ kafka-ingest format=avro topic=topic0 schema=${schema} repeat=1
{"f2": 1}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE source0
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic0-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM source0
f2
---
1

# Now recreate the topic with fewer partitions and observe the error

$ kafka-delete-topic-flaky topic=topic0

# Even though `kafka-delete-topic` ensures that the topic no longer exists in
# the broker metadata there is still work to be done asychnronously before it's
# truly gone that must complete before we attempt to recreate it. There is no
# way to observe this work completing so the only option left is sleeping for a
# while. This is the sad state of Kafka. If this test ever becomes flaky let's
# just delete it.
# See: https://github.com/confluentinc/confluent-kafka-python/issues/541
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

$ kafka-create-topic topic=topic0 partitions=2

! SELECT * FROM source0
contains:topic was recreated: partition count regressed from 4 to 2

# We can also detect that a topic got recreated by observing the high watermark regressing

$ kafka-create-topic topic=topic1 partitions=1

$ kafka-ingest format=avro topic=topic1 schema=${schema} repeat=1
{"f2": 1}

> CREATE SOURCE source1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT * FROM source1
f2
---
1

# Now recreate the topic with the same number of partitions and observe the error

$ kafka-delete-topic-flaky topic=topic1

# Even though `kafka-delete-topic` ensures that the topic no longer exists in
# the broker metadata there is still work to be done asychnronously before it's
# truly gone that must complete before we attempt to recreate it. There is no
# way to observe this work completing so the only option left is sleeping for a
# while. This is the sad state of Kafka. If this test ever becomes flaky let's
# just delete it.
# See: https://github.com/confluentinc/confluent-kafka-python/issues/541
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

$ kafka-create-topic topic=topic1 partitions=1

! SELECT * FROM source1
contains:topic was recreated: high watermark of partition 0 regressed from 1 to 0
