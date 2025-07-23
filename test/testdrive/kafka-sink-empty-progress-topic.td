# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET transaction_isolation = 'strict serializable'

# Test creating a sink when the progress topic is empty but has a non-zero low
# water mark.
#
# Note that, without outside intervention, it's impossible for modern versions
# of Materialize to get a progress topic into this state because:
#
#  * Materialize always creates progress topics with compaction enabled but
#    time-based retention disabled.
#
#  * Materialize never publishes tombstones to the progress topic.
#
# So the progress topic will either have a low water mark of zero, or it will
# have at least one progress message in it.
#
# Still, we've observed at least one progress topic in this state in the wild.
# We believe this occured because the user created the progress topic in an old
# version of Materialize that did not force disable the retention policy when
# creating the progress topic.
#
# Even though modern versions of Materialize don't leave progress topics in this
# state naturally, outside intervention can still cause Materialize to observe
# progress topics in this state:
#
#  * A user could enable a retention policy on the progress topic after it's
#    created. While this is incorrect it won't cause problems in the steady
#    state in practice as long as the retention window is large enough to never
#    delete the latest messages. But if all sinks associated with the progress
#    topic are dropped, and then the progress topic is allowed to sit for a
#    while ... it will eventually empty out and have a nonzero low water mark.
#
#  * A user could manually clear the progress topic out after it's created by
#    sending DeleteRecords requests to the topic. This is technically valid as
#    long as there are no sinks actively associated with the progress topic.
#
# So it's worth testing that we're handling these sorts of progress topics
# correctly. Previous versions of Materialize used to get wedged if you created
# a sink against a progress topic in this state.

$ set-arg-default single-replica-cluster=quickstart

> CREATE MATERIALIZED VIEW mv AS VALUES (1)

# Ensure that sinks can be created against a progress topic that is empty with a
# low water mark of zero.

$ kafka-create-topic topic=empty-at-zero partitions=1

> CREATE CONNECTION kafka_conn_empty_at_zero
  TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT,
    PROGRESS TOPIC 'testdrive-empty-at-zero-${testdrive.seed}'
  )

> CREATE SINK empty_at_zero
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mv
  INTO KAFKA CONNECTION kafka_conn_empty_at_zero (TOPIC 'testdrive-empty-at-zero-${testdrive.seed}')
  KEY (column1) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data sink=materialize.public.empty_at_zero format=json
{"column1": 1}

# Ensure that sinks can be created against a progress topic that is empty with a
# low water mark that is nonzero.

$ kafka-create-topic topic=empty-at-nonzero partitions=1
$ kafka-ingest topic=empty-at-nonzero format=bytes
data
data
data
$ kafka-delete-records topic=empty-at-nonzero partition=0 offset=3

> CREATE CONNECTION kafka_conn_empty_at_nonzero
  TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT,
    PROGRESS TOPIC 'testdrive-empty-at-nonzero-${testdrive.seed}'
  )

> CREATE SINK empty_at_nonzero
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mv
  INTO KAFKA CONNECTION kafka_conn_empty_at_nonzero (TOPIC 'testdrive-empty-at-nonzero-${testdrive.seed}')
  KEY (column1) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data sink=materialize.public.empty_at_nonzero format=json
{"column1": 1}
