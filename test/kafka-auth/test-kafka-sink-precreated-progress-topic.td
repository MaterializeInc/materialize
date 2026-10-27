# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: Materialize should not require the Create ACL on the
# progress topic when it has been pre-created by an admin.
#
# The `materialize_no_create_progress` user has Read, Write, Describe, and
# DescribeConfigs on the progress and data topics, but NOT Create. With the
# bug, Materialize would unconditionally call CreateTopics for the progress
# topic on every sink startup, causing a permanent stall:
#
#   kafka: error registering kafka progress topic for sink: Error creating
#   topic <topic> for sink: Admin operation error: TopicAuthorizationFailed
#
# With the fix, Materialize checks topic existence via a Metadata request
# first and skips CreateTopics when the topic is already present.

# ==> Set up. <==

# Pre-create both topics as the testdrive admin (superuser) before handing
# control to the restricted `materialize_no_create_progress` user.
$ kafka-create-topic topic=no-create-progress compaction=true
$ kafka-create-topic topic=no-create-data partitions=1

> CREATE SECRET kafka_password AS 'sekurity'

> CREATE CONNECTION kafka_conn TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize_no_create_progress',
    SASL PASSWORD = SECRET kafka_password,
    SECURITY PROTOCOL SASL_PLAINTEXT,
    PROGRESS TOPIC = 'testdrive-no-create-progress-${testdrive.seed}'
  )

> CREATE TABLE t (a int)
> INSERT INTO t VALUES (1), (2)
> CREATE MATERIALIZED VIEW mv AS SELECT DISTINCT a FROM t

# ==> Test. <==

> CREATE CLUSTER c (SIZE = 'scale=1,workers=1')

# The sink must reach running status without the Create ACL on the progress
# topic. Before the fix, this would stall indefinitely.
> CREATE SINK working IN CLUSTER c FROM mv
  INTO KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-no-create-data-${testdrive.seed}',
    TRANSACTIONAL ID PREFIX 'no-create',
    PROGRESS GROUP ID PREFIX 'no-create'
  )
  KEY (a) FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data format=json key=false sink=materialize.public.working sort-messages=true
{"a": 1}
{"a": 2}

# Verify the sink never entered the stalled status.
> SELECT DISTINCT status FROM mz_sinks
  JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
  WHERE mz_sinks.name = 'working'
starting
running
