# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for rust-rdkafka ignoring per-resource errors in
# DescribeConfigsFuture (and AlterConfigsFuture).
#
# The `materialize_rdkafka_config_bug` user has AlterConfigs but NOT
# DescribeConfigs. With the bug, get_topic_config returns Ok([]) instead of
# Err(TopicAuthorizationFailed), so ensure_topic_config proceeds to alter the
# progress topic — overwriting its cleanup.policy from "delete" to "compact".
#
# After fixing rust-rdkafka, get_topic_config would properly return Err,
# ensure_topic_config would bail out, and the progress topic would keep its
# original cleanup.policy=delete.

$ kafka-create-topic topic=rdkafka-bug-progress partitions=1 compaction=false
$ kafka-create-topic topic=rdkafka-bug-data partitions=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_sink_ensure_topic_config = 'alter'

> CREATE SECRET kafka_password AS 'sekurity'

> CREATE CONNECTION kafka_rdkafka_bug TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize_rdkafka_config_bug',
    SASL PASSWORD = SECRET kafka_password,
    SECURITY PROTOCOL SASL_PLAINTEXT,
    PROGRESS TOPIC = 'testdrive-rdkafka-bug-progress-${testdrive.seed}'
  )

> CREATE TABLE t (a int)
> INSERT INTO t VALUES (1), (2)
> CREATE MATERIALIZED VIEW mv AS SELECT DISTINCT a FROM t
> CREATE SINK reproducer FROM mv
  INTO KAFKA CONNECTION kafka_rdkafka_bug (
    TOPIC 'testdrive-rdkafka-bug-data-${testdrive.seed}',
    TRANSACTIONAL ID PREFIX 'rdkafka-bug',
    PROGRESS GROUP ID PREFIX 'rdkafka-bug'
  )
  KEY (a) FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data format=json key=false sink=materialize.public.reproducer sort-messages=true
{"a": 1}
{"a": 2}

# The progress topic must keep its original cleanup.policy=delete. With the
# rust-rdkafka bug, the DescribeConfigs error is swallowed and the alter
# overwrites it to "compact", causing this assertion to fail.
$ kafka-verify-topic topic=testdrive-rdkafka-bug-progress-${testdrive.seed} topic-config={"cleanup.policy": "delete"}
