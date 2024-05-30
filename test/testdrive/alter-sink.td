# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Verify that envelope types are correctly reported in mz_sinks

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE TABLE pre_alter (pre_name string);
> INSERT INTO pre_alter VALUES ('fish');

> CREATE TABLE post_alter (post_name string, post_value int);
# This value should be ignored by the sink because the alter will happen after
# this record has been inserted and we don't re-emit a snapshot of the new
# collection when it changes.
> INSERT INTO post_alter VALUES ('ignored', 0);

> CREATE SINK sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pre_alter
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM;

> ALTER SINK sink FROM post_alter;

# The sink will start sinking updates from `post_alter` at the timestamp that
# the previous dataflow happens to stop. This happens pretty quickly but we
# wait a few milliseconds more for good measure to avoid flaking.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=200ms

> INSERT INTO post_alter VALUES ('chips', 42);

$ kafka-verify-data format=json sink=materialize.public.sink key=false
{"before": null, "after": {"pre_name": "fish"}}
{"before": null, "after": {"post_name": "chips", "post_value": 42}}
