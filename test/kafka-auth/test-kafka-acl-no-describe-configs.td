# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that sinks can be created even in the absence of the `DescribeConfigs`
# cluster permission.

# ==> Set up. <==

> SET transaction_isolation = 'strict serializable'

> CREATE SECRET kafka_password AS 'sekurity'

> CREATE CONNECTION kafka TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize_no_describe_configs',
    SASL PASSWORD = SECRET kafka_password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  );

> CREATE TABLE t (column1 integer)
> INSERT INTO t VALUES (1), (2)
> CREATE MATERIALIZED VIEW mv AS SELECT DISTINCT column1 FROM t

# ==> Test. <==

# Creating the sink when the topic does not yet exist should work, because
# Materialize will gracefully continue even if it cannot discover the default
# number of partitions and replication factor to use.
> CREATE SINK nonexisting FROM mv
  INTO KAFKA CONNECTION kafka (TOPIC 'testdrive-nonexisting-${testdrive.seed}')
  KEY (column1) FORMAT JSON ENVELOPE UPSERT

# Validate that the sink is actually emitting data. Success of the `CREATE SINK`
# command itself is not sufficient validation.
$ kafka-verify-data format=json key=false sink=materialize.public.nonexisting sort-messages=true
{"column1": 1}
{"column1": 2}

# Ensure that the sink never entered the `stalled` status. Previously we had a
# glitch where the sink would stall once before becoming healthy due to a glitch
# in the topic creation code path when access to `DescribeConfigs` is banned.
> SELECT DISTINCT status FROM mz_sinks
  JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
  WHERE mz_sinks.name = 'nonexisting'
starting
running

# Creating the sink after creating the topic outside of Materialize should
# succeed, because Materialize no longer needs to run DescribeConfigs to
# determine the replication factor/number of partitions to use.
$ kafka-create-topic topic=no-describe-configs

> CREATE SINK preexisting FROM mv
  INTO KAFKA CONNECTION kafka (TOPIC 'testdrive-preexisting-${testdrive.seed}')
  KEY (column1) FORMAT JSON ENVELOPE UPSERT

# Validate that the sink is actually emitting data. Success of the `CREATE SINK`
# command itself is not sufficient validation.
$ kafka-verify-data format=json key=false sink=materialize.public.preexisting sort-messages=true
{"column1": 1}
{"column1": 2}

# Ensure that the sink never entered the `stalled` status. Previously we had a
# glitch where the sink would stall once before becoming healthy due to a glitch
# in the topic creation code path when access to `DescribeConfigs` is banned.
> SELECT DISTINCT status FROM mz_sinks
  JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
  WHERE mz_sinks.name = 'preexisting'
starting
running
