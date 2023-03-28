# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a Kafka sink using SSH.

# A tunnel-less kafka source to build the sink.
> CREATE CONNECTION kafka_conn_no_tunnel
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE mz_source_no_tunnel
  FROM KAFKA CONNECTION kafka_conn_no_tunnel (TOPIC 'testdrive-thetopic-${testdrive.seed}')
  FORMAT TEXT
  ENVELOPE NONE

# Test at least one reasonable setup for a sink with an ssh tunnel.
> CREATE SINK mz_sink FROM mz_source_no_tunnel
  INTO KAFKA CONNECTION kafka_conn_dynamic (TOPIC 'unnamed-cols-sink-${testdrive.seed}')
  KEY (text)
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.mz_sink key=false sort-messages=true
{"before": null, "after": {"text": "one"}}
{"before": null, "after": {"text": "two"}}
