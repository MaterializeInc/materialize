# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A sink through the tunnel, to check that it recovers after the broker
# restarts.

> CREATE TABLE restart_sink_input (text TEXT)
> INSERT INTO restart_sink_input VALUES ('one')

> CREATE SINK restart_sink
  IN CLUSTER sc
  FROM restart_sink_input
  INTO KAFKA CONNECTION kafka_conn_dynamic (TOPIC 'restart_sink-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.restart_sink sort-messages=true
{"before": null, "after": {"text": "one"}}

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'restart_sink'
running
