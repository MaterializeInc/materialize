# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_transaction_timeout = '60s'

# Test creating a Kafka sink using ssh.

$ kafka-create-topic topic=thetopic

$ kafka-ingest topic=thetopic format=bytes
one

> DROP CLUSTER IF EXISTS sc;
> CREATE CLUSTER sc REPLICAS (r1 (SIZE 'scale=1,workers=32'))


# Unfortunately, currently we need a source we can add stuff to (so kafka), but one that
# isn't also broken by ssh (so we can ensure we are testing sinks specifically). This is because
# sinks sometimes require new values to actually fall over. In the future, when sinks
# dynamically check the connection status, we can use a simple `SELECT 1` MV here.
> CREATE CONNECTION kafka_conn_non_ssh
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE non_ssh IN CLUSTER sc
  FROM KAFKA CONNECTION kafka_conn_non_ssh (TOPIC 'testdrive-thetopic-${testdrive.seed}')

> CREATE TABLE non_ssh_tbl FROM SOURCE non_ssh (REFERENCE "testdrive-thetopic-${testdrive.seed}")
  FORMAT TEXT
  ENVELOPE NONE

> COMMIT

> CREATE CONNECTION kafka_conn_using
  TO KAFKA (BROKER '${testdrive.kafka-addr}' USING SSH TUNNEL thancred, SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION kafka_conn_dynamic
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT, SSH TUNNEL thancred);

> CREATE SINK sink_fixed
  IN CLUSTER sc
  FROM non_ssh_tbl
  INTO KAFKA CONNECTION kafka_conn_using (TOPIC 'sink_fixed-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

> CREATE SINK sink_dynamic
  IN CLUSTER sc
  FROM non_ssh_tbl
  INTO KAFKA CONNECTION kafka_conn_dynamic (TOPIC 'sink_dynamic-${testdrive.seed}')
  FORMAT JSON ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.sink_fixed key=false sort-messages=true
{"before": null, "after": {"text": "one"}}

$ kafka-verify-data format=json sink=materialize.public.sink_dynamic key=false sort-messages=true
{"before": null, "after": {"text": "one"}}

# ensure they all were marked as running correctly
> SELECT status FROM mz_internal.mz_sink_statuses st
  JOIN mz_sinks s ON st.id = s.id
  WHERE s.name in ('sink_fixed', 'sink_dynamic')
running
running

> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink_fixed')
  GROUP BY s.name
sink_fixed 1 1 true true
