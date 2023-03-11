# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=status-history

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE TABLE sink_table (f1 INTEGER);

> CREATE SINK kafka_sink FROM sink_table
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ set-from-sql var=sink_id
SELECT id FROM mz_sinks WHERE name = 'kafka_sink'

$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

# Verify we get a starting -- it's possible we move to running by the time this query runs.
> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' ORDER BY occurred_at LIMIT 1;
"<TIMESTAMP> UTC" ${sink_id} starting <null> <null>

> INSERT INTO sink_table VALUES (1);

> INSERT INTO sink_table VALUES (2);

# We should get a stalled error message next
> SELECT status, sink_id, error, jsonb_object_keys(details) FROM mz_internal.mz_sink_status_history
  WHERE sink_id = '${sink_id}' AND status = 'stalled' LIMIT 1;
stalled ${sink_id} "retriable transaction error" hint




