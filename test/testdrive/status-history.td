# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Specify the behaviour of the status history tables
$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

# History starts out empty when there are no sources
> select * from mz_internal.mz_source_status_history

$ kafka-create-topic topic=status-history

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE kafka_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
  FORMAT TEXT

> CREATE SINK kafka_sink FROM kafka_source
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ set-from-sql var=sink_id
SELECT id FROM mz_sinks WHERE name = 'kafka_sink'

# Verify we get a starting -- it's possible we move to running by the time this query runs.
> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' AND status = 'starting' ORDER BY occurred_at;
"<TIMESTAMP> UTC" ${sink_id} starting <null> <null>

$ kafka-ingest format=bytes topic=status-history
a
b
c
d


> SELECT * FROM kafka_source ORDER BY 1;
a
b
c
d

$ kafka-verify-data format=avro sink=materialize.public.kafka_sink sort-messages=true
{"before": null, "after": {"row":{"text": "a"}}}
{"before": null, "after": {"row":{"text": "b"}}}
{"before": null, "after": {"row":{"text": "c"}}}
{"before": null, "after": {"row":{"text": "d"}}}

> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' ORDER BY occurred_at;
"<TIMESTAMP> UTC" ${sink_id} starting <null> <null>
"<TIMESTAMP> UTC" ${sink_id} running <null> <null>
