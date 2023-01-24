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

$ kafka-create-topic topic=status-history

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

## The basics: create a source and sink, pass in some data, and confirm that we see the status
## entries we expect.

> CREATE SOURCE kafka_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
  FORMAT TEXT

> CREATE SINK kafka_sink FROM kafka_source
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'kafka_source'

> select * from mz_internal.mz_source_status_history where source_id = '${source_id}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id} starting <null> <null>
"<TIMESTAMP> UTC" ${source_id} running <null> <null>

> select * from mz_internal.mz_source_statuses where id = '${source_id}';
"${source_id}" kafka_source kafka "<TIMESTAMP> UTC" running <null> <null>

$ set-from-sql var=sink_id
SELECT id FROM mz_sinks WHERE name = 'kafka_sink'

# Verify we get a starting -- it's possible we move to running by the time this query runs.
> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' ORDER BY occurred_at LIMIT 1;
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

> select * from mz_internal.mz_sink_statuses where id = '${sink_id}';
"${sink_id}" kafka_sink kafka "<TIMESTAMP> UTC" running <null> <null>

> select * from mz_internal.mz_source_status_history where source_id = '${source_id}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id} starting <null> <null>
"<TIMESTAMP> UTC" ${source_id} running <null> <null>

> select * from mz_internal.mz_source_statuses where id = '${source_id}';
"${source_id}" kafka_source kafka "<TIMESTAMP> UTC" running <null> <null>

## Confirm that the tables report statuses for multiple sources and sinks.

> CREATE SOURCE kafka_source_2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
  FORMAT TEXT

> CREATE SINK kafka_sink_2 FROM kafka_source_2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-2-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ set-from-sql var=source_id_2
SELECT id FROM mz_sources WHERE name = 'kafka_source_2'

$ set-from-sql var=sink_id_2
SELECT id FROM mz_sinks WHERE name = 'kafka_sink_2'

> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id_2}' ORDER BY occurred_at;
"<TIMESTAMP> UTC" ${sink_id_2} starting <null> <null>
"<TIMESTAMP> UTC" ${sink_id_2} running <null> <null>

> select * from mz_internal.mz_source_status_history where source_id = '${source_id_2}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id_2} starting <null> <null>
"<TIMESTAMP> UTC" ${source_id_2} running <null> <null>

> select * from mz_internal.mz_sink_statuses where id in ('${sink_id}', '${sink_id_2}') order by id;
"${sink_id}" kafka_sink kafka "<TIMESTAMP> UTC" running <null> <null>
"${sink_id_2}" kafka_sink_2 kafka "<TIMESTAMP> UTC" running <null> <null>

> select * from mz_internal.mz_source_statuses where id in ('${source_id}', '${source_id_2}') order by id;
"${source_id}" kafka_source kafka "<TIMESTAMP> UTC" running <null> <null>
"${source_id_2}" kafka_source_2 kafka "<TIMESTAMP> UTC" running <null> <null>


# ensure `dropped` also shows up
> DROP SINK kafka_sink

> SELECT * FROM mz_internal.mz_sink_status_history WHERE sink_id = '${sink_id}' ORDER BY occurred_at;
"<TIMESTAMP> UTC" ${sink_id} starting <null> <null>
"<TIMESTAMP> UTC" ${sink_id} running <null> <null>
"<TIMESTAMP> UTC" ${sink_id} dropped <null> <null>

> DROP SOURCE kafka_source

> select * from mz_internal.mz_source_status_history where source_id = '${source_id}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id} starting <null> <null>
"<TIMESTAMP> UTC" ${source_id} running <null> <null>
"<TIMESTAMP> UTC" ${source_id} dropped <null> <null>
