# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

#
# Make sure that data that was ingested during the lifetime of a SUBSCRIBE cursor can be FETCH-ed
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET min_timestamp_interval = '100ms'

$set-regex match=\d{13} replacement=<TIMESTAMP>

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=tail-fetch-during-ingest

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE fetch_during_ingest
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-tail-fetch-during-ingest-${testdrive.seed}')
  WITH (TIMESTAMP INTERVAL '100ms')

> CREATE TABLE fetch_during_ingest_tbl FROM SOURCE fetch_during_ingest (REFERENCE "testdrive-tail-fetch-during-ingest-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE fetch_during_ingest_tbl;

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=1
{"f1": 123}

> FETCH 1 c WITH (timeout='60s');
<TIMESTAMP> 1 123

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=2
{"f1": 234}

# The row just inserted is ours to fetch
> FETCH 1 c WITH (timeout='60s');
<TIMESTAMP> 1 234
