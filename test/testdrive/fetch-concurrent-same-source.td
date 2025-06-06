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
# Make sure that FETCH-ing using multiple cursors from the same source works as expected
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET min_timestamp_interval = '10ms'

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=fetch-concurrent-same-source

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE fetch_concurrent_same_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-fetch-concurrent-same-source-${testdrive.seed}')
  WITH (TIMESTAMP INTERVAL '10ms')

> CREATE TABLE fetch_concurrent_same_source_tbl FROM SOURCE fetch_concurrent_same_source (REFERENCE "testdrive-fetch-concurrent-same-source-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=fetch-concurrent-same-source schema=${int} timestamp=1
{"f1": 123}
{"f1": 234}
{"f1": 345}

> SELECT COUNT(*) FROM fetch_concurrent_same_source_tbl;
3

> BEGIN

> DECLARE c1 CURSOR FOR SELECT * FROM fetch_concurrent_same_source_tbl;

> DECLARE c2 CURSOR FOR SELECT * FROM fetch_concurrent_same_source_tbl;

> FETCH ALL c1;
123
234
345

> FETCH ALL c2;
123
234
345

> DECLARE c3 CURSOR FOR SELECT * FROM fetch_concurrent_same_source_tbl;

> FETCH ALL c3;
123
234
345
