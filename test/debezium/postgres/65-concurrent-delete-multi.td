# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Concurrent deletes over different tables

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE concurrent_delete1 (f1 INTEGER);
ALTER TABLE concurrent_delete1 REPLICA IDENTITY FULL;
CREATE TABLE concurrent_delete2 (f1 INTEGER);
ALTER TABLE concurrent_delete2 REPLICA IDENTITY FULL;

INSERT INTO concurrent_delete1 VALUES (1);
INSERT INTO concurrent_delete1 VALUES (2);
INSERT INTO concurrent_delete1 VALUES (3);
INSERT INTO concurrent_delete1 VALUES (4);
INSERT INTO concurrent_delete1 VALUES (5);

INSERT INTO concurrent_delete2 VALUES (1);
INSERT INTO concurrent_delete2 VALUES (2);
INSERT INTO concurrent_delete2 VALUES (3);
INSERT INTO concurrent_delete2 VALUES (4);
INSERT INTO concurrent_delete2 VALUES (5);

$ schema-registry-wait-schema schema=postgres.public.concurrent_delete1-value

$ schema-registry-wait-schema schema=postgres.public.concurrent_delete2-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE concurrent_delete1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.concurrent_delete1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> CREATE SOURCE concurrent_delete2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.concurrent_delete2'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
DELETE FROM concurrent_delete1 WHERE f1 = 2;

$ postgres-execute connection=conn2
BEGIN;
DELETE FROM concurrent_delete2 WHERE f1 = 3;

$ postgres-execute connection=conn1
DELETE FROM concurrent_delete2 WHERE f1 = 4;
COMMIT;

$ postgres-execute connection=conn2
DELETE FROM concurrent_delete1 WHERE f1 = 5;
COMMIT;

> SELECT * FROM concurrent_delete1;
1
3
4

> SELECT * FROM concurrent_delete2;
1
2
5
