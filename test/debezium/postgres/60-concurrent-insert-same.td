# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Concurrent inserts into the same table
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE insert_same_table (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE insert_same_table REPLICA IDENTITY FULL;
INSERT INTO insert_same_table VALUES (0);

$ schema-registry-wait-schema schema=postgres.public.insert_same_table-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE insert_same_table
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.insert_same_table'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO insert_same_table VALUES (1);

$ postgres-execute connection=conn2
BEGIN;
INSERT INTO insert_same_table VALUES (2);

$ postgres-execute connection=conn1
INSERT INTO insert_same_table VALUES (3);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO insert_same_table VALUES (4);
COMMIT;

> SELECT * FROM insert_same_table;
0
1
2
3
4
