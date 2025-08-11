# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Concurrent inserts into multiple tables. Each transaction operates on
# two separate tables.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE insert_multi_table1 (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE insert_multi_table1 REPLICA IDENTITY FULL;
INSERT INTO insert_multi_table1 VALUES (0);

CREATE TABLE insert_multi_table2 (f1 INTEGER, PRIMARY KEY (f1));
ALTER TABLE insert_multi_table2 REPLICA IDENTITY FULL;
INSERT INTO insert_multi_table2 VALUES (0);

$ schema-registry-wait topic=postgres.public.insert_multi_table1

$ schema-registry-wait topic=postgres.public.insert_multi_table2

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE insert_multi_table1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.insert_multi_table1');

> CREATE TABLE insert_multi_table1_tbl FROM SOURCE insert_multi_table1 (REFERENCE "postgres.public.insert_multi_table1")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

> BEGIN
> CREATE SOURCE insert_multi_table2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.insert_multi_table2');

> CREATE TABLE insert_multi_table2_tbl FROM SOURCE insert_multi_table2 (REFERENCE "postgres.public.insert_multi_table2")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

$ postgres-connect name=conn1 url=postgres://postgres:postgres@postgres

$ postgres-connect name=conn2 url=postgres://postgres:postgres@postgres

$ postgres-execute connection=conn1
BEGIN;
INSERT INTO insert_multi_table1 VALUES (1);

$ postgres-execute connection=conn2
BEGIN;
INSERT INTO insert_multi_table2 VALUES (2);

$ postgres-execute connection=conn1
INSERT INTO insert_multi_table1 VALUES (3);
COMMIT;

$ postgres-execute connection=conn2
INSERT INTO insert_multi_table2 VALUES (4);
COMMIT;

> SELECT * FROM insert_multi_table1_tbl;
0
1
3

> SELECT * FROM insert_multi_table2_tbl;
0
2
4
