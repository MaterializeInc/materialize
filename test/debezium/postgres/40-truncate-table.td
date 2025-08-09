# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that TRUNCATE table is properly replicated. Debezium is supposed
# to issue a dedicated TRUNCATE message on the topic, however instead
# it suffers an internal Java exception and replication stops
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE truncate_table (f1 INTEGER PRIMARY KEY);
INSERT INTO truncate_table VALUES (1),(2),(3);

$ schema-registry-wait topic=postgres.public.truncate_table

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE truncate_table
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.truncate_table');

> CREATE TABLE truncate_table_tbl FROM SOURCE truncate_table (REFERENCE "postgres.public.truncate_table")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

> SELECT * FROM truncate_table_tbl;
1
2
3

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE truncate_table;
INSERT INTO truncate_table VALUES (2),(3),(4);

# TODO: database-issues#8602
# ! SELECT * FROM truncate_table_tbl;
# contains:table was truncated
