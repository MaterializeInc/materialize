# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ schema-registry-wait topic=sql-server.test.dbo.delete_table_pk

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE delete_table_pk
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sql-server.test.dbo.delete_table_pk');

> CREATE TABLE delete_table_pk_tbl FROM SOURCE delete_table_pk (REFERENCE "sql-server.test.dbo.delete_table_pk")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

# [btv] uncomment if we bring back classic debezium mode
# $ schema-registry-wait topic=sql-server.dbo.delete_table_nopk

# > CREATE SOURCE delete_table_nopk
#   FROM KAFKA CONNECTION kafka_conn (TOPIC 'sql-server.dbo.delete_table_nopk');

# > CREATE TABLE delete_table_nopk_tbl FROM SOURCE delete_table_nopk (REFERENCE "sql-server.dbo.delete_table_nopk")
#   FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
#   ENVELOPE DEBEZIUM;

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
DELETE FROM delete_table_pk WHERE key_col = 'key1';
# DELETE FROM delete_table_nopk WHERE key_col = 'key2';

> SELECT * FROM delete_table_pk_tbl;
key2 value2

# > SELECT * FROM delete_table_nopk_tbl;
# key1 value1
