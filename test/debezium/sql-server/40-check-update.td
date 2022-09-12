# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ schema-registry-wait-schema schema=sql-server.dbo.update_table_pk-value

> CREATE SOURCE update_table_pk
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.dbo.update_table_pk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ schema-registry-wait-schema schema=sql-server.dbo.update_table_nopk-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE update_table_nopk
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.dbo.update_table_nopk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
UPDATE update_table_pk SET value_col = 'value_new' WHERE key_col = 'key1';
UPDATE update_table_pk SET key_col = 'key_new' WHERE key_col = 'key2';

UPDATE update_table_nopk SET value_col = 'value_new' WHERE key_col = 'key1';
UPDATE update_table_nopk SET key_col = 'key_new' WHERE key_col = 'key2';

> SELECT * FROM update_table_pk;
key1 value_new
key_new value2

> SELECT * FROM update_table_nopk;
key1 value_new
key_new value2
