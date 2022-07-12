# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ schema-registry-wait-schema schema=sql-server.dbo.delete_table_pk-value

> CREATE SOURCE delete_table_pk
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.dbo.delete_table_pk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ schema-registry-wait-schema schema=sql-server.dbo.delete_table_nopk-value

> CREATE SOURCE delete_table_nopk
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.dbo.delete_table_nopk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
DELETE FROM delete_table_pk WHERE key_col = 'key1';
DELETE FROM delete_table_nopk WHERE key_col = 'key2';

> SELECT * FROM delete_table_pk;
key2 value2

> SELECT * FROM delete_table_nopk;
key1 value1
