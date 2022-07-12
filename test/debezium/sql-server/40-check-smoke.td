# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ schema-registry-wait-schema schema=sql-server.dbo.t1-value

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server
USE test;
INSERT INTO t1 VALUES (345);

$ schema-registry-wait-schema schema=sql-server.transaction-value

> CREATE SOURCE sqlserver_tx_metadata
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.transaction'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE;

# TODO: make sqlserver debezium tx_metadata work.  It doesn't send the END message
> CREATE SOURCE t1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sql-server.dbo.t1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ sql-server-execute name=sql-server
INSERT INTO t1 VALUES (456);

> SELECT * FROM t1;
123
234
345
456
