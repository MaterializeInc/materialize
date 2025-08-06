# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ schema-registry-wait topic=sql-server.test.dbo.types_table

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE types_table
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'sql-server.test.dbo.types_table');

> CREATE TABLE types_table_tbl FROM SOURCE types_table (REFERENCE "sql-server.test.dbo.types_table")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;
> COMMIT

#
# DATETIMEOFFSET is replicated as TEXT, see https://github.com/MaterializeInc/database-issues/issues/2453
# DATETIME2 is replicated as BIGINT, see https://github.com/MaterializeInc/database-issues/issues/2458
# SMALLDATETIME does not store seconds or fractional seconds

> SELECT * FROM types_table_tbl
123.45 "2025-12-10T12:32:10+01:00" "1289567655123456700" "1900-01-01 11:12:00" true 0


> SELECT pg_typeof(decimal_col), pg_typeof(datetimeoffset_col), pg_typeof(datetime2_col), pg_typeof(smalldatetime_col), pg_typeof(bit_col) FROM types_table_tbl;
numeric text bigint "timestamp without time zone" boolean
