# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the temporal data types
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

# Insert data pre-snapshot
$ sql-server-execute name=sql-server
CREATE TABLE t1 (date_col DATE, time_col TIME, timestamp_col DATETIME2);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO t1 VALUES (CAST('2011-11-11' AS DATE), CAST('11:11:11.123456' AS TIME), CAST('2011-11-11 11:11:11.123456' AS DATETIME2));

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(date_col), pg_typeof(time_col), pg_typeof(timestamp_col) FROM t1 LIMIT 1;
date time "timestamp without time zone"

> SELECT * FROM t1;
2011-11-11 11:11:11.123456 "2011-11-11 11:11:11.123456"
2011-11-11 11:11:11.123456 "2011-11-11 11:11:11.123456"