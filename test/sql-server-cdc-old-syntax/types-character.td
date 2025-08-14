# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the various character data types
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

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
CREATE TABLE t1 (f1 VARCHAR(10), f2 CHAR(10), f3 VARCHAR(128), f4 VARCHAR(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 VALUES ('abc', 'abc', 'abc', 'abc');
INSERT INTO t1 VALUES ('abc ', 'abc ', 'abc ', 'abc ');

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ sql-server-execute name=sql-server
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1), pg_typeof(f2), pg_typeof(f3), pg_typeof(f4) FROM t1 LIMIT 1;
"character varying" "character" "character varying" "character varying"

> SELECT * FROM t1;
"abc" "abc       " "abc" "abc"
"abc" "abc       " "abc" "abc"
"abc " "abc       " "abc " "abc "
"abc " "abc       " "abc " "abc "

# NB(ptravers): String comparison in SQL Server pads trailing whitespace for comparison
# operators. The `LIKE` operator doesn't pad when performing comparisons. Padded 'abc '
# must be run before unpadded 'abc' otherwise unpadded will match both.
$ sql-server-execute name=sql-server
UPDATE t1 SET f1 = 'xyz ', f2 = 'xyz ', f3 = 'xyz ', f4 = 'xyz ' WHERE f1 LIKE 'abc ';
UPDATE t1 SET f1 = 'klm', f2 = 'klm', f3 = 'klm', f4 = 'klm' WHERE f1 LIKE 'abc';

> SELECT * FROM t1;
"klm" "klm       " "klm" "klm"
"klm" "klm       " "klm" "klm"
"xyz " "xyz       " "xyz " "xyz "
"xyz " "xyz       " "xyz " "xyz "