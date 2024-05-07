# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

#
# Test that identically-named tables in two source schemas can be
# successfully disambiguated and replicated
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP SCHEMA IF EXISTS schema1;
CREATE SCHEMA schema1;

CREATE TABLE schema1.t1 (f1 INTEGER);
INSERT INTO schema1.t1 VALUES (1);

DROP SCHEMA IF EXISTS schema2;
CREATE SCHEMA schema2;

CREATE TABLE schema2.t1 (f1 INTEGER);
INSERT INTO schema2.t1 VALUES (2);

INSERT INTO schema1.t1 SELECT * FROM schema1.t1;
INSERT INTO schema2.t1 SELECT * FROM schema2.t1;

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;
contains:multiple subsources would be named t1

# TODO: #24797
# ! CREATE SOURCE mz_source
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (t1);
# contains:table t1 is ambiguous, consider specifying the schema

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (schema1.t1);

> SELECT * FROM t1;
1
1

> DROP SOURCE mz_source CASCADE;

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (schema1.t1 AS t1_1, schema2.t1 AS t1_2);

> SELECT * FROM t1_1;
1
1

> SELECT * FROM t1_2;
2
2

$ mysql-execute name=mysql
DROP SCHEMA schema1;
DROP SCHEMA schema2;

> DROP SOURCE mz_source CASCADE;
