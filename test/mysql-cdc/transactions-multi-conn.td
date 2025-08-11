# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test that transactions work properly
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql1 url=mysql://root@mysql password=${arg.mysql-root-password}
$ mysql-connect name=mysql2 url=mysql://root@mysql password=${arg.mysql-root-password}
$ mysql-connect name=mysql3 url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql1
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);
CREATE TABLE t3 (a INT);

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE public.t2);
> CREATE TABLE t3 FROM SOURCE mz_source (REFERENCE public.t3);
> COMMIT

> SELECT count(*) FROM t1;
0

> SELECT count(*) FROM t2;
0

> SELECT count(*) FROM t3;
0

$ mysql-execute name=mysql1
SET AUTOCOMMIT = FALSE;

$ mysql-execute name=mysql2
SET AUTOCOMMIT = FALSE;
USE public;

$ mysql-execute name=mysql3
SET AUTOCOMMIT = FALSE;
USE public;

$ mysql-execute name=mysql1
INSERT INTO t1 VALUES (1000);
INSERT INTO t2 VALUES (1000);
INSERT INTO t3 VALUES (1000);

$ mysql-execute name=mysql2
INSERT INTO t1 VALUES (2000);
INSERT INTO t2 VALUES (2000);
INSERT INTO t3 VALUES (2000);

$ mysql-execute name=mysql3
INSERT INTO t1 VALUES (3000);
INSERT INTO t2 VALUES (3000);
INSERT INTO t3 VALUES (3000);

$ mysql-execute name=mysql1
COMMIT;

$ mysql-execute name=mysql3
COMMIT;

> SELECT * FROM t1;
1000
3000

> SELECT * FROM t2;
1000
3000

> SELECT * FROM t3;
1000
3000

$ mysql-execute name=mysql2
COMMIT;

> SELECT * FROM t1;
1000
2000
3000

# delete and insert statements cannot be done in multiple transactions on the same table even with fine-grained where condition

$ mysql-execute name=mysql1
INSERT INTO t1 VALUES (1001);
INSERT INTO t2 VALUES (1001);
INSERT INTO t3 VALUES (1001);

$ mysql-execute name=mysql2
INSERT INTO t1 VALUES (2001);
INSERT INTO t2 VALUES (2001);
INSERT INTO t3 VALUES (2001);

$ mysql-execute name=mysql3
INSERT INTO t1 VALUES (3001);
INSERT INTO t2 VALUES (3001);
INSERT INTO t3 VALUES (3001);

$ mysql-execute name=mysql1
COMMIT;

$ mysql-execute name=mysql3
COMMIT;

> SELECT * FROM t1;
1000
1001
2000
3000
3001

> SELECT * FROM t2;
1000
1001
2000
3000
3001

> SELECT * FROM t3;
1000
1001
2000
3000
3001

$ mysql-execute name=mysql2
COMMIT;
INSERT INTO t1 VALUES (2002);

$ mysql-execute name=mysql1
INSERT INTO t1 VALUES (1002);
DELETE FROM t2 WHERE a = 2000;

$ mysql-execute name=mysql2
DELETE FROM t3 WHERE a = 2000;

$ mysql-execute name=mysql1
COMMIT;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2000
2001
3000
3001

$ mysql-execute name=mysql2
COMMIT;

> SELECT * FROM t1;
1000
1001
1002
2000
2001
2002
3000
3001

> SELECT * FROM t2;
1000
1001
2001
3000
3001

> SELECT * FROM t3;
1000
1001
2001
3000
3001
