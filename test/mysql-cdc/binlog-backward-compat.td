# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that MySQL replication works correctly when binlog_row_metadata is
# set to MINIMAL (the MySQL default prior to 8.0). In MINIMAL mode the binlog
# does not include column names, so Materialize must fall back to matching
# columns by position rather than by name.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = MINIMAL;
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE foo (name VARCHAR(16), value VARCHAR(32));
INSERT INTO foo VALUES ('a', 'apple'), ('b', 'banana');

> CREATE SOURCE mysql_src FROM MYSQL CONNECTION mysql_conn FOR TABLES (foo);

> SELECT * FROM foo;
a apple
b banana

$ mysql-execute name=mysql
INSERT INTO foo VALUES ('c', 'cherry');

> SELECT * FROM foo;
a apple
b banana
c cherry

$ mysql-execute name=mysql
UPDATE foo SET value = 'avocado' WHERE name = 'a';

> SELECT * FROM foo;
a avocado
b banana
c cherry

$ mysql-execute name=mysql
DELETE FROM foo WHERE name = 'b';

> SELECT * FROM foo;
a avocado
c cherry

> DROP SOURCE mysql_src CASCADE;

$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = FULL;
USE public;
CREATE TABLE bar (a INT, b INT, c INT);
INSERT INTO bar VALUES (1, 2, 3), (4, 5, 6);

> CREATE SOURCE mysql_src2 FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE bar FROM SOURCE mysql_src2 (REFERENCE public.bar) WITH (EXCLUDE COLUMNS (b));

> SELECT * FROM bar;
1 3
4 6

$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = MINIMAL;
INSERT INTO bar VALUES (7, 8, 9);

> SELECT * FROM bar;
1 3
4 6
7 9

$ mysql-execute name=mysql
UPDATE bar SET c = 30 WHERE a = 1;

> SELECT * FROM bar;
1 30
4 6
7 9

$ mysql-execute name=mysql
DELETE FROM bar WHERE a = 4;

> SELECT * FROM bar;
1 30
7 9

> DROP SOURCE mysql_src2 CASCADE;


# Restore to a clean state so other tests are unaffected.
$ mysql-execute name=mysql
SET GLOBAL binlog_row_metadata = FULL;
