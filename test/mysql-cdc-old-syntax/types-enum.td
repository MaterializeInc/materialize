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


#
# ENUM supported only as TEXT
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE enum_type (f1 ENUM ('val1', 'val2'), f2 TEXT);
INSERT INTO enum_type VALUES ('val1', 'val1'), ('val2', 'val2');

# TODO: database-issues#7719 (enum unsupported)
! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;
contains:referenced tables use unsupported types

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn (
    TEXT COLUMNS (public.enum_type.f1)
  )
  FOR ALL TABLES;

> SELECT * FROM enum_type;
val1 val1
val2 val2

# Add an additional enum value type
$ mysql-execute name=mysql
ALTER TABLE enum_type CHANGE f1 f1 ENUM ('val1', 'val2', 'val3');
INSERT INTO enum_type VALUES ('val1', 'val1');

> SELECT * FROM enum_type;
val1 val1
val2 val2
val1 val1

$ mysql-execute name=mysql
INSERT INTO enum_type VALUES ('val3', 'val3');

! SELECT * FROM enum_type;
contains:received invalid enum value: 3 for column f1

$ mysql-execute name=mysql
DELETE FROM enum_type WHERE f1 = 'val3';

> SELECT * FROM enum_type;
val1 val1
val2 val2
val1 val1

# Add an additional enum value type and change the ordering
$ mysql-execute name=mysql
ALTER TABLE enum_type CHANGE f1 f1 ENUM ('val2', 'val1', 'val3', 'val4');
INSERT INTO enum_type VALUES ('val1', 'val1');

# TODO: database-issues#7776 (expected incompatible schema change)
$ skip-if
SELECT true

! SELECT * FROM enum_type;
contains:incompatible schema change: column f1 in table enum_type has been altered
