# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a MySQL source using SSH


> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass,
    SSH TUNNEL thancred
  );


$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS dummyschema;
CREATE DATABASE dummyschema;
USE dummyschema;
CREATE TABLE dummy (f1 INTEGER);
INSERT INTO dummy VALUES (1), (2);
COMMIT;

> BEGIN
> CREATE SOURCE mysql_source FROM MYSQL CONNECTION mysql;

> CREATE TABLE dummy FROM SOURCE mysql_source (REFERENCE dummyschema.dummy);
> COMMIT

> SELECT COUNT(*) FROM dummy;
2

$ mysql-execute name=mysql
INSERT INTO dummy VALUES (3);

> SELECT * FROM dummy;
1
2
3
