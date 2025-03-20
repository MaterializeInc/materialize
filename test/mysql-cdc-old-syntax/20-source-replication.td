# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


> CREATE CONNECTION mysq TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE dummy (f1 INTEGER PRIMARY KEY, id VARCHAR(128));
INSERT INTO dummy VALUES (123, "dummy data");
INSERT INTO dummy VALUES (234, "moar dummy");
COMMIT;

> CREATE SOURCE foo FROM MYSQL CONNECTION mysq FOR ALL TABLES;
> SELECT * FROM dummy;
123 "dummy data"
234 "moar dummy"

$ mysql-execute name=mysql
USE public;
INSERT INTO dummy VALUES (145, "next row");
COMMIT;

> SELECT * FROM dummy;
123 "dummy data"
234 "moar dummy"
145 "next row"
