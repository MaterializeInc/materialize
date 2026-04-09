# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE drop_recreate (f1 INTEGER, f2 TEXT);
INSERT INTO drop_recreate VALUES (1, 'before');

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION myconn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> BEGIN
> CREATE SOURCE mysrc FROM MYSQL CONNECTION myconn;
> CREATE TABLE drop_recreate FROM SOURCE mysrc (REFERENCE public.drop_recreate);
> COMMIT

> SELECT * FROM drop_recreate;
1 before
