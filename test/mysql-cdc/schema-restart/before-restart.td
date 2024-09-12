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

# Create some test tables
# Insert a bunch of rows one of the tables so the snapshot doesn't complete before we restart MZ
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE dummy (f1 INTEGER, f2 INTEGER);
SET @i:=0;
INSERT INTO dummy (f1, f2) SELECT @i:=@i+1, FLOOR(RAND()*10000) FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 50000;
CREATE TABLE other (d1 INTEGER);
INSERT INTO other VALUES (1), (2), (3);

> CREATE SOURCE schema_test FROM MYSQL CONNECTION mysq;
> CREATE TABLE dummy FROM SOURCE schema_test (REFERENCE public.dummy);
> CREATE TABLE other FROM SOURCE schema_test (REFERENCE public.other);

# Now alter the dummy table
$ mysql-execute name=mysql
USE public;
ALTER TABLE dummy DROP COLUMN f2;

# Now restart MZ
